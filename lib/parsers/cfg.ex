defmodule ExComtrade.Parsers.Cfg do
  require Logger

  # COMTRADE standard revisions
  @standards ["1991", "1999", "2001", "2013"]
  @newer_standards ["1999", "2001", "2013"]
  @rev_1991 "1991"
  @rev_2013 "2013"

  @separator ","

  # DAT file format types
  # @type_ascii "ASCII"
  # @type_binary "BINARY"
  # @type_binary32 "BINARY32"
  # @type_float32 "FLOAT32"

  # Special values
  # @timestamp_missing 0xFFFFFFFF

  # timestamp regular expression
  defp regex_date(), do: Regex.compile!("([0-9]{1,2})/([0-9]{1,2})/([0-9]{2,4})")
  defp regex_time(), do: Regex.compile!("([0-9]{1,2}):([0-9]{2}):([0-9]{1,2})(.([0-9]{1,12}))?")

  # Non-standard revision warning
  # _WARNING_UNKNOWN_REVISION = "Unknown standard revision \"{}\""
  # Date time with nanoseconds resolution warning
  # Date time with year 0, month 0 and/or day 0.
  # _WARNING_MIN_DATE = "Missing date values. Using minimum values: {}."

  def parse(io_device) do
    io_device
    |> IO.stream(:line)
    |> Enum.with_index(fn el, index ->
      split =
        el
        |> String.trim()
        |> String.split(@separator)

      {index, split}
    end)
    |> Enum.reduce({%{}, %{}}, fn {index, content}, {output, config} ->
      parse(index, output, config, content)
    end)
  end

  defp parse(line_nr, output, config, line)

  # First line should contain either:
  # station, device, and comtrade standard revision information (only 1999 revision and above has the standard revision year)
  # station, device (older standard)
  defp parse(0, output, config, [station, device]) do
    output =
      output
      |> Map.put(:station, station)
      |> Map.put(:device, device)
      |> Map.put(:standard, @rev_1991)

    {output, config}
  end

  defp parse(0, output, config, [station, device, standard])
       when standard in @standards do
    output =
      output
      |> Map.put(:station, station)
      |> Map.put(:device, device)
      |> Map.put(:standard, standard)

    {output, config}
  end

  # Second Line: number of channels and its type
  # Example: ["20", "4A", "16D"]
  defp parse(1, output, config, [tot_chn, analog_count, status_count]) do
    # Assign defaults
    tot_chn = if tot_chn == "", do: "0", else: tot_chn
    analog_count = if analog_count == "", do: "0A", else: analog_count
    status_count = if status_count == "", do: "0D", else: status_count

    # Parse arguments
    {tot_chn, ""} = Integer.parse(tot_chn)
    {analog_count, "A"} = Integer.parse(analog_count)
    {status_count, "D"} = Integer.parse(status_count)

    output =
      output
      |> Map.put(:total_channels, tot_chn)
      |> Map.put(:analog_channels_count, analog_count)
      |> Map.put(:status_channels_count, status_count)

    # Add page numbers for known ranges
    config =
      config
      |> Map.put(:analog_line_start, 2)
      |> Map.put(:analog_line_end, analog_count + 1)
      |> Map.put(:status_line_start, analog_count + 2)
      |> Map.put(:status_line_end, analog_count + status_count + 1)
      |> Map.put(:frequency_line, analog_count + status_count + 2)
      |> Map.put(:nrates_line, analog_count + status_count + 3)

    {output, config}
  end

  # Analog channel description lines
  defp parse(
         line_number,
         output,
         %{analog_line_start: analog_line_start, analog_line_end: analog_line_end} = config,
         [
           n,
           name,
           ph,
           ccbm,
           uu,
           a,
           b,
           skew,
           cmin,
           cmax,
           primary,
           secondary,
           pors
         ]
       )
       when line_number >= analog_line_start and line_number <= analog_line_end do
    # Parse types
    {n, ""} = Integer.parse(n)
    {a, ""} = Float.parse(a)
    {b, ""} = Float.parse(b)
    {skew, ""} = Float.parse(skew)
    {cmin, ""} = Float.parse(cmin)
    {cmax, ""} = Float.parse(cmax)
    {primary, ""} = Float.parse(primary)
    {secondary, ""} = Float.parse(secondary)

    analog_value = %{
      n: n,
      a: a,
      b: b,
      skew: skew,
      cmin: cmin,
      cmax: cmax,
      name: name,
      uu: uu,
      ph: ph,
      ccbm: ccbm,
      primary: primary,
      secondary: secondary,
      pors: pors
    }

    output =
      Map.put(
        output,
        :analog_values,
        [analog_value | output[:analog_values] || []]
      )

    {output, config}
  end

  # Status channel description lines
  defp parse(
         line_nr,
         output,
         %{
           status_line_start: status_line_start,
           status_line_end: status_line_end
         } = config,
         [n, name, ph, ccbm, y]
       )
       when line_nr >= status_line_start and
              line_nr <= status_line_end do
    # Parse types
    {n, ""} = Integer.parse(n)
    {y, ""} = Float.parse(y)

    status_value =
      %{
        n: n,
        name: name,
        ph: ph,
        ccbm: ccbm,
        y: y
      }

    output =
      Map.put(
        output,
        :status_values,
        [status_value | output[:status_values] || []]
      )

    {output, config}
  end

  # Frequency line
  defp parse(
         line_nr,
         output,
         %{frequency_line: frequency_line} = config,
         [frequency]
       )
       when line_nr == frequency_line do
    {frequency, ""} = Float.parse(frequency)
    output = Map.put(output, :frequency, frequency)

    {output, config}
  end

  # Nrates line
  defp parse(
         line_nr,
         output,
         %{nrates_line: nrates_line} = config,
         [nrates]
       )
       when line_nr == nrates_line do
    {nrates, ""} = Integer.parse(nrates)
    timestamp_critical = nrates == 0

    output =
      output
      |> Map.put(:timestamp_critical, timestamp_critical)
      |> Map.put(:nrates, if(nrates == 0, do: 1, else: nrates))

    config =
      config
      |> Map.put(:nrates_line_start, nrates_line + 1)
      |> Map.put(:nrates_line_end, nrates_line + nrates)
      |> Map.put(:first_data_point_line, nrates_line + nrates + 1)
      |> Map.put(:event_data_point_line, nrates_line + nrates + 2)
      |> Map.put(:dat_filetype_line, nrates_line + nrates + 3)

    {output, config}
  end

  # Fetch nrates
  defp parse(
         line_nr,
         output,
         %{nrates_line_start: nrates_line_start, nrates_line_end: nrates_line_end} = config,
         [samp, endsamp]
       )
       when line_nr >= nrates_line_start and
              line_nr <= nrates_line_end do
    {samp, ""} = Float.parse(samp)
    {endsamp, ""} = Integer.parse(endsamp)

    sample_rate = [%{samp: samp, endsamp: endsamp}]

    output =
      Map.put(
        output,
        :sample_rates,
        [sample_rate | output[:sample_rates] || []]
      )

    {output, config}
  end

  # First data point time and time base
  defp parse(
         line_nr,
         %{standard: standard} = output,
         %{first_data_point_line: first_data_point_line} = config,
         [date_str, time_str]
       )
       when line_nr == first_data_point_line do
    datetime = parse_date_time(standard, date_str, time_str)
    output = Map.put(output, :start_timestamp, datetime)

    {output, config}
  end

  # Event data point and time base
  defp parse(
         line_nr,
         %{standard: standard} = output,
         %{event_data_point_line: event_data_point_line} = config,
         [date_str, time_str]
       )
       when line_nr == event_data_point_line do
    datetime = parse_date_time(standard, date_str, time_str)
    output = Map.put(output, :event_timestamp, datetime)

    {output, config}
  end

  # DAT filetype
  defp parse(
         line_nr,
         output,
         %{dat_filetype_line: dat_filetype_line} = config,
         [dat_filetype]
       )
       when line_nr == dat_filetype_line do
    {Map.put(output, :dat_filetype, dat_filetype), config}
  end

  # Timestamp multiplication factor
  defp parse(
         line_nr,
         %{standard: standard} = output,
         %{dat_filetype_line: dat_filetype_line} = config,
         [time_multiplier]
       )
       when line_nr == dat_filetype_line + 1 and standard in @newer_standards do
    {time_multiplier, ""} =
      if time_multiplier == "", do: {1.0, ""}, else: Float.parse(time_multiplier)

    {
      Map.put(output, :time_multiplier, time_multiplier),
      Map.put(config, :time_code_line, line_nr + 1)
    }
  end

  # time_code and local_code
  defp parse(
         line_nr,
         %{standard: standard} = output,
         %{time_code_line: time_code_line} = config,
         [time_code, local_code]
       )
       when line_nr == time_code_line and standard == @rev_2013 do
    output =
      output
      |> Map.put(:time_code, time_code)
      |> Map.put(:local_code, local_code)

    config = Map.put(config, :tmq_code_leap_second_line, line_nr + 1)

    {output, config}
  end

  # time_code and local_code
  defp parse(
         line_nr,
         %{standard: standard} = output,
         %{tmq_code_leap_second_line: tmq_code_leap_second_line} = config,
         [tmq_code, leap_second]
       )
       when line_nr == tmq_code_leap_second_line and standard == @rev_2013 do
    output =
      output
      |> Map.put(:tmq_code, tmq_code)
      |> Map.put(:leap_second, leap_second)

    {output, config}
  end

  defp parse(line_nr, output, config, line) do
    Logger.warning("Parse fallback called with: #{inspect(line)}\ on line: #{line_nr}")
    {output, config}
  end

  defp parse_date_time(standard, date_str, time_str) when standard in @standards do
    true = Regex.match?(regex_date(), date_str)
    [a, b, c] = String.split(date_str, "/")

    # 1991 Format Uses mm/dd/yyyy format
    # Modern Formats Use dd/mm/yyyy format
    [dd, mm, yyyy] =
      if standard not in @newer_standards, do: [b, a, c], else: [a, b, c]

    {dd, ""} = Integer.parse(dd)
    {mm, ""} = Integer.parse(mm)
    {yyyy, ""} = Integer.parse(yyyy)

    true = Regex.match?(regex_time(), time_str)
    [hours, mins, seconds, mills] = Regex.split(~r/\.|:/, time_str)

    {hours, ""} = Integer.parse(hours)
    {mins, ""} = Integer.parse(mins)
    {seconds, ""} = Integer.parse(seconds)
    {mills, ""} = Integer.parse(mills)

    NaiveDateTime.new!(
      yyyy,
      mm,
      dd,
      hours,
      mins,
      seconds,
      mills
    )
  end
end
