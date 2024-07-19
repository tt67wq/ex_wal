defmodule ExWal.Models.VirtualLog do
  @moduledoc false

  @type log_num :: non_neg_integer()

  @type t :: %__MODULE__{
          log_num: log_num(),
          segments: [ExWal.Models.Segment.t()]
        }
  defstruct log_num: 0, segments: []

  def filename(log_num, index)

  def filename(log_num, 0) do
    log_num
    |> log_num_string()
    |> Kernel.<>(".log")
  end

  def filename(log_num, index) do
    log_num
    |> log_num_string()
    |> Kernel.<>("-#{index}.log")
  end

  defp log_num_string(log_num) do
    log_num
    |> Integer.to_string()
    |> String.pad_leading(6, "0")
  end

  @spec parse_filename(filename :: binary()) :: {log_num :: non_neg_integer(), index :: non_neg_integer()}
  def parse_filename(filename) do
    filename
    |> String.replace_suffix(".log", "")
    |> String.split("-")
    |> case do
      [m] -> {String.to_integer(m), 0}
      [m, i] -> {String.to_integer(m), String.to_integer(i)}
    end
  end
end
