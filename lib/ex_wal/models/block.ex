defmodule ExWal.Models.Block do
  @moduledoc """
  A block of data in a WAL file.
  """

  defstruct written: 0, flushed: 0, buf: ""

  @type t :: %__MODULE__{
          written: non_neg_integer(),
          flushed: non_neg_integer(),
          buf: binary()
        }

  def append(%__MODULE__{written: w, buf: buf} = m, content) do
    %__MODULE__{m | written: w + byte_size(content), buf: buf <> content}
  end

  def flushable(%__MODULE__{written: w, flushed: f} = m) when w > f do
    %__MODULE__{buf: buf} = m
    binary_slice(buf, f..w)
  end

  def flushable(_), do: ""

  def fullfill(%__MODULE__{buf: buf} = m, full_size) do
    %__MODULE__{m | buf: IO.iodata_to_binary([buf, List.duplicate(0, full_size - byte_size(buf))])}
  end

  def flush_to_written(%__MODULE__{written: w, flushed: f} = m) when w > f do
    %__MODULE__{m | flushed: w}
  end

  def flush_to_written(m), do: m
end
