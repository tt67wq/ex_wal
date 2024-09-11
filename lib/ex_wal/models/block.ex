defmodule ExWal.Models.Block do
  @moduledoc """
  A block of data in a WAL file.
  """

  require ExWal.Constant.Record

  defstruct written: 0, flushed: 0, flushable: []

  @type t :: %__MODULE__{
          written: non_neg_integer(),
          flushed: non_neg_integer(),
          flushable: [iodata()]
        }

  @block_size ExWal.Constant.Record.block_size()

  def new, do: %__MODULE__{written: 0, flushed: 0, flushable: []}

  def append(m, ""), do: m

  def append(%__MODULE__{written: w} = m, content) when w + byte_size(content) <= @block_size do
    %__MODULE__{written: w, flushable: flushable} = m
    %__MODULE__{m | written: w + byte_size(content), flushable: [content | flushable]}
  end

  def append(m, content) do
    %__MODULE__{written: w} = m

    raise(ExWal.Exception, message: "block is full", details: [written: w, content_size: byte_size(content)])
  end

  def flushable(%__MODULE__{flushable: f}) do
    f
    |> Enum.reverse()
    |> IO.iodata_to_binary()
  end

  def flushable(_), do: ""

  def fullfill(%__MODULE__{written: written} = m, full_size) do
    to_ff =
      0
      |> List.duplicate(full_size - written)
      |> IO.iodata_to_binary()

    append(m, to_ff)
  end

  def flush_all(%__MODULE__{written: w, flushed: f} = m) when w > f do
    %__MODULE__{m | flushed: w, flushable: []}
  end

  def flush_all(m), do: m

  def flush_to(%__MODULE__{flushed: f1} = m, f2) when f1 >= f2, do: m

  def flush_to(m, f2) do
    %__MODULE__{flushed: f1} = m

    fa =
      m
      |> flushable()
      |> binary_slice(f1, f2 - f1)

    %__MODULE__{m | flushed: f2, flushable: [fa]}
  end
end
