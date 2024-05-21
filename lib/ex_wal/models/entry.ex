defmodule ExWal.Models.Entry do
  @moduledoc """
  entry struct for each record in WAL

  ## Description
  - index: the index of the entry, monotonically increasing.
  - data: the binary data of the entry
  - cache: This field only exists in hot data and is lost once the entry is persisted to cold data.
      This field can be used in the scenario of "nearby reading", when the data that was just written into WAL is read,
      the value of cache can be directly used, reducing the consumption of repeated serialization.
  """

  defstruct index: 0, data: "", cache: nil

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          data: binary(),
          cache: any()
        }

  @doc """
  Create a new entry

  ## Examples

      iex> Entry.new(1, "Hello Elixir")
      %Entry{index: 1, data: "Hello Elixir"}
  """
  @spec new(non_neg_integer(), binary(), any()) :: t()
  def new(index, data, cache \\ nil), do: %__MODULE__{index: index, data: data, cache: cache}
end
