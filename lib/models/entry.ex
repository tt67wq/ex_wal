defmodule ExWal.Models.Entry do
  @moduledoc """
  entry struct for each record in WAL
  """

  defstruct index: 0, data: ""

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          data: binary()
        }

  @spec new(non_neg_integer(), binary()) :: t()
  def new(index, data), do: %__MODULE__{index: index, data: data}
end
