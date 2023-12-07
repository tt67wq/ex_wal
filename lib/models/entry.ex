defmodule ExWal.Models.Entry do
  @moduledoc false

  defstruct index: 0, data: ""

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          data: binary()
        }
end
