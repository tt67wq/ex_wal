defmodule ExWal.Models.Segment do
  @moduledoc false

  defstruct index: 0, dir: "", fs: nil

  @type index :: non_neg_integer()
  @type t :: %__MODULE__{
          index: index(),
          dir: String.t(),
          fs: ExWal.FS.t()
        }
end
