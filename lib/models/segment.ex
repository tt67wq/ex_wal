defmodule ExWal.Models.Segment do
  @moduledoc """
  a single segment file
  """

  defstruct path: "", index: 1, buf: "", block_count: 0, blocks: :array.new()

  @type t :: %__MODULE__{
          path: String.t(),
          # first index of the segment
          index: non_neg_integer(),
          buf: binary(),
          block_count: non_neg_integer(),
          blocks: :array.array(ExWal.Models.Block.t()) | nil
        }
end

defmodule ExWal.Models.Block do
  @moduledoc """
  a single block in a segment file
  """

  defstruct begin: 0, size: 0

  @type t :: %__MODULE__{
          begin: non_neg_integer(),
          size: non_neg_integer()
        }
end
