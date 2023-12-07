defmodule ExWal.Models.Segment do
  @moduledoc """
  a single segment file
  """

  defstruct path: "", index: 0, buf: "", block_count: 0, blocks: []

  @type t :: %__MODULE__{
          path: String.t(),
          # first index of the segment
          index: non_neg_integer(),
          buf: binary(),
          block_count: non_neg_integer(),
          # blocks in the segment, in reverse order, first block is the last block in the segment
          blocks: [ExWal.Models.Block.t()]
        }
end

defmodule ExWal.Models.Block do
  @moduledoc """
  a single block in a segment file
  format:
  """

  defstruct begin_post: 0, end_post: 0

  @type t :: %__MODULE__{
          begin_post: non_neg_integer(),
          end_post: non_neg_integer()
        }
end
