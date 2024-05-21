defmodule ExWal.Models.Segment do
  @moduledoc """
  A single segment file.

  ## Description

  The `ExWal.Models.Segment` module represents a single segment unit.
  It contains information about the path, index, buffer, block count, and blocks within the segment.

  ## Examples

      iex> segment = %ExWal.Models.Segment{path: "segment1.wal", index: 1, buf: "", block_count: 0, blocks: :array.new()}
      %ExWal.Models.Segment{path: "segment1.wal", index: 1, buf: "", block_count: 0, blocks: :array.new()}

  ## Struct Fields

  - `path` - The path of the segment file.
  - `index` - The index of the segment.
  - `buf` - The binary buffer of the segment.
  - `block_count` - The count of blocks within the segment.
  - `blocks` - The array of blocks within the segment.

  """

  defstruct path: "", index: 1, buf: "", block_count: 0, blocks: :array.new(), caches: :array.new()

  @type t :: %__MODULE__{
          path: String.t(),
          # first index of the segment
          index: non_neg_integer(),
          buf: binary(),
          block_count: non_neg_integer(),
          blocks: :array.array(ExWal.Models.Block.t()) | nil,
          caches: :array.array(any()) | nil
        }
end

defmodule ExWal.Models.Block do
  @moduledoc """
  A single block in a segment file.

  ## Description

  The `ExWal.Models.Block` module represents a single block in a segment file.
  It contains information about the offset and size of the block.

  ## Examples

      iex> block = %ExWal.Models.Block{offset: 0, size: 1024}
      %ExWal.Models.Block{offset: 0, size: 1024}

  ## Struct Fields

  - `offset` - The offset of the block within the segment file.
  - `size` - The size of the block in bytes.
  - `data` - The binary data of the block.

  """

  defstruct offset: 0, size: 0, data: ""

  @type t :: %__MODULE__{
          offset: non_neg_integer(),
          size: non_neg_integer(),
          data: binary()
        }
end
