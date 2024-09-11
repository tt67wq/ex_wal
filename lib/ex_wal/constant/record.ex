defmodule ExWal.Constant.Record do
  @moduledoc false

  import ExWal.Constant

  const(:full_chunk_type, 1)
  const(:first_chunk_type, 2)
  const(:middle_chunk_type, 3)
  const(:last_chunk_type, 4)

  const(:recyclable_full_chunk_type, 5)
  const(:recyclable_first_chunk_type, 6)
  const(:recyclable_middle_chunk_type, 7)
  const(:recyclable_last_chunk_type, 8)

  @block_size 1024 * 32
  @legacy_header_size 7
  @recyclable_header_size @legacy_header_size + 4

  const(:block_size, @block_size)
  const(:legacy_header_size, @legacy_header_size)
  const(:recyclable_header_size, @recyclable_header_size)
end
