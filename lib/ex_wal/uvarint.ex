defmodule ExWal.Uvarint do
  @moduledoc """
  The Uvarint module is used to read variable-length encoded unsigned integers.
  The implementation is based on the variable-length encoding specification of Protocol Buffers.
  Variable-length encoding is a method of encoding smaller integers with fewer bytes, thereby saving storage space.

  In the implementation of the Uvarint function, if the highest bit of a byte is 0, then that byte is the last byte of the encoding;
  if the highest bit is 1, then that byte is not the last byte and there are other bytes following it.
  This encoding method can be used to store any size of unsigned integer, but for larger integers, multiple bytes may be required for encoding.
  """

  @not_last 0x80

  @doc """
  Encodes a non-negative integer into a variable-length binary representation.

  ## Examples

      iex> ExWal.Uvarint.encode(42)
      <<42::size(8)>>

      iex> ExWal.Uvarint.encode(300)
      <<0x8C, 0x02>>
  """
  @spec encode(non_neg_integer()) :: binary()
  def encode(n) do
    case n do
      n when n < 0 ->
        raise ArgumentError, "n must be positive"

      n when n < 128 ->
        <<n::size(8)>>

      n ->
        n
        |> do_encode()
        |> IO.iodata_to_binary()
    end
  end

  @doc """
  Decodes a variable-length binary representation into a non-negative integer.

  ## Examples

      iex> ExWal.Uvarint.decode(<<42::size(8)>>)
      {42, 1, ""}

      iex> ExWal.Uvarint.decode(<<0x8E, 0x02>> <> "Hello World")
      {270, 2, "Hello World"}
  """
  @spec decode(binary()) ::
          {value :: non_neg_integer(), bytes_read :: non_neg_integer(), rest :: binary()}
  def decode(data) do
    case data do
      <<n::size(8), rest::binary>> when n < 128 ->
        {n, 1, rest}

      <<n::size(8), rest::binary>> ->
        {rn, bytes_read, rest} = decode(rest)

        {Bitwise.bsl(rn, 7) + Bitwise.band(n, 0x7F), bytes_read + 1, rest}
    end
  end

  defp do_encode(n) do
    case n do
      n when n < 0 ->
        raise ArgumentError, "n must be positive"

      n when n < 128 ->
        [n]

      n ->
        [Bitwise.band(n, 0x7F) + @not_last | do_encode(Bitwise.bsr(n, 7))]
    end
  end
end
