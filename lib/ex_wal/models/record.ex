defmodule ExWal.Models.LegacyRecord do
  @moduledoc """
  The wire format is that the stream is divided into 32KiB blocks, and each
  block contains a number of tightly packed chunks. Chunks cannot cross block
  boundaries. The last block may be shorter than 32 KiB. Any unused bytes in a
  block must be zero.

  A record maps to one or more chunks. There are two chunk formats: legacy and
  recyclable. The legacy chunk format:

  +----------+-----------+-----------+--- ... ---+
  | CRC (4B) | Size (2B) | Type (1B) | Payload   |
  +----------+-----------+-----------+--- ... ---+

  CRC is computed over the type and payload
  Size is the length of the payload in bytes
  Type is the chunk type

  There are four chunk types: whether the chunk is the full record, or the
  first, middle or last chunk of a multi-chunk record. A multi-chunk record
  has one first chunk, zero or more middle chunks, and one last chunk.
  """

  defstruct crc: 0, size: 0, type: 0, payload: <<>>

  @type t :: %__MODULE__{
          crc: non_neg_integer,
          size: non_neg_integer,
          type: non_neg_integer,
          payload: binary
        }

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{crc: crc, size: size, type: type, payload: payload}) do
    <<crc::size(32), size::size(16), type::size(8), payload::binary>>
  end
end

defmodule ExWal.Models.RecyclableRecord do
  @moduledoc """
  The recyclyable chunk format is similar to the legacy format, but extends
  the chunk header with an additional log number field. This allows reuse
  (recycling) of log files which can provide significantly better performance
  when syncing frequently as it avoids needing to update the file
  metadata. Additionally, recycling log files is a prequisite for using direct
  IO with log writing. The recyclyable format is:

  +----------+-----------+-----------+----------------+--- ... ---+
  | CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
  +----------+-----------+-----------+----------------+--- ... ---+

  Recyclable chunks are distinguished from legacy chunks by the addition of 4
  extra "recyclable" chunk types that map directly to the legacy chunk types
  (i.e. full, first, middle, last). The CRC is computed over the type, log
  number, and payload.

  The wire format allows for limited recovery in the face of data corruption:
  on a format error (such as a checksum mismatch), the reader moves to the
  next block and looks for the next full or first chunk.
  """

  alias ExWal.Constant.Record

  require ExWal.Constant.Record

  defstruct crc: 0, size: 0, type: 0, log_number: 0, payload: <<>>

  @recyclable_full_chunk_type Record.recyclable_full_chunk_type()
  @recyclable_first_chunk_type Record.recyclable_first_chunk_type()
  @recyclable_middle_chunk_type Record.recyclable_middle_chunk_type()
  @recyclable_last_chunk_type Record.recyclable_last_chunk_type()

  @type t :: %__MODULE__{
          crc: non_neg_integer,
          size: non_neg_integer,
          type: non_neg_integer,
          log_number: non_neg_integer,
          payload: binary
        }

  def new(type, log_number, payload) do
    m = %__MODULE__{size: byte_size(payload), type: type, log_number: log_number, payload: payload}
    %__MODULE__{m | crc: checksum(m)}
  end

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{crc: crc, size: size, type: type}), do: crc == 0 and size == 0 and type == 0

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{crc: crc, size: size, type: type, log_number: log_number, payload: payload}) do
    <<crc::size(32), size::size(16), type, log_number::size(32), payload::binary>>
  end

  @spec checksum(t()) :: non_neg_integer()
  defp checksum(%__MODULE__{type: type, log_number: log_number, payload: payload}) do
    :erlang.crc32(<<type, log_number::size(32), payload::binary>>)
  end

  @spec parse(binary()) :: {:ok, record :: t() | nil, rest :: binary()} | {:error, :checksum_mismatch | :invalid_type}
  def parse(buf)

  def parse(buf) when byte_size(buf) >= 11 do
    <<crc::size(32), size::size(16), type, log_number::size(32), rest::binary>> = buf

    with :ok <- verify_type(type),
         {<<payload::bytes-size(size), rest::binary>>, _, _} = {rest, size, crc},
         m = %__MODULE__{crc: crc, size: size, type: type, log_number: log_number, payload: payload},
         :ok <- verify_length(m),
         :ok <- verify_checksum(m),
         do: {:ok, m, rest}
  end

  # no room for another record, maybe aligned to next block
  def parse(buf), do: {:ok, nil, buf}

  @spec last_chunk?(t()) :: boolean()
  def last_chunk?(%__MODULE__{type: type}), do: type in [@recyclable_full_chunk_type, @recyclable_last_chunk_type]

  defp verify_type(type) do
    if(
      type in [
        @recyclable_first_chunk_type,
        @recyclable_middle_chunk_type,
        @recyclable_last_chunk_type,
        @recyclable_full_chunk_type
      ]
    ) do
      :ok
    else
      {:error, :invalid_type}
    end
  end

  defp verify_length(%__MODULE__{payload: payload, size: size}) do
    if byte_size(payload) == size do
      :ok
    else
      {:error, :invalid_size}
    end
  end

  defp verify_checksum(%__MODULE__{crc: crc} = m) do
    m
    |> checksum()
    |> case do
      ^crc ->
        :ok

      _ ->
        {:error, :checksum_mismatch}
    end
  end
end

defimpl Inspect, for: ExWal.Models.RecyclableRecord do
  alias ExWal.Models.RecyclableRecord

  def inspect(%RecyclableRecord{type: type, log_number: log_number, size: size, crc: crc, payload: payload}, _opts) do
    "#<ExWal.Models.RecyclableRecord type=#{type} log_number=#{log_number} size=#{size} crc=#{crc}, payload=#{payload}>"
  end
end
