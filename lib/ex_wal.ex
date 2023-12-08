defmodule ExWal do
  @moduledoc """
  Documentation for `ExWal`.
  """

  use GenServer
  alias ExWal.Models.Segment
  alias ExWal.Models.Block
  alias ExWal.Models.Entry
  alias ExWal.LRU
  alias ExWal.Uvarint

  @wal_option_schema [
    path: [
      type: :string,
      doc: "The path of the WAL. Default is `./wal`.",
      default: "./wal"
    ],
    nosync: [
      type: :boolean,
      doc: "If true, WAL will not sync to disk. Default is false.",
      default: false
    ],
    segment_size: [
      type: :integer,
      doc: "The size of each segment file. Default is 16MB.",
      default: 16 * 1024 * 1024
    ],
    segment_cache_size: [
      type: :integer,
      doc: "The size of the segment cache. Default is 100.",
      default: 2
    ],
    dir_permission: [
      type: :integer,
      doc: "The permission of the directory. Default is 0o750.",
      default: 0o750
    ],
    file_permission: [
      type: :integer,
      doc: "The permission of the file. Default is 0o640.",
      default: 0o640
    ],
    name: [
      type: :atom,
      doc: "The name of the WAL. Default is `wal`.",
      default: __MODULE__
    ]
  ]

  defstruct data_path: "",
            hot: %Segment{},
            cold: :array.new(),
            first_index: 0,
            last_index: 0,
            lru_cache: nil,
            tail_file_handler: nil,
            opts: [
              nosync: false,
              segment_size: 64 * 1024 * 1024,
              dir_permission: 0o750,
              file_permission: 0o640
            ]

  @type t :: %__MODULE__{
          data_path: String.t(),
          hot: Segment.t(),
          cold: :array.array(),
          first_index: non_neg_integer(),
          last_index: non_neg_integer(),
          lru_cache: atom(),
          tail_file_handler: File.io_device(),
          opts: [
            nosync: boolean(),
            segment_size: non_neg_integer(),
            dir_permission: non_neg_integer(),
            file_permission: non_neg_integer()
          ]
        }

  @type index :: non_neg_integer()

  @type wal_option_schema_t :: [unquote(NimbleOptions.option_typespec(@wal_option_schema))]

  @spec start_link(wal_option_schema_t()) :: GenServer.on_start()
  def start_link(opts) do
    opts = NimbleOptions.validate!(opts, @wal_option_schema)
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @spec stop(atom() | pid()) :: :ok
  def stop(name_or_pid) do
    GenServer.stop(name_or_pid)
  end

  @spec write(atom() | pid(), [Entry.t()]) :: :ok
  def write(name_or_pid, entries) do
    GenServer.call(name_or_pid, {:write, entries})
  end

  @spec read(atom() | pid(), index()) :: {:ok, binary()} | {:error, :index_not_found}
  def read(name_or_pid, index) do
    GenServer.call(name_or_pid, {:read, index})
  end

  @spec last_index(atom() | pid()) :: index()
  def last_index(name_or_pid) do
    GenServer.call(name_or_pid, :last_index)
  end

  # ----------------- Server  -----------------

  @impl GenServer
  def init(opts) do
    lru_name = :"#{opts[:name]}_lru"
    {:ok, _} = LRU.start_link(lru_name, opts[:segment_cache_size])

    path = Path.absname(opts[:path])
    File.mkdir_p!(path)
    File.chmod!(path, opts[:dir_permission])

    segments =
      path
      |> Path.join("*")
      |> Path.wildcard()
      |> Enum.reject(fn x -> File.dir?(x) end)
      |> Enum.map(&Path.basename(&1))
      |> Enum.sort(:desc)
      |> Enum.reject(&(String.length(&1) < 20))
      |> Enum.map(fn x ->
        %Segment{
          path: Path.join(path, x),
          index: parse_segment_filename(x)
        }
      end)

    if Enum.empty?(segments) do
      seg1 = Path.join(path, segment_filename(1))

      {:ok, h} = File.open(seg1, [:read, :append])
      File.chmod!(seg1, opts[:file_permission])

      {:ok,
       %__MODULE__{
         data_path: path,
         hot: %Segment{path: seg1, index: 1},
         cold: :array.new(),
         first_index: 1,
         last_index: 0,
         lru_cache: lru_name,
         tail_file_handler: h,
         opts: [
           nosync: opts[:nosync],
           segment_size: opts[:segment_size],
           dir_permission: opts[:dir_permission],
           file_permission: opts[:file_permission]
         ]
       }}
    else
      # NOTE: segment is in reverse order
      first_index = List.last(segments).index

      [%Segment{path: spath, index: begin_index} = seg | t] = segments
      {:ok, h} = File.open(spath, [:read, :append])

      %Segment{block_count: bc} =
        seg =
        load_segment(seg)

      {:ok,
       %__MODULE__{
         data_path: path,
         hot: seg,
         cold: :array.from_list(t),
         first_index: first_index,
         last_index: begin_index + bc - 1,
         lru_cache: lru_name,
         tail_file_handler: h,
         opts: [
           nosync: opts[:nosync],
           segment_size: opts[:segment_size],
           dir_permission: opts[:dir_permission],
           file_permission: opts[:file_permission]
         ]
       }}
    end
  end

  @impl GenServer
  def terminate(_reason, %__MODULE__{lru_cache: lru, tail_file_handler: h}) do
    LRU.stop(lru)
    :file.sync(h)
    :file.close(h)
  end

  @impl GenServer
  def handle_call(:last_index, _, %__MODULE__{last_index: last_index} = state),
    do: {:reply, last_index, state}

  def handle_call(
        {:write, entries},
        _from,
        %__MODULE__{
          last_index: last_index,
          hot: %Segment{buf: buf0},
          opts: opts
        } = state
      ) do
    # check
    entries
    |> Enum.with_index(last_index + 1)
    |> Enum.find(fn {%Entry{index: index}, i} -> index != i end)
    |> is_nil() || raise ArgumentError, "invalid index"

    # cycle if needed
    state =
      if byte_size(buf0) > opts[:segment_size] do
        cycle(state)
      else
        state
      end

    {:reply, :ok, write_entries(entries, state)}
  end

  def handle_call(
        {:read, index},
        _from,
        %__MODULE__{
          first_index: first_index,
          last_index: last_index
        } = state
      )
      when index < first_index or index > last_index do
    {:reply, {:error, :index_not_found}, state}
  end

  def handle_call(
        {:read, index},
        _from,
        %__MODULE__{
          lru_cache: lru,
          hot: %Segment{index: last_begin_index} = seg,
          cold: cold
        } = state
      ) do
    %Segment{index: begin_index, blocks: blocks, buf: buf} =
      if index >= last_begin_index do
        seg
      else
        find_segment(lru, index, cold)
      end

    %Block{begin: begin, size: size} = :array.get(index - begin_index, blocks)

    # ExWal.Debug.stacktrace(%{
    #   target_index: index,
    #   begin_index: begin_index,
    #   block_begin: begin,
    #   block_size: size,
    #   buf_size: byte_size(buf),
    #   path: path
    # })

    data = binary_part(buf, begin, size)
    {data_size, _, data} = Uvarint.decode(data)
    # ExWal.Debug.debug(data)
    byte_size(data) == data_size || raise ArgumentError, "invalid data"

    {:reply, {:ok, data}, state}
  end

  @spec load_segment(Segment.t()) :: Segment.t()
  defp load_segment(
         %Segment{
           #  index: begin_index,
           path: path
         } = seg
       ) do
    {:ok, content} = File.read(path)

    {bc, blocks} = parse_blocks(content, 0, 0, [])
    %Segment{seg | buf: content, blocks: :array.from_list(blocks), block_count: bc}
  end

  @spec parse_segment_filename(String.t()) :: index()
  defp parse_segment_filename(filename) do
    filename
    |> String.trim_leading("0")
    |> String.to_integer()
  end

  @spec segment_filename(index()) :: String.t()
  defp segment_filename(index) do
    index
    |> Integer.to_string()
    |> String.pad_leading(20, "0")
  end

  @spec parse_blocks(
          data :: binary(),
          since :: non_neg_integer(),
          block_count :: non_neg_integer(),
          blocks :: [Block.t()]
        ) ::
          {non_neg_integer(), [Block.t()]}
  defp parse_blocks("", _, bc, blocks), do: {bc, Enum.reverse(blocks)}

  defp parse_blocks(data, since, bc, acc) do
    {size, bytes_read, data} = Uvarint.decode(data)

    # check
    size == 0 && raise ArgumentError, "invalid block size"

    next = since + size + bytes_read
    <<_::bytes-size(size), rest::binary>> = data

    parse_blocks(rest, next, bc + 1, [
      %Block{begin: since, size: size + bytes_read} | acc
    ])
  end

  @spec append_entry(Segment.t(), Entry.t()) :: {binary(), Segment.t()}
  defp append_entry(
         %Segment{
           buf: buf,
           index: begin_index,
           block_count: bc,
           blocks: blocks
         } = seg,
         %Entry{
           index: index,
           data: data
         }
       ) do
    index == begin_index + bc ||
      raise ArgumentError,
            "invalid index, begin_index: #{begin_index}, bc: #{bc}, index: #{index}"

    data = Uvarint.encode(byte_size(data)) <> data

    {data,
     %Segment{
       seg
       | buf: <<buf::binary, data::binary>>,
         block_count: bc + 1,
         blocks: :array.set(bc, %Block{begin: byte_size(buf), size: byte_size(data)}, blocks)
     }}
  end

  @spec write_entries([Entry.t()], t()) :: t()
  defp write_entries([], m), do: m

  defp write_entries(
         [entry | t],
         %__MODULE__{
           hot: seg,
           opts: opts,
           tail_file_handler: h
         } = m
       ) do
    {data, %Segment{buf: buf, index: begin_index, block_count: bc} = seg} =
      append_entry(seg, entry)

    :ok = :file.write(h, data)

    if opts[:nosync] do
      :ok = :file.sync(h)
    end

    m = %__MODULE__{m | hot: seg, last_index: begin_index + bc - 1}

    if byte_size(buf) < opts[:segment_size] do
      write_entries(t, m)
    else
      m = cycle(m)
      write_entries(t, m)
    end
  end

  # Cycle the old segment for a new segment.
  @spec cycle(t()) :: t()
  defp cycle(
         %__MODULE__{
           data_path: path,
           tail_file_handler: h,
           lru_cache: lru,
           last_index: last_index,
           hot: seg,
           cold: cold,
           opts: opts
         } = m
       ) do
    :ok = :file.sync(h)
    :ok = :file.close(h)
    :ok = LRU.put(lru, make_ref(), seg)

    new_seg = %Segment{
      path: Path.join(path, segment_filename(last_index + 1)),
      index: last_index + 1
    }

    {:ok, h} = File.open(new_seg.path, [:read, :append])
    :ok = File.chmod!(new_seg.path, opts[:file_permission])

    size = :array.size(cold)

    %__MODULE__{
      m
      | tail_file_handler: h,
        hot: new_seg,
        cold: :array.set(size, %Segment{seg | blocks: nil, buf: ""}, cold),
        last_index: last_index
    }
  end

  @spec find_segment(atom(), index(), :array.array()) :: Segment.t()
  defp find_segment(lru, index, cold) do
    LRU.select(lru, fn %Segment{index: x, block_count: bc} ->
      index >= x and index < x + bc
    end)
    |> case do
      nil ->
        cold
        |> bin_search(index)
        |> load_segment()
        |> tap(fn x -> LRU.put(lru, make_ref(), x) end)

      seg ->
        seg
    end
  end

  @spec bin_search(:array.array(), index()) :: Segment.t() | nil
  defp bin_search(cold, target_index), do: do_search(cold, target_index, 0, :array.size(cold) - 1)

  defp do_search(_cold, _target_index, min, max) when min > max, do: nil

  defp do_search(cold, target_index, min, max) do
    mid = div(min + max, 2)
    %Segment{index: index, block_count: bc} = seg = :array.get(mid, cold)

    cond do
      target_index < index ->
        do_search(cold, target_index, min, mid - 1)

      target_index >= index and target_index < index + bc ->
        seg

      true ->
        do_search(cold, target_index, mid + 1, max)
    end
  end
end
