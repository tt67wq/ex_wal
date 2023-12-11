defmodule ExWal do
  @moduledoc """
  Documentation for `ExWal`.
  """

  use GenServer

  alias ExWal.LRU
  alias ExWal.Models.Block
  alias ExWal.Models.Entry
  alias ExWal.Models.Segment
  alias ExWal.Store
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
    store: [
      type: :atom,
      doc: "The store module which implement `ExWal.Store`. Default is `ExWal.Store.File`.",
      default: Store.File
    ],
    store_opts: [
      type: :keyword_list,
      doc: "The options for the store module.",
      default: []
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
            store: nil,
            tail_store_handler: nil,
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
          store: Store.t(),
          tail_store_handler: Store.handler(),
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

  @spec truncate_after(atom() | pid(), index()) :: :ok | {:error, :index_out_of_range}
  def truncate_after(name_or_pid, index) do
    GenServer.call(name_or_pid, {:truncate_after, index})
  end

  # ----------------- Server  -----------------

  @impl GenServer
  def init(opts) do
    lru_name = :"#{opts[:name]}_lru"
    {:ok, _} = LRU.start_link(lru_name, opts[:segment_cache_size])

    store = apply(opts[:store], :new, [opts[:store_opts]])

    path = Path.absname(opts[:path])
    Store.mkdir(store, path, opts[:dir_permission])

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
      seg1_path = Path.join(path, segment_filename(1))

      {:ok, h} =
        Store.open(
          store,
          seg1_path,
          [:read, :append],
          opts[:file_permission]
        )

      {:ok,
       %__MODULE__{
         data_path: path,
         hot: %Segment{path: seg1_path, index: 1},
         cold: :array.new(),
         first_index: 1,
         last_index: 0,
         lru_cache: lru_name,
         store: store,
         tail_store_handler: h,
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

      [%Segment{path: spath} = seg | t] = segments
      {:ok, h} = Store.open(store, spath, [:read, :append], opts[:file_permission])

      {:ok,
       %__MODULE__{
         data_path: path,
         hot: seg,
         cold: :array.from_list(t),
         first_index: first_index,
         lru_cache: lru_name,
         store: store,
         tail_store_handler: h,
         opts: [
           nosync: opts[:nosync],
           segment_size: opts[:segment_size],
           dir_permission: opts[:dir_permission],
           file_permission: opts[:file_permission]
         ]
       }, {:continue, :load_segment}}
    end
  end

  @impl GenServer
  def terminate(_reason, %__MODULE__{lru_cache: lru, store: store, tail_store_handler: h}) do
    LRU.stop(lru)
    Store.sync(store, h)
    Store.close(store, h)
  end

  @impl GenServer
  def handle_continue(:load_segment, %__MODULE__{hot: seg, store: store} = state) do
    %Segment{block_count: bc, index: begin_index} =
      seg =
      load_segment(seg, store)

    {:noreply, %__MODULE__{state | hot: seg, last_index: begin_index + bc - 1}}
  end

  @impl GenServer
  def handle_call(:last_index, _, %__MODULE__{last_index: last_index} = state), do: {:reply, last_index, state}

  def handle_call(
        {:write, entries},
        _from,
        %__MODULE__{last_index: last_index, hot: %Segment{buf: buf0}, opts: opts} = state
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

  def handle_call({:read, index}, _from, %__MODULE__{first_index: first_index, last_index: last_index} = state)
      when index < first_index or index > last_index do
    {:reply, {:error, :index_not_found}, state}
  end

  def handle_call({:read, index}, _from, %__MODULE__{} = state) do
    %Segment{index: begin_index, blocks: blocks, buf: buf} = find_segment(index, state)

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

  def handle_call({:truncate_after, 0}, _from, state), do: {:reply, {:error, :index_out_of_range}, state}

  def handle_call({:truncate_after, index}, _from, %__MODULE__{first_index: first_index, last_index: last_index} = state)
      when index < first_index or index > last_index do
    {:reply, {:error, :index_out_of_range}, state}
  end

  def handle_call({:truncate_after, index}, _from, state) do
    {:reply, :ok, __truncate_after(index, state)}
  end

  @spec load_segment(Segment.t(), Store.t()) :: Segment.t()
  #  index: begin_index,
  defp load_segment(%Segment{path: path} = seg, store) do
    {:ok, content} = Store.read_all(store, path)

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
  defp append_entry(%Segment{buf: buf, index: begin_index, block_count: bc, blocks: blocks} = seg, %Entry{
         index: index,
         data: data
       }) do
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

  defp write_entries([entry | t], %__MODULE__{hot: seg, opts: opts, store: store, tail_store_handler: h} = m) do
    {data, %Segment{buf: buf, index: begin_index, block_count: bc} = seg} =
      append_entry(seg, entry)

    :ok = Store.append(store, h, data)

    if opts[:nosync] do
      :ok = Store.sync(store, h)
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
           data_path: data_path,
           store: store,
           tail_store_handler: h,
           lru_cache: lru,
           last_index: last_index,
           hot: %Segment{path: path} = seg,
           cold: cold,
           opts: opts
         } = m
       ) do
    :ok = Store.sync(store, h)
    :ok = Store.close(store, h)
    :ok = LRU.put(lru, path, seg)

    new_seg = %Segment{
      path: Path.join(data_path, segment_filename(last_index + 1)),
      index: last_index + 1
    }

    {:ok, h} = Store.open(store, new_seg.path, [:read, :append], opts[:file_permission])

    size = :array.size(cold)

    %__MODULE__{
      m
      | tail_store_handler: h,
        hot: new_seg,
        cold: :array.set(size, %Segment{seg | blocks: nil, buf: ""}, cold),
        last_index: last_index
    }
  end

  @spec find_segment(index :: index(), t()) :: Segment.t()
  defp find_segment(index, %__MODULE__{hot: %Segment{index: last_begin_index} = seg} = state) do
    if index >= last_begin_index do
      seg
    else
      find_cold_segment(index, state)
    end
  end

  @spec find_cold_segment(index(), t()) :: Segment.t()
  defp find_cold_segment(index, %__MODULE__{lru_cache: lru, cold: cold, store: store}) do
    lru
    |> LRU.select(fn %Segment{index: x, block_count: bc} ->
      index >= x and index < x + bc
    end)
    |> case do
      nil ->
        cold
        |> bin_search(index)
        |> :array.get(cold)
        |> load_segment(store)
        |> tap(fn %Segment{path: path} = x -> LRU.put(lru, path, x) end)

      seg ->
        seg
    end
  end

  @spec bin_search(:array.array(), index()) :: non_neg_integer() | nil
  defp bin_search(cold, target_index), do: do_search(cold, target_index, 0, :array.size(cold) - 1)

  defp do_search(_cold, _target_index, min, max) when min > max, do: nil

  defp do_search(cold, target_index, min, max) do
    mid = div(min + max, 2)
    %Segment{index: index, block_count: bc} = :array.get(mid, cold)

    cond do
      target_index < index ->
        do_search(cold, target_index, min, mid - 1)

      target_index >= index and target_index < index + bc ->
        mid

      true ->
        do_search(cold, target_index, mid + 1, max)
    end
  end

  # defp truncate_after(0, _), do: {:error, :index_out_of_range}

  # defp truncate_after(index, %__MODULE__{last_index: last_index, first_index: first_index})
  #      when index < first_index or index > last_index do
  #   {:error, :index_out_of_range}
  # end

  @spec __truncate_after(index(), t()) :: t()
  defp __truncate_after(
         index,
         %__MODULE__{hot: %Segment{index: begin_index, blocks: blocks, buf: buf, path: path} = seg, lru_cache: lru} =
           state
       )
       when index >= begin_index do
    # truncate buf and blocks
    bc = index - begin_index
    %Block{begin: offset} = :array.get(bc + 1, blocks)
    buf = binary_part(buf, 0, offset - 1)

    LRU.delete(lru, path)

    # no need to gc blocks, reset block_count is all we need
    %__MODULE__{
      state
      | hot: %Segment{
          seg
          | buf: buf,
            block_count: bc
        }
    }
  end

  defp __truncate_after(
         index,
         %__MODULE__{cold: cold, store: store, data_path: data_path, tail_store_handler: h, lru_cache: lru} = state
       ) do
    # find segment by index

    idx = bin_search(cold, index)

    %Segment{buf: buf, blocks: blocks, index: begin_index, path: path} =
      seg =
      idx
      |> :array.get(cold)
      |> load_segment(store)

    # truncate buf and blocks
    bc = index - begin_index
    %Block{begin: offset} = :array.get(bc + 1, blocks)
    buf = binary_part(buf, 0, offset - 1)
    # no need to gc blocks, reset block_count is all we need
    new_seg = %Segment{
      seg
      | buf: buf,
        block_count: bc
    }

    # create tmp file to store new buf
    temp_file = Path.join(data_path, "tmp")
    :ok = Store.write_all(store, temp_file, buf)

    # swap the tmp file with the old segment file
    :ok = Store.rename(store, temp_file, path)
    :ok = Store.rm(store, temp_file)

    # cleanup
    # all segments and cache after the new segment should be deleted
    :ok = Store.close(store, h)
    cold_size = :array.size(cold)

    :ok = LRU.delete(lru, path)

    Enum.each((idx + 1)..(cold_size - 1), fn i ->
      %Segment{path: path} = :array.get(i, cold)
      Store.rm(store, path)
      LRU.delete(lru, path)
    end)

    cold = :array.resize(idx + 1, cold)

    # reopen tail handler
    {:ok, h} = Store.open(store, path, [:read, :append], state.opts[:file_permission])

    %__MODULE__{
      state
      | hot: new_seg,
        cold: cold,
        tail_store_handler: h,
        last_index: index
    }
  end
end
