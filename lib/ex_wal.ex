defmodule ExWal do
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  use GenServer

  alias ExWal.LRU
  alias ExWal.Models.Block
  alias ExWal.Models.Entry
  alias ExWal.Models.Segment
  alias ExWal.Store
  alias ExWal.Uvarint

  require Logger

  @external_resource "README.md"
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

  @doc """
  Start a WAL process

  ## Options
  #{NimbleOptions.docs(@wal_option_schema)}
  """
  @spec start_link(wal_option_schema_t()) :: GenServer.on_start()
  def start_link(opts) do
    opts = NimbleOptions.validate!(opts, @wal_option_schema)
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @doc """
  Stop a WAL process
  """
  @spec stop(atom() | pid()) :: :ok
  def stop(name_or_pid) do
    GenServer.stop(name_or_pid)
  end

  @doc """
  Write entries to WAL, the entries must be strictly consecutive and incremental,
  and the index of the first entry must be WAL's last_index + 1.

  ## Examples

      iex> last_index = ExWal.last_index(:wal_name)
      iex> enties = last_index..(last_index+10) |> Enum.map(fn i -> Entry.new(i, "some data") end)
      iex> :ok = ExWal.write(:wal_name, entries)
  """
  @spec write(atom() | pid(), [Entry.t()], non_neg_integer()) :: :ok
  def write(name_or_pid, entries, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:write, entries}, timeout)
  end

  @doc """
  Read entry content from WAL by index. if index is not found, return {:error, :index_not_found}

  ## Examples

      iex> {:ok, data} = ExWal.read(:wal_name, 1)
  """
  @spec read(atom() | pid(), index()) :: {:ok, Entry.t()} | {:error, :index_not_found}
  def read(name_or_pid, index, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:read, index}, timeout)
  end

  @doc """
  Get the last index of WAL
  """
  @spec last_index(atom() | pid()) :: index()
  def last_index(name_or_pid) do
    GenServer.call(name_or_pid, :last_index)
  end

  @doc """
  Get the first index of WAL
  """
  @spec first_index(atom() | pid()) :: index()
  def first_index(name_or_pid) do
    GenServer.call(name_or_pid, :first_index)
  end

  @doc """
  Get the segment count of WAL
  """
  @spec segment_count(atom() | pid()) :: non_neg_integer()
  def segment_count(name_or_pid) do
    GenServer.call(name_or_pid, :segment_count)
  end

  @doc """
  Truncates the write-ahead log (WAL) after a specific index.

  ## Examples

      iex> MyApp.WAL.truncate_after(:my_wal, 10)
      :ok

  ## Parameters

    * `name_or_pid` - The name or process identifier of the WAL GenServer.
    * `index` - The index after which to truncate the WAL.
    * `timeout` (optional) - The timeout value in milliseconds (default: 5000).

  ## Returns

  - `:ok` - If the truncation is successful.
  - `{:error, :index_out_of_range}` - If the provided index is out of range.

  """
  @spec truncate_after(atom() | pid(), index(), non_neg_integer()) :: :ok | {:error, :index_out_of_range}
  def truncate_after(name_or_pid, index, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:truncate_after, index}, timeout)
  end

  @doc """
  Truncates the write-ahead log (WAL) before a specific index.

  ## Examples

      iex> MyApp.WAL.truncate_before(:my_wal, 10)
      :ok

  ## Parameters

    * `name_or_pid` - The name or process identifier of the WAL GenServer.
    * `index` - The index before which to truncate the WAL.
    * `timeout` (optional) - The timeout value in milliseconds (default: 5000).

  ## Returns

  - `:ok` - If the truncation is successful.
  - `{:error, :index_out_of_range}` - If the provided index is out of range.

  """
  @spec truncate_before(atom() | pid(), index(), non_neg_integer()) :: :ok | {:error, :index_out_of_range}
  def truncate_before(name_or_pid, index, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:truncate_before, index}, timeout)
  end

  @doc """
  Clears the write-ahead log (WAL) by removing all entries.

  ## Examples

      iex> MyApp.WAL.clear(:my_wal)
      :ok

  ## Parameters

    * `name_or_pid` - The name or process identifier of the WAL GenServer.

  ## Returns

  - `:ok` - If the clearing is successful.

  """
  @spec clear(atom() | pid()) :: :ok
  def clear(name_or_pid) do
    GenServer.cast(name_or_pid, :clear)
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

      {:ok, h} = Store.open(store, seg1_path, opts[:file_permission])

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
      # segment is in reverse order
      first_index = List.last(segments).index

      [%Segment{path: spath} = seg | t] = segments
      {:ok, h} = Store.open(store, spath, opts[:file_permission])

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

  def handle_call(:first_index, _, %__MODULE__{first_index: first_index} = state), do: {:reply, first_index, state}

  def handle_call(:segment_count, _, %__MODULE__{cold: cold} = state) do
    {:reply, :array.size(cold) + 1, state}
  end

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
    %Segment{index: begin_index, blocks: blocks, buf: _buf, caches: caches} = find_segment(index, state)

    %Block{offset: _offset, size: _size, data: data} = :array.get(index - begin_index, blocks)
    cache = :array.get(index - begin_index, caches)

    # ExWal.Debug.stacktrace(%{
    #   target_index: index,
    #   begin_index: begin_index,
    #   block_offset: offset,
    #   block_size: size,
    #   buf_size: byte_size(buf)
    # })

    # data = binary_part(buf, offset, size)
    # {data_size, _, data} = Uvarint.decode(data)
    # byte_size(data) == data_size || raise ArgumentError, "invalid data"

    {:reply, {:ok, Entry.new(index, data, cache)}, state}
  end

  def handle_call({:truncate_after, 0}, _from, state), do: {:reply, {:error, :index_out_of_range}, state}

  def handle_call({:truncate_after, index}, _from, %__MODULE__{first_index: first_index, last_index: last_index} = state)
      when index < first_index or index > last_index do
    {:reply, {:error, :index_out_of_range}, state}
  end

  def handle_call({:truncate_after, index}, _from, %__MODULE__{last_index: last_index} = state) when index == last_index,
    do: {:reply, :ok, state}

  def handle_call({:truncate_after, index}, _from, state) do
    {:reply, :ok, __truncate_after(index, state)}
  end

  def handle_call({:truncate_before, 0}, _from, state), do: {:reply, {:error, :index_out_of_range}, state}

  def handle_call({:truncate_before, _}, _from, %__MODULE__{last_index: 0} = state),
    do: {:reply, {:error, :index_out_of_range}, state}

  def handle_call({:truncate_before, index}, _from, %__MODULE__{last_index: last_index, first_index: first_index} = state)
      when index < first_index or index > last_index do
    {:reply, {:error, :index_out_of_range}, state}
  end

  def handle_call({:truncate_before, index}, _from, %__MODULE__{first_index: first_index} = state)
      when index == first_index,
      do: {:reply, :ok, state}

  def handle_call({:truncate_before, index}, _from, state) do
    {:reply, :ok, __truncate_before(index, state)}
  end

  @impl GenServer
  def handle_cast(
        :clear,
        %__MODULE__{
          data_path: data_path,
          lru_cache: lru,
          hot: %Segment{path: path},
          store: store,
          tail_store_handler: h,
          cold: cold,
          opts: opts
        } = state
      ) do
    # rm cache
    LRU.clear(lru)

    # rm hot
    :ok = Store.close(store, h)
    :ok = Store.rm(store, path)

    # rm cold
    if :array.size(cold) > 0 do
      Enum.each(0..(:array.size(cold) - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm(store, path)
      end)
    end

    # reinit
    seg1_path = Path.join(data_path, segment_filename(1))

    {:ok, h} = Store.open(store, seg1_path, opts[:file_permission])

    {:noreply,
     %__MODULE__{
       state
       | hot: %Segment{path: seg1_path, index: 1},
         cold: :array.new(),
         first_index: 1,
         last_index: 0,
         tail_store_handler: h
     }}
  end

  # ----------------- Private -----------------

  @spec load_segment(Segment.t(), Store.t()) :: Segment.t()
  #  index: begin_index,
  defp load_segment(%Segment{path: path} = seg, store) do
    {:ok, content} = Store.read_all(store, path)

    {bc, blocks} = parse_blocks(content, 0, 0, [])

    %Segment{
      seg
      | buf: content,
        blocks: :array.from_list(blocks),
        block_count: bc,
        caches: nil |> List.duplicate(bc) |> :array.from_list()
    }
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
          {block_count :: non_neg_integer(), blocks :: [Block.t()]}
  defp parse_blocks("", _, bc, blocks), do: {bc, Enum.reverse(blocks)}

  defp parse_blocks(data, since, bc, blocks) do
    {size, bytes_read, data} = Uvarint.decode(data)

    # check
    size == 0 && raise ArgumentError, "invalid block size"

    next = since + size + bytes_read
    <<bin::bytes-size(size), rest::binary>> = data

    parse_blocks(rest, next, bc + 1, [
      %Block{offset: since, size: size + bytes_read, data: bin} | blocks
    ])
  end

  @spec append_entry(Segment.t(), Entry.t()) :: {binary(), Segment.t()}
  defp append_entry(%Segment{buf: buf, index: begin_index, block_count: bc, blocks: blocks, caches: caches} = seg, %Entry{
         index: index,
         data: data,
         cache: cache
       }) do
    index == begin_index + bc ||
      raise ArgumentError,
            "invalid index, begin_index: #{begin_index}, bc: #{bc}, index: #{index}"

    data_with_size = Uvarint.encode(byte_size(data)) <> data

    {data_with_size,
     %Segment{
       seg
       | buf: <<buf::binary, data_with_size::binary>>,
         block_count: bc + 1,
         blocks: :array.set(bc, %Block{offset: byte_size(buf), size: byte_size(data_with_size), data: data}, blocks),
         caches: :array.set(bc, cache, caches)
     }}
  end

  @spec write_entries([Entry.t()], t()) :: t()
  defp write_entries([], %__MODULE__{tail_store_handler: h, opts: opts, store: store} = m) do
    unless opts[:nosync] do
      :ok = Store.sync(store, h)
    end

    m
  end

  defp write_entries([entry | t], %__MODULE__{hot: seg, opts: opts, store: store, tail_store_handler: h} = m) do
    {data, %Segment{buf: buf, index: begin_index, block_count: bc} = seg} =
      append_entry(seg, entry)

    :ok = Store.append(store, h, data)

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

    {:ok, h} = Store.open(store, new_seg.path, opts[:file_permission])

    size = :array.size(cold)

    %__MODULE__{
      m
      | tail_store_handler: h,
        hot: new_seg,
        cold: :array.set(size, %Segment{seg | blocks: nil, caches: nil, buf: ""}, cold),
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

  defp do_search(cold, target_index, min, max) when min == max do
    %Segment{index: index, block_count: bc} = :array.get(min, cold)

    if target_index >= index and target_index < index + bc do
      min
    end
  end

  defp do_search(cold, target_index, min, max) do
    mid = div(min + max, 2)
    %Segment{index: index, block_count: bc} = :array.get(mid, cold)

    cond do
      target_index < index ->
        do_search(cold, target_index, min, mid)

      target_index >= index and target_index < index + bc ->
        mid

      true ->
        do_search(cold, target_index, mid + 1, max)
    end
  end

  @spec __truncate_after(index(), t()) :: t()
  defp __truncate_after(
         index,
         %__MODULE__{
           hot: %Segment{index: begin_index, blocks: blocks, buf: buf, path: path} = seg,
           data_path: data_path,
           store: store,
           tail_store_handler: h
         } = state
       )
       when index >= begin_index do
    # truncate buf and blocks
    truncate_idx = index - begin_index + 1

    new_seg =
      if truncate_idx < :array.size(blocks) do
        %Block{offset: offset} = :array.get(truncate_idx, blocks)
        buf = binary_part(buf, 0, offset)
        blocks = :array.resize(truncate_idx, blocks)

        %Segment{
          seg
          | buf: buf,
            block_count: truncate_idx - 1,
            blocks: blocks
        }
      else
        seg
      end

    # truncate file
    temp_file = Path.join(data_path, "tmp")
    :ok = Store.write_all(store, temp_file, buf)

    # swap the tmp file with the old segment file
    :ok = Store.close(store, h)
    :ok = Store.rename(store, temp_file, path)

    # reopen tail handler
    {:ok, h} = Store.open(store, path, state.opts[:file_permission])

    # no need to gc blocks, reset block_count is all we need
    %__MODULE__{
      state
      | hot: new_seg,
        last_index: index,
        tail_store_handler: h
    }
  end

  defp __truncate_after(
         index,
         %__MODULE__{
           cold: cold,
           store: store,
           data_path: data_path,
           tail_store_handler: h,
           lru_cache: lru,
           hot: %Segment{path: hot_path}
         } = state
       ) do
    # find segment by index
    idx = bin_search(cold, index)

    %Segment{buf: buf, blocks: blocks, index: begin_index, path: path} =
      seg =
      idx
      |> :array.get(cold)
      |> load_segment(store)

    # truncate buf and blocks
    truncate_idx = index - begin_index + 1

    new_seg =
      if truncate_idx < :array.size(blocks) do
        %Block{offset: offset} = :array.get(truncate_idx, blocks)
        buf = binary_part(buf, 0, offset)
        blocks = :array.resize(truncate_idx, blocks)

        %Segment{
          seg
          | buf: buf,
            block_count: truncate_idx - 1,
            blocks: blocks
        }
      else
        seg
      end

    # create tmp file to store new buf
    temp_file = Path.join(data_path, "tmp")
    :ok = Store.write_all(store, temp_file, buf)

    # swap the tmp file with the old segment file
    :ok = Store.rename(store, temp_file, path)

    # cleanup
    # all segments and cache after the new segment should be deleted
    :ok = LRU.clear(lru)

    if idx + 1 < :array.size(cold) do
      Enum.each((idx + 1)..(:array.size(cold) - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm(store, path)
      end)
    end

    :ok = Store.close(store, h)
    :ok = Store.rm(store, hot_path)

    # reopen tail handler
    {:ok, h} = Store.open(store, path, state.opts[:file_permission])

    %__MODULE__{
      state
      | hot: new_seg,
        cold: :array.resize(idx, cold),
        tail_store_handler: h,
        last_index: index
    }
  end

  @spec __truncate_before(index(), t()) :: t()
  defp __truncate_before(
         index,
         %__MODULE__{
           hot: %Segment{index: begin_index, blocks: blocks, buf: buf, path: path},
           cold: cold,
           store: store,
           data_path: data_path,
           tail_store_handler: h,
           lru_cache: lru
         } = state
       )
       when index >= begin_index do
    %Block{offset: offset} = :array.get(index - begin_index, blocks)

    new_buf = binary_part(buf, offset, byte_size(buf) - offset)
    {bc, blocks} = parse_blocks(new_buf, 0, 0, [])

    # remake last segment
    new_seg = %Segment{
      index: index,
      buf: new_buf,
      path: Path.join(data_path, segment_filename(index)),
      block_count: bc,
      blocks: :array.from_list(blocks),
      caches: nil |> List.duplicate(bc) |> :array.from_list()
    }

    # rewrite file
    # make a tmp file to store new buf
    temp_file = Path.join(data_path, "tmp")
    :ok = Store.write_all(store, temp_file, new_seg.buf)

    # swap the tmp file with the old segment file
    :ok = Store.close(store, h)
    :ok = Store.rename(store, temp_file, new_seg.path)
    :ok = Store.rm(store, path)

    # delete all segment files after the new segment
    cold_size = :array.size(cold)

    if cold_size > 0 do
      # Task.async_stream(0..(cold_size - 1), fn i ->
      #   %Segment{path: path} = :array.get(i, cold)
      #   :ok = Store.rm(store, path)
      #   :ok = LRU.delete(lru, path)
      # end)
      # |> Stream.run()
      Enum.each(0..(cold_size - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm(store, path)
        :ok = LRU.delete(lru, path)
      end)
    end

    # reopen tail handler
    {:ok, h} = Store.open(store, new_seg.path, state.opts[:file_permission])

    %__MODULE__{state | hot: new_seg, first_index: index, cold: :array.new(), tail_store_handler: h}
  end

  defp __truncate_before(
         index,
         %__MODULE__{hot: %Segment{index: begin_index}, cold: cold, store: store, data_path: data_path, lru_cache: lru} =
           state
       )
       when index < begin_index do
    idx = bin_search(cold, index)

    %Segment{buf: buf, blocks: blocks, index: begin_index} =
      idx
      |> :array.get(cold)
      |> load_segment(store)

    %Block{offset: offset} = :array.get(index - begin_index, blocks)

    # remake segment

    new_seg = %Segment{
      index: index,
      buf: binary_part(buf, offset, byte_size(buf) - offset),
      path: Path.join(data_path, segment_filename(index)),
      block_count: :array.size(blocks) - (index - begin_index)
    }

    # clear cache
    LRU.clear(lru)

    # delete all segment files and cache before the new segment
    Enum.each(0..idx, fn i ->
      %Segment{path: path} = :array.get(i, cold)
      :ok = Store.rm(store, path)
    end)

    # rewrite file
    # make a tmp file to store new buf
    temp_file = Path.join(data_path, "tmp")
    :ok = Store.write_all(store, temp_file, new_seg.buf)

    # swap the tmp file with the old segment file
    :ok = Store.rename(store, temp_file, new_seg.path)

    cold_size = :array.size(cold)

    new_cold =
      cold |> array_slice(idx..(cold_size - 1)) |> then(fn x -> :array.set(0, %Segment{new_seg | buf: ""}, x) end)

    %__MODULE__{state | first_index: index, cold: new_cold}
  end

  @spec array_slice(:array.array(), Range.t()) :: :array.array()
  defp array_slice(array, range) do
    for_result =
      for i <- range do
        :array.get(i, array)
      end

    :array.from_list(for_result)
  end
end
