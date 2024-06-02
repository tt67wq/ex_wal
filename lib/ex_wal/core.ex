defmodule ExWal.Core do
  @moduledoc false

  use GenServer

  alias ExWal.LRU
  alias ExWal.Models.Block
  alias ExWal.Models.Config
  alias ExWal.Models.Entry
  alias ExWal.Models.Segment
  alias ExWal.Store
  alias ExWal.Typespecs
  alias ExWal.Uvarint

  require Logger

  @store_impl ExWal.Store.File

  defstruct data_path: "",
            store_name: nil,
            hot: %Segment{},
            cold: :array.new(),
            first_index: -1,
            last_index: -1,
            lru_cache: nil,
            tail_store_handler: nil,
            opts: [
              nosync: false,
              segment_size: 64 * 1024 * 1024
            ]

  @type t :: %__MODULE__{
          data_path: String.t(),
          store_name: Typespecs.name(),
          hot: Segment.t(),
          cold: :array.array(),
          first_index: integer(),
          last_index: integer(),
          lru_cache: Typespecs.name(),
          tail_store_handler: Typespecs.handler(),
          opts: [
            nosync: boolean(),
            segment_size: non_neg_integer()
          ]
        }

  @doc """
  Start a WAL process

  ## Options
  #{Config.doc()}
  """
  @spec start_link({Config.wal_option_schema_t(), Typespecs.name(), Typespecs.name(), Typespecs.name()}) ::
          GenServer.on_start()
  def start_link({config, lru_name, store_name, name}) do
    config = Config.validate!(config)
    GenServer.start_link(__MODULE__, {config, lru_name, store_name, name}, name: name)
  end

  @doc """
  Stop a WAL process
  """
  @spec stop(Typespecs.name()) :: :ok
  def stop(name_or_pid) do
    GenServer.stop(name_or_pid)
  end

  @spec write(Typespecs.name(), [Entry.t()], non_neg_integer()) :: :ok
  def write(name_or_pid, entries, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:write, entries}, timeout)
  end

  @spec read(Typespecs.name(), Typespecs.index()) :: {:ok, Entry.t()} | {:error, :index_not_found}
  def read(name_or_pid, index, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:read, index}, timeout)
  end

  @spec last_index(Typespecs.name()) :: Typespecs.index()
  def last_index(name_or_pid) do
    GenServer.call(name_or_pid, :last_index)
  end

  @spec first_index(Typespecs.name()) :: Typespecs.index()
  def first_index(name_or_pid) do
    GenServer.call(name_or_pid, :first_index)
  end

  @spec segment_count(Typespecs.name()) :: non_neg_integer()
  def segment_count(name_or_pid) do
    GenServer.call(name_or_pid, :segment_count)
  end

  @spec truncate_after(Typespecs.name(), Typespecs.index(), non_neg_integer()) :: :ok | {:error, :index_out_of_range}
  def truncate_after(name_or_pid, index, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:truncate_after, index}, timeout)
  end

  @spec truncate_before(Typespecs.name(), Typespecs.index(), non_neg_integer()) :: :ok | {:error, :index_out_of_range}
  def truncate_before(name_or_pid, index, timeout \\ 5000) do
    GenServer.call(name_or_pid, {:truncate_before, index}, timeout)
  end

  @spec sync(Typespecs.name()) :: :ok
  def sync(name_or_pid) do
    GenServer.call(name_or_pid, :sync)
  end

  @spec reinit(Typespecs.name()) :: :ok
  def reinit(name_or_pid) do
    GenServer.call(name_or_pid, :reinit)
  end

  @spec clear(Typespecs.name()) :: :ok
  def clear(name_or_pid) do
    GenServer.call(name_or_pid, :clear)
  end

  # ----------------- Server  -----------------

  @impl GenServer
  def init({config, lru_name, store_name, _}) do
    path = Path.absname(config[:path])
    :ok = Store.mkdir({@store_impl, store_name}, path)

    segments =
      path
      |> Path.join("*")
      |> then(fn x -> Store.wildcard({@store_impl, store_name}, x) end)
      |> Enum.reject(fn x -> Store.dir?({@store_impl, store_name}, x) end)
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
      {:ok, h} = Store.open({@store_impl, store_name}, seg1_path)

      {:ok,
       %__MODULE__{
         data_path: path,
         store_name: store_name,
         hot: %Segment{path: seg1_path, index: 0},
         cold: :array.new(),
         first_index: -1,
         last_index: -1,
         lru_cache: lru_name,
         tail_store_handler: h,
         opts: [
           nosync: config[:nosync],
           segment_size: config[:segment_size]
         ]
       }}
    else
      # segment is in reverse order
      %Segment{index: first_index} = List.last(segments)

      [%Segment{path: spath, index: begin_index, block_count: bc} = seg | t] = segments
      {:ok, h} = Store.open({@store_impl, store_name}, spath)

      {:ok,
       %__MODULE__{
         data_path: path,
         store_name: store_name,
         hot: seg,
         cold: :array.from_list(t),
         first_index: first_index,
         last_index: begin_index + bc - 1,
         lru_cache: lru_name,
         tail_store_handler: h,
         opts: [
           nosync: config[:nosync],
           segment_size: config[:segment_size]
         ]
       }, {:continue, :load_segment}}
    end
  end

  @impl GenServer
  def terminate(_reason, %__MODULE__{store_name: store_name, tail_store_handler: h}) do
    Store.sync({@store_impl, store_name}, h)
    Store.close({@store_impl, store_name}, h)
  end

  @impl GenServer
  def handle_continue(:load_segment, %__MODULE__{hot: seg} = state) do
    %Segment{block_count: bc, index: begin_index} =
      seg = load_segment(seg, state)

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

  def handle_call({:truncate_after, -1}, _from, state), do: {:reply, :ok, do_reinit(state)}

  def handle_call({:truncate_after, index}, _from, %__MODULE__{first_index: first_index, last_index: last_index} = state)
      when index < first_index or index > last_index do
    {:reply, {:error, :index_out_of_range}, state}
  end

  def handle_call({:truncate_after, index}, _from, %__MODULE__{last_index: last_index} = state) when index == last_index,
    do: {:reply, :ok, state}

  def handle_call({:truncate_after, index}, _from, state) do
    {:reply, :ok, __truncate_after(index, state)}
  end

  def handle_call({:truncate_before, -1}, _from, state), do: {:reply, {:error, :index_out_of_range}, state}

  def handle_call({:truncate_before, _}, _from, %__MODULE__{last_index: -1} = state),
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

  def handle_call(:sync, _, state) do
    %__MODULE__{tail_store_handler: h, store_name: store_name} = state
    {:reply, Store.sync({@store_impl, store_name}, h), state}
  end

  def handle_call(:clear, _, state) do
    state = do_clear(state)
    {:reply, :ok, state}
  end

  def handle_call(:reinit, _, state) do
    state = do_reinit(state)
    {:reply, :ok, state}
  end

  # ----------------- Private -----------------

  # @spec load_segment(Segment.t()) :: Segment.t()
  defp load_segment(%Segment{path: path} = seg, %__MODULE__{store_name: store_name}) do
    {:ok, content} = Store.read_all({@store_impl, store_name}, path)

    {bc, blocks} = parse_blocks(content, 0, 0, [])

    %Segment{
      seg
      | buf: content,
        blocks: :array.from_list(blocks),
        block_count: bc,
        caches: nil |> List.duplicate(bc) |> :array.from_list()
    }
  end

  @spec parse_segment_filename(String.t()) :: Typespecs.index()
  defp parse_segment_filename(filename) do
    filename
    |> String.trim_leading("0")
    |> String.to_integer()
  end

  @spec segment_filename(Typespecs.index()) :: String.t()
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
  defp write_entries([], state) do
    %__MODULE__{tail_store_handler: h, opts: opts, store_name: store_name} = state

    unless opts[:nosync] do
      :ok = Store.sync({@store_impl, store_name}, h)
    end

    state
  end

  defp write_entries([entry | t], state) do
    %__MODULE__{hot: seg, opts: opts, tail_store_handler: h, store_name: store_name} = state

    {data, %Segment{buf: buf, index: begin_index, block_count: bc} = seg} =
      append_entry(seg, entry)

    :ok = Store.append({@store_impl, store_name}, h, data)

    state = %__MODULE__{state | hot: seg, last_index: begin_index + bc - 1}

    if byte_size(buf) < opts[:segment_size] do
      write_entries(t, state)
    else
      state = cycle(state)
      write_entries(t, state)
    end
  end

  # Cycle the old segment for a new segment.
  @spec cycle(t()) :: t()
  defp cycle(
         %__MODULE__{
           data_path: data_path,
           tail_store_handler: h,
           lru_cache: lru,
           last_index: last_index,
           hot: %Segment{path: path} = seg,
           store_name: store_name,
           cold: cold
         } = m
       ) do
    # :ok = sync(h)
    # :ok = close(h)
    :ok = Store.sync({@store_impl, store_name}, h)
    :ok = Store.close({@store_impl, store_name}, h)

    :ok = LRU.put(lru, path, seg)

    new_seg = %Segment{
      path: Path.join(data_path, segment_filename(last_index + 1)),
      index: last_index + 1
    }

    {:ok, h} = Store.open({@store_impl, store_name}, new_seg.path)

    size = :array.size(cold)

    %__MODULE__{
      m
      | tail_store_handler: h,
        hot: new_seg,
        cold: :array.set(size, %Segment{seg | blocks: nil, caches: nil, buf: ""}, cold),
        last_index: last_index
    }
  end

  @spec find_segment(index :: Typespecs.index(), t()) :: Segment.t()
  defp find_segment(index, %__MODULE__{hot: %Segment{index: last_begin_index} = seg} = state) do
    if index >= last_begin_index do
      seg
    else
      find_cold_segment(index, state)
    end
  end

  @spec find_cold_segment(Typespecs.index(), t()) :: Segment.t()
  defp find_cold_segment(index, %__MODULE__{lru_cache: lru, cold: cold} = state) do
    lru
    |> LRU.select(fn %Segment{index: x, block_count: bc} ->
      index >= x and index < x + bc
    end)
    |> case do
      nil ->
        cold
        |> bin_search(index)
        |> :array.get(cold)
        |> load_segment(state)
        |> tap(fn %Segment{path: path} = x -> LRU.put(lru, path, x) end)

      seg ->
        seg
    end
  end

  @spec bin_search(:array.array(), Typespecs.index()) :: non_neg_integer() | nil
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

  @spec __truncate_after(Typespecs.index(), t()) :: t()
  defp __truncate_after(index, %__MODULE__{hot: %Segment{index: begin_index}} = state) when index >= begin_index do
    %__MODULE__{
      hot: %Segment{blocks: blocks, buf: buf, path: path} = seg,
      data_path: data_path,
      tail_store_handler: h,
      store_name: store_name
    } = state

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
    :ok = Store.write_all({@store_impl, store_name}, temp_file, buf)

    # swap the tmp file with the old segment file
    :ok = Store.close({@store_impl, store_name}, h)
    :ok = Store.rename({@store_impl, store_name}, temp_file, path)

    # reopen tail handler
    {:ok, h} = Store.open({@store_impl, store_name}, path)

    # no need to gc blocks, reset block_count is all we need
    %__MODULE__{
      state
      | hot: new_seg,
        last_index: index,
        tail_store_handler: h
    }
  end

  defp __truncate_after(index, state) do
    %__MODULE__{
      cold: cold,
      data_path: data_path,
      tail_store_handler: h,
      lru_cache: lru,
      hot: %Segment{path: hot_path},
      store_name: store_name
    } = state

    # find segment by index
    idx = bin_search(cold, index)

    %Segment{buf: buf, blocks: blocks, index: begin_index, path: path} =
      seg =
      idx
      |> :array.get(cold)
      |> load_segment(state)

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
    :ok = Store.write_all({@store_impl, store_name}, temp_file, buf)

    # swap the tmp file with the old segment file
    :ok = Store.rename({@store_impl, store_name}, temp_file, path)

    # cleanup
    # all segments and cache after the new segment should be deleted
    :ok = LRU.clear(lru)

    if idx + 1 < :array.size(cold) do
      Enum.each((idx + 1)..(:array.size(cold) - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm({@store_impl, store_name}, path)
      end)
    end

    :ok = Store.close({@store_impl, store_name}, h)
    :ok = Store.rm({@store_impl, store_name}, hot_path)

    # reopen tail handler
    {:ok, h} = Store.open({@store_impl, store_name}, path)

    %__MODULE__{
      state
      | hot: new_seg,
        cold: :array.resize(idx, cold),
        tail_store_handler: h,
        last_index: index
    }
  end

  @spec __truncate_before(Typespecs.index(), t()) :: t()
  defp __truncate_before(index, %__MODULE__{hot: %Segment{index: begin_index}} = state) when index >= begin_index do
    %__MODULE__{
      hot: %Segment{blocks: blocks, buf: buf, path: path},
      cold: cold,
      data_path: data_path,
      tail_store_handler: h,
      lru_cache: lru,
      store_name: store_name
    } = state

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

    temp_file = Path.join(data_path, "tmp")

    with do
      # rewrite file
      # make a tmp file to store new buf
      :ok = Store.write_all({@store_impl, store_name}, temp_file, new_seg.buf)
    end

    # swap the tmp file with the old segment file
    with do
      :ok = Store.close({@store_impl, store_name}, h)
      :ok = Store.rename({@store_impl, store_name}, temp_file, new_seg.path)
      :ok = Store.rm({@store_impl, store_name}, path)
    end

    # delete all segment files after the new segment
    cold_size = :array.size(cold)

    if cold_size > 0 do
      Enum.each(0..(cold_size - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm({@store_impl, store_name}, path)
        :ok = LRU.delete(lru, path)
      end)
    end

    # reopen tail handler
    {:ok, h} = Store.open({@store_impl, store_name}, new_seg.path)

    %__MODULE__{state | hot: new_seg, first_index: index, cold: :array.new(), tail_store_handler: h}
  end

  defp __truncate_before(index, %__MODULE__{hot: %Segment{index: begin_index}} = state) when index < begin_index do
    %__MODULE__{cold: cold, data_path: data_path, lru_cache: lru, store_name: store_name} = state
    idx = bin_search(cold, index)

    %Segment{buf: buf, blocks: blocks, index: begin_index} =
      idx
      |> :array.get(cold)
      |> load_segment(state)

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
      :ok = Store.rm({@store_impl, store_name}, path)
    end)

    # rewrite file
    # make a tmp file to store new buf
    # swap the tmp file with the old segment file
    temp_file = Path.join(data_path, "tmp")

    with do
      :ok = Store.write_all({@store_impl, store_name}, temp_file, new_seg.buf)
      :ok = Store.rename({@store_impl, store_name}, temp_file, new_seg.path)
    end

    cold_size = :array.size(cold)

    new_cold =
      cold |> array_slice(idx..(cold_size - 1)) |> then(fn x -> :array.set(0, %Segment{new_seg | buf: ""}, x) end)

    %__MODULE__{state | first_index: index, cold: new_cold}
  end

  defp do_reinit(
         %__MODULE__{
           store_name: store_name,
           data_path: data_path,
           lru_cache: lru,
           hot: %Segment{path: path},
           tail_store_handler: h,
           cold: cold
         } = state
       ) do
    # rm cache
    LRU.clear(lru)

    # rm hot
    with do
      :ok = Store.close({@store_impl, store_name}, h)
      :ok = Store.rm({@store_impl, store_name}, path)
    end

    # rm cold
    if :array.size(cold) > 0 do
      Enum.each(0..(:array.size(cold) - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm({@store_impl, store_name}, path)
      end)
    end

    # reinit
    seg1_path = Path.join(data_path, segment_filename(1))

    {:ok, h} = Store.open({@store_impl, store_name}, seg1_path)

    %__MODULE__{
      state
      | hot: %Segment{path: seg1_path, index: 0},
        cold: :array.new(),
        first_index: -1,
        last_index: -1,
        tail_store_handler: h
    }
  end

  defp do_clear(%__MODULE__{
         store_name: store_name,
         lru_cache: lru,
         hot: %Segment{path: path},
         tail_store_handler: h,
         cold: cold
       }) do
    # rm cache
    LRU.clear(lru)

    # rm hot
    with do
      :ok = Store.close({@store_impl, store_name}, h)
      :ok = Store.rm({@store_impl, store_name}, path)
    end

    # rm cold
    if :array.size(cold) > 0 do
      Enum.each(0..(:array.size(cold) - 1), fn i ->
        %Segment{path: path} = :array.get(i, cold)
        :ok = Store.rm({@store_impl, store_name}, path)
      end)
    end

    :ok
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
