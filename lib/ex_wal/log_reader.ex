defprotocol ExWal.LogReader do
  @spec next(t()) :: {:ok, bytes :: binary(), last? :: boolean()} | :eof | {:error, reason :: term()}
  def next(impl)
end

defmodule ExWal.LogReader.Single do
  @moduledoc false

  use Agent

  alias ExWal.Constant.Record
  alias ExWal.Models
  alias ExWal.Models.RecyclableRecord

  require ExWal.Constant.Record

  @legacy_header_size Record.legacy_header_size()
  @block_size Record.block_size()
  @fs %ExWal.FS.Default{}

  @type t :: %__MODULE__{
          name: Agent.name(),
          log_num: non_neg_integer(),
          file: ExWal.File.t(),
          buf: binary()
        }

  defstruct name: nil, log_num: 0, file: nil, buf: <<>>

  def start_link({name, log_num, file_path}) do
    Agent.start_link(__MODULE__, :init, [name, log_num, file_path], name: name)
  end

  @spec get(Agent.name()) :: t()
  def get(name), do: Agent.get(name, fn state -> state end)

  @spec next(Agent.name()) :: {:ok, bytes :: binary(), last? :: boolean()} | :eof | {:error, reason :: term()}
  def next(name) do
    Agent.get_and_update(name, __MODULE__, :handle_next, [])
  end

  def stop(name) do
    Agent.get(name, __MODULE__, :handle_closed, [])
    Agent.stop(name)
  end

  # ------------- handlers -------------
  def init(name, log_num, file_path) do
    {:ok, file} = ExWal.FS.open(@fs, file_path, [])
    %__MODULE__{name: name, log_num: log_num, file: file}
  end

  def handle_next(state)

  def handle_next(%__MODULE__{buf: <<>>} = state) do
    %__MODULE__{file: file} = state

    file
    |> ExWal.File.read(@block_size)
    |> case do
      {:ok, buf} ->
        handle_next(%__MODULE__{state | buf: buf})

      :eof ->
        {:eof, state}

      err ->
        {err, state}
    end
  end

  def handle_next(%__MODULE__{buf: buf} = state) when byte_size(buf) > @legacy_header_size do
    buf
    |> Models.RecyclableRecord.parse()
    |> case do
      {:ok, %Models.RecyclableRecord{payload: p} = rec, rest} ->
        {{:ok, p, RecyclableRecord.last_chunk?(rec)}, %__MODULE__{state | buf: rest}}

      {:ok, nil, _} ->
        handle_next(%__MODULE__{state | buf: <<>>})

      err ->
        {err, state}
    end
  end

  def handle_closed(%__MODULE__{file: file}) do
    ExWal.File.close(file)
  end
end

defmodule ExWal.LogReader.Virtual do
  @moduledoc false

  alias ExWal.Models.Segment
  alias ExWal.Models.VirtualLog

  defstruct name: nil, dynamic: nil, registry: nil, virtual_log: nil, reader: nil

  def start_link({name, dynamic, registry, vlog}) do
    Agent.start_link(__MODULE__, :init, [name, dynamic, registry, vlog], name: name)
  end

  def get(name), do: Agent.get(name, fn state -> state end)

  @spec next(Agent.name()) :: {:ok, binary()} | :eof | {:error, reason :: term()}
  def next(name), do: Agent.get_and_update(name, __MODULE__, :handle_next, [])

  # ---------------- handlers ----------------

  def init(name, dynamic, registry, vlog) do
    %__MODULE__{name: name, dynamic: dynamic, registry: registry, virtual_log: vlog}
  end

  def handle_next(state)

  def handle_next(%__MODULE__{reader: reader} = state) do
    reader
    |> ExWal.LogReader.next()
    |> case do
      {:ok, p, last?} ->
        {{:ok, p, last?}, state}

      :eof ->
        may_handle_next(state)

      err ->
        {err, state}
    end
  end

  defp next_file(%__MODULE__{reader: nil, virtual_log: %VirtualLog{segments: []}}), do: {:error, :eof}

  defp next_file(%__MODULE__{reader: nil} = state) do
    %__MODULE__{virtual_log: vlog, dynamic: dynamic, registry: registry} = state
    %VirtualLog{log_num: log_num, segments: [%Segment{index: index, dir: dir} | segs]} = vlog
    filename = VirtualLog.filename(log_num, index)
    name = {:via, Registry, {registry, {:reader, filename}}}
    {:ok, _} = DynamicSupervisor.start_child(dynamic, {ExWal.LogReader.Single, {name, log_num, Path.join(dir, filename)}})
    vlog = %VirtualLog{vlog | segments: segs}
    {:ok, %__MODULE__{state | reader: name, virtual_log: vlog}}
  end

  defp next_file(%__MODULE__{reader: reader} = state) do
    ExWal.LogReader.Single.stop(reader)
    next_file(%__MODULE__{state | reader: nil})
  end

  defp may_handle_next(state) do
    state
    |> next_file()
    |> case do
      {:ok, state} -> handle_next(state)
      err -> {err, state}
    end
  end
end

defimpl ExWal.LogReader, for: ExWal.LogReader.Single do
  alias ExWal.LogReader.Single

  def next(%Single{name: name}) do
    Single.next(name)
  end
end

defimpl ExWal.LogReader, for: ExWal.LogReader.Virtual do
  alias ExWal.LogReader.Virtual

  def next(%Virtual{name: name}) do
    Virtual.next(name)
  end
end
