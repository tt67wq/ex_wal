defmodule ExWal.Manager.Standalone do
  @moduledoc false

  use GenServer, restart: :transient

  alias ExWal.FS
  alias ExWal.Models
  alias ExWal.Recycler

  @type t :: %__MODULE__{
          name: GenServer.name(),
          recycler: ExWal.Recycler.t(),
          dynamic_sup: atom(),
          registry: atom(),
          fs: ExWal.FS.t(),
          dirname: binary(),
          queue: [Models.VirtualLog.log_num()]
        }

  defstruct name: nil, recycler: nil, dynamic_sup: nil, registry: nil, fs: nil, dirname: "", queue: []

  @spec start_link({
          name :: GenServer.name(),
          recycler :: ExWal.Recycler.t(),
          dynamic_sup :: atom(),
          registry :: atom(),
          fs :: ExWal.FS.t(),
          dirname :: binary()
        }) :: GenServer.on_start()
  def start_link({name, recycler, dynamic_sup, registry, fs, dirname}) do
    GenServer.start_link(__MODULE__, {name, recycler, dynamic_sup, registry, fs, dirname}, name: name)
  end

  @spec stop(GenServer.name()) :: :ok | {:error, reason :: any()}
  def stop(name) do
    GenServer.stop(name)
  end

  @spec get(name :: GenServer.name()) :: t()
  def get(name), do: GenServer.call(name, :get_self)

  @spec create(name :: GenServer.name(), log_num :: ExWal.Models.VirtualLog.log_num()) ::
          {:ok, ExWal.LogWriter.t()} | {:error, reason :: any()}
  def create(name, log_num) do
    GenServer.call(name, {:create, log_num})
  end

  @spec obsolete(
          name :: GenServer.name(),
          min_log_num :: ExWal.Models.VirtualLog.log_num(),
          recycle? :: boolean()
        ) ::
          :ok | {:error, reason :: any()}
  def obsolete(name, min_log_num, recycle?) do
    GenServer.call(name, {:obsolete, min_log_num, recycle?})
  end

  @spec list(name :: GenServer.name()) :: {:ok, [ExWal.Models.VirtualLog.t()]} | {:error, reason :: any()}
  def list(name) do
    GenServer.call(name, :list)
  end

  @spec open_for_read(name :: GenServer.name(), log_num :: ExWal.Models.VirtualLog.log_num()) ::
          {:ok, ExWal.LogReader.t()} | {:error, reason :: any()}
  def open_for_read(name, log_num) do
    GenServer.call(name, {:open_for_read, log_num})
  end

  # --------------------- server -----------------

  @impl GenServer
  def init({name, recycler, dynamic_sup, registry, fs, dirname}) do
    {:ok,
     %__MODULE__{
       name: name,
       recycler: recycler,
       dynamic_sup: dynamic_sup,
       registry: registry,
       fs: fs,
       dirname: dirname
     }, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    %__MODULE__{dirname: dirname, fs: fs, recycler: recycler} = state
    {:ok, files} = FS.list(fs, dirname)

    files
    |> Enum.map(fn f -> Models.VirtualLog.parse_filename(f) end)
    |> Enum.each(fn {log_num, _} -> Recycler.add(recycler, log_num) end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:get_self, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:create, log_num}, _from, state) do
    %__MODULE__{dirname: dirname, registry: registry, dynamic_sup: dynamic_sup, queue: q} = state
    log_name = Path.join(dirname, Models.VirtualLog.filename(log_num, 0))
    {:ok, file} = create_or_reuse(log_name, state)
    writer_name = {:via, Registry, {registry, {:writer, log_num}}}

    {:ok, _} = DynamicSupervisor.start_child(dynamic_sup, {ExWal.LogWriter.Single, {writer_name, file, log_num}})
    {:reply, {:ok, ExWal.LogWriter.Single.get(writer_name)}, %__MODULE__{state | queue: [log_num | q]}}
  end

  def handle_call({:obsolete, min_log_num, true}, _from, state) do
    %__MODULE__{recycler: recycler, queue: q} = state

    q
    |> Enum.filter(fn log_num -> log_num < min_log_num end)
    |> Enum.each(fn log_num -> Recycler.add(recycler, log_num) end)

    queue = Enum.reject(q, fn log_num -> log_num < min_log_num end)
    {:reply, :ok, %__MODULE__{state | queue: queue}}
  end

  def handle_call({:obsolete, min_log_num, false}, _from, state) do
    %__MODULE__{queue: q} = state
    {:reply, :ok, %__MODULE__{state | queue: Enum.reject(q, fn log_num -> log_num < min_log_num end)}}
  end

  def handle_call(:list, _from, state) do
    %__MODULE__{queue: q, dirname: dirname} = state

    logs =
      Enum.map(q, fn log_num ->
        %Models.VirtualLog{log_num: log_num, segments: [%Models.Segment{index: 0, dir: dirname}]}
      end)

    {:reply, {:ok, logs}, state}
  end

  def handle_call({:open_for_read, log_num}, _from, state) do
    %__MODULE__{queue: q} = state
    {:reply, start_log_reader(log_num, Enum.member?(q, log_num), state), state}
  end

  defp start_log_reader(log_num, log_exists?, state)

  defp start_log_reader(_log_num, false, _state), do: {:error, :not_found}

  defp start_log_reader(log_num, true, state) do
    %__MODULE__{registry: registry, dynamic_sup: dynamic_sup, dirname: dirname, fs: fs} = state
    filepath = Path.join(dirname, Models.VirtualLog.filename(log_num, 0))
    reader_name = {:via, Registry, {registry, {:single_reader, log_num}}}

    {:ok, reader_pid} =
      DynamicSupervisor.start_child(dynamic_sup, {ExWal.LogReader.Single, {reader_name, log_num, filepath, fs}})

    {:ok, ExWal.LogReader.Single.get(reader_pid)}
  end

  defp create_or_reuse(log_name, %__MODULE__{fs: fs, recycler: recycler, dirname: dirname}) do
    recycler
    |> Recycler.pop()
    |> case do
      nil ->
        FS.create(fs, log_name)

      recycled_log ->
        old_name = Path.join(dirname, Models.VirtualLog.filename(recycled_log, 0))
        FS.reuse_for_write(fs, old_name, log_name)
    end
  end
end

defimpl ExWal.Manager, for: ExWal.Manager.Standalone do
  alias ExWal.Manager.Standalone

  @spec list(ExWal.Manager.t()) :: {:ok, [ExWal.Models.VirtualLog.t()]} | {:error, reason :: any()}
  def list(%Standalone{name: name}), do: Standalone.list(name)

  @spec obsolete(impl :: ExWal.Manager.t(), min_log_num :: ExWal.Models.VirtualLog.log_num(), recycle? :: boolean()) ::
          :ok | {:error, reason :: any()}
  def obsolete(%Standalone{name: name}, min_log_num, recycle?), do: Standalone.obsolete(name, min_log_num, recycle?)

  @spec create(impl :: ExWal.Manager.t(), log_num :: ExWal.Models.VirtualLog.log_num()) ::
          {:ok, ExWal.LogWriter.t()} | {:error, reason :: any()}
  def create(%Standalone{name: name}, log_num), do: Standalone.create(name, log_num)

  @spec close(impl :: ExWal.Manager.t()) :: :ok | {:error, reason :: any()}
  def close(%Standalone{name: name}), do: Standalone.stop(name)
end
