defmodule ExWal.Manager.Standalone do
  @moduledoc false

  use GenServer, restart: :transient

  alias ExWal.FS
  alias ExWal.Manager.Options
  alias ExWal.Models
  alias ExWal.Models.Deletable
  alias ExWal.Models.VirtualLog
  alias ExWal.Recycler

  @type t :: %__MODULE__{
          name: GenServer.name(),
          recycler: ExWal.Recycler.t(),
          dynamic_sup: atom(),
          registry: atom(),
          fs: ExWal.FS.t(),
          dirname: binary(),
          queue: [Models.VirtualLog.t()],
          initial_obsolete: [Models.Deletable.t()]
        }

  defstruct name: nil,
            recycler: nil,
            dynamic_sup: nil,
            registry: nil,
            fs: nil,
            dirname: "",
            queue: [],
            initial_obsolete: []

  @spec start_link({
          name :: GenServer.name(),
          recycler :: ExWal.Recycler.t(),
          dynamic_sup :: GenServer.name(),
          registry :: GenServer.name(),
          opts :: Options.t()
        }) :: GenServer.on_start()
  def start_link({name, recycler, dynamic_sup, registry, opts}) do
    GenServer.start_link(
      __MODULE__,
      {name, recycler, dynamic_sup, registry, opts},
      name: name
    )
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
          {:ok, [Deletable.t()]} | {:error, reason :: any()}
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
  def init({name, recycler, dynamic_sup, registry, opts}) do
    %Options{primary: primary} = opts

    {:ok,
     %__MODULE__{
       name: name,
       recycler: recycler,
       dynamic_sup: dynamic_sup,
       registry: registry,
       fs: primary[:fs],
       dirname: primary[:dir]
     }, {:continue, {:initialize, opts}}}
  end

  @impl GenServer
  def handle_continue({:initialize, opts}, state) do
    %__MODULE__{dirname: dirname, fs: fs, recycler: recycler} = state

    with do
      %Options{max_num_recyclable_logs: ml} = opts
      :ok = Recycler.initialize(recycler, ml)
    end

    :ok = FS.mkdir_all(fs, dirname)
    {:ok, files} = FS.list(fs, dirname)

    # build logs
    logs =
      with do
        files
        |> Enum.map(fn f -> VirtualLog.parse_filename(f) end)
        |> Enum.group_by(fn {log_num, _} -> log_num end)
        |> Enum.map(fn {log_num, ms} ->
          ss = Enum.map(ms, fn {_, index} -> %Models.Segment{index: index, dir: dirname} end)
          %VirtualLog{log_num: log_num, segments: ss}
        end)
      end

    # set recycler min
    Enum.each(logs, fn %VirtualLog{log_num: n} ->
      recycler
      |> Recycler.get_min()
      |> Kernel.<=(n)
      |> if do
        Recycler.set_min(recycler, n + 1)
      end
    end)

    # add to initial obsolete
    initial_obsolete =
      Enum.map(logs, fn %VirtualLog{log_num: log_num} ->
        %Models.Deletable{fs: fs, path: Path.join(dirname, VirtualLog.filename(log_num, 0)), log_num: log_num}
      end)

    {:noreply, %__MODULE__{state | initial_obsolete: initial_obsolete}}
  end

  @impl GenServer
  def handle_call(:get_self, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:create, log_num}, _from, state) do
    %__MODULE__{
      dirname: dirname,
      registry: registry,
      dynamic_sup: dynamic_sup,
      queue: q
    } = state

    # new file
    writer =
      with do
        log_name = Path.join(dirname, Models.VirtualLog.filename(log_num, 0))
        {:ok, file} = create_or_reuse(log_name, state)
        writer_name = {:via, Registry, {registry, {:writer, log_num}}}
        {:ok, _} = DynamicSupervisor.start_child(dynamic_sup, {ExWal.LogWriter.Single, {writer_name, file, log_num}})
        ExWal.LogWriter.Single.get(writer_name)
      end

    # add to queue
    q = [
      %VirtualLog{
        log_num: log_num,
        segments: [%Models.Segment{index: 0, dir: dirname}]
      }
      | q
    ]

    {
      :reply,
      {:ok, writer},
      %__MODULE__{state | queue: q}
    }
  end

  def handle_call({:obsolete, min_log_num, true}, _from, state) do
    %__MODULE__{
      recycler: recycler,
      queue: q,
      initial_obsolete: init_ob,
      fs: fs
    } = state

    to_del =
      with do
        q
        |> Enum.filter(fn %VirtualLog{log_num: n} -> n < min_log_num end)
        |> Enum.reduce(
          init_ob,
          fn l, acc ->
            recycler
            |> Recycler.add(l)
            |> if do
              [from_log(fs, l) | acc]
            else
              acc
            end
          end
        )
      end

    queue = Enum.reject(q, fn %VirtualLog{log_num: n} -> n < min_log_num end)

    {
      :reply,
      {:ok, to_del},
      %__MODULE__{
        state
        | queue: queue,
          initial_obsolete: []
      }
    }
  end

  def handle_call({:obsolete, min_log_num, false}, _from, state) do
    %__MODULE__{queue: q, initial_obsolete: init_ob} = state
    q = Enum.reject(q, fn %VirtualLog{log_num: n} -> n < min_log_num end)

    {
      :reply,
      {:ok, init_ob},
      %__MODULE__{
        state
        | queue: q,
          initial_obsolete: []
      }
    }
  end

  def handle_call(:list, _from, state) do
    %__MODULE__{queue: q} = state

    {:reply, {:ok, q}, state}
  end

  def handle_call({:open_for_read, log_num}, _from, state) do
    %__MODULE__{queue: q} = state

    exist? =
      q
      |> Enum.map(fn %VirtualLog{log_num: n} -> n end)
      |> Enum.member?(log_num)

    {:reply, start_log_reader(log_num, exist?, state), state}
  end

  defp start_log_reader(log_num, log_exists?, state)

  defp start_log_reader(_log_num, false, _state), do: {:error, :not_found}

  defp start_log_reader(log_num, true, state) do
    %__MODULE__{
      registry: registry,
      dynamic_sup: dynamic_sup,
      dirname: dirname,
      fs: fs
    } = state

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
        %VirtualLog{log_num: n} = recycled_log
        old_name = Path.join(dirname, Models.VirtualLog.filename(n, 0))
        FS.reuse_for_write(fs, old_name, log_name)
    end
  end

  @spec from_log(fs :: ExWal.FS.t(), log :: Models.VirtualLog.t()) :: Models.Deletable.t()
  defp from_log(fs, %VirtualLog{log_num: l, segments: [%Models.Segment{dir: dir}]}) do
    %Models.Deletable{
      fs: fs,
      path: Path.join(dir, VirtualLog.filename(l, 0)),
      log_num: l
    }
  end
end

defimpl ExWal.Manager, for: ExWal.Manager.Standalone do
  alias ExWal.Manager.Standalone

  @spec list(ExWal.Manager.t()) :: {:ok, [ExWal.Models.VirtualLog.t()]} | {:error, reason :: any()}
  def list(%Standalone{name: name}), do: Standalone.list(name)

  @spec obsolete(
          impl :: ExWal.Manager.t(),
          min_log_num :: ExWal.Models.VirtualLog.log_num(),
          recycle? :: boolean()
        ) ::
          {:ok, [ExWal.Models.Deletable.t()]} | {:error, reason :: any()}
  def obsolete(%Standalone{name: name}, min_log_num, recycle?), do: Standalone.obsolete(name, min_log_num, recycle?)

  @spec create(impl :: ExWal.Manager.t(), log_num :: ExWal.Models.VirtualLog.log_num()) ::
          {:ok, ExWal.LogWriter.t()} | {:error, reason :: any()}
  def create(%Standalone{name: name}, log_num), do: Standalone.create(name, log_num)

  @spec close(impl :: ExWal.Manager.t()) :: :ok | {:error, reason :: any()}
  def close(%Standalone{name: name}), do: Standalone.stop(name)
end
