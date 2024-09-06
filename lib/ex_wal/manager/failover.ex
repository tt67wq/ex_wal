defmodule ExWal.Manager.Failover do
  @moduledoc false

  use GenServer, restart: :transient

  alias ExWal.FS
  alias ExWal.Manager.Options
  alias ExWal.Models
  alias ExWal.Monitor
  alias ExWal.Monitor.DirAndFile
  alias ExWal.Obeserver
  alias ExWal.Recycler

  require Logger

  @type t :: %__MODULE__{
          name: GenServer.name(),
          recycler: ExWal.Recycler.t(),
          dynamic_sup: GenServer.name(),
          registry: GenServer.name(),
          monitor: pid() | GenServer.name(),
          observer: pid() | GenServer.name(),
          initial_obsolete: [Models.Deletable.t()],
          current_writer: ExWal.LogWriter.t(),
          closed_writers: %{Models.VirtualLog.log_num() => ExWal.LogWriter.t()}
        }

  defstruct name: nil,
            recycler: nil,
            dynamic_sup: nil,
            registry: nil,
            monitor: nil,
            observer: nil,
            initial_obsolete: [],
            # log_num => failover writer
            current_writer: nil,
            closed_writers: %{}

  def start_link({name, recycler, dynamic_sup, registry, opts}) do
    GenServer.start_link(
      __MODULE__,
      {name, recycler, dynamic_sup, registry, opts},
      name: name
    )
  end

  def stop(p), do: GenServer.stop(p)

  @spec create(
          name :: GenServer.name(),
          log_num :: non_neg_integer()
        ) ::
          {:ok, ExWal.LogWriter.t()} | {:error, reason :: any()}
  def create(name, log_num) do
    GenServer.call(name, {:create, log_num})
  end

  # ---------------- server ---------------

  @impl GenServer
  def init({name, recycler, dynamic_sup, registry, opts}) do
    state = %__MODULE__{
      name: name,
      recycler: recycler,
      dynamic_sup: dynamic_sup,
      registry: registry,
      monitor: nil
    }

    %Options{secondary: sc} = opts

    sc
    |> test_secondary_dir()
    |> case do
      :ok ->
        {:ok, state, {:continue, {:monitor, opts}}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def terminate(reason, state) do
    %__MODULE__{monitor: m, observer: ob} = state
    Monitor.stop(m)
    Obeserver.stop(ob)

    may_log_reason(reason)
  end

  @impl GenServer
  def handle_continue({:monitor, opts}, state) do
    {:ok, m} =
      with do
        %Options{primary: pr, secondary: sc} = opts

        Monitor.start_link({
          [
            primary: %DirAndFile{dir: pr[:dir], fs: pr[:fs]},
            secondary: %DirAndFile{dir: sc[:dir], fs: pr[:fs]}
          ]
        })
      end

    {
      :noreply,
      %__MODULE__{state | monitor: m},
      {:continue, {:initialize, opts}}
    }
  end

  def handle_continue({:initialize, opts}, state) do
    %__MODULE__{recycler: recycler} = state

    # recycler
    with do
      %Options{max_num_recyclable_logs: ml} = opts
      :ok = Recycler.initialize(recycler, ml)
    end

    # init primary
    init_obsolete_primary =
      with do
        %Options{primary: [fs: fs, dir: dir]} = opts
        init_recycler_and_obsolete(fs, dir, recycler)
      end

    init_obsolete_secondary =
      with do
        %Options{secondary: [fs: fs, dir: dir]} = opts
        init_recycler_and_obsolete(fs, dir, recycler)
      end

    {:noreply, %__MODULE__{state | initial_obsolete: init_obsolete_primary ++ init_obsolete_secondary}}
  end

  @impl GenServer
  def handle_call({:create, log_num}, _, %__MODULE__{current_writer: nil} = state) do
    %__MODULE__{
      monitor: monitor,
      registry: registry,
      dynamic_sup: ds,
      observer: ob
    } = state

    writer_create_func = fn dir, fs ->
      name = {:via, Registry, {registry, {:failover_writer, log_num}}}

      {:ok, _} =
        DynamicSupervisor.start_child(
          ds,
          {
            ExWal.LogWriter.Failover,
            {
              name,
              registry,
              fs,
              dir,
              log_num,
              ob
            }
          }
        )

      ExWal.LogWriter.Failover.get(name)
    end

    {:ok, w} = Monitor.new_writer(monitor, writer_create_func)

    {:reply, {:ok, w}, %__MODULE__{state | current_writer: w}}
  end

  def handle_call({:create, _log_num}, _, state), do: {:reply, {:error, "previous writer not closed"}, state}

  # def handle_call({:obsolete, min_log_num, true}, _from, state) do
  #   %__MODULE__{
  #     recycler: recycler,
  #     initial_obsolete: to_del,
  #   } = state

  # end

  defp test_secondary_dir(fs: fs, dir: dir) do
    fs
    |> ExWal.FS.create(Path.join(dir, "failover_source"))
    |> case do
      {:ok, file} ->
        test_file(file)

      err ->
        err
    end
  end

  defp test_file(file) do
    ExWal.File.write(file, "secondary: #{Path.dirname(file)}\nprocess start: #{System.system_time()}\n")
  end

  defp init_recycler_and_obsolete(fs, dir, recycler) do
    :ok = FS.mkdir_all(fs, dir)
    {:ok, files} = FS.list(fs, dir)

    init_recycler(files, recycler)
    init_obsolete(files, fs, dir)
  end

  defp init_recycler(files, recycler) do
    files
    |> Enum.map(fn f -> Models.VirtualLog.parse_filename(f) end)
    |> Enum.uniq_by(fn {log_num, _} -> log_num end)
    |> Enum.each(fn {log_num, _} ->
      recycler
      |> Recycler.get_min()
      |> Kernel.<=(log_num)
      |> if do
        Recycler.set_min(recycler, log_num + 1)
      end
    end)
  end

  defp init_obsolete(files, fs, dir) do
    files
    |> Enum.map(fn f -> Models.VirtualLog.parse_filename(f) end)
    |> Enum.map(fn {log_num, index} ->
      %Models.Deletable{
        fs: fs,
        path: Path.join(dir, Models.VirtualLog.filename(log_num, index))
      }
    end)
  end

  defp may_log_reason(:normal), do: :pass
  defp may_log_reason(reason), do: Logger.error("failover manager terminated: #{inspect(reason)}")
end
