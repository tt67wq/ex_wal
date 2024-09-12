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
          current_writer: {Models.VirtualLog.log_num(), ExWal.LogWriter.t()} | nil,
          closed_logs: [Models.VirtualLog.t()],
          opts: Options.t()
        }

  defstruct name: nil,
            recycler: nil,
            dynamic_sup: nil,
            registry: nil,
            monitor: nil,
            observer: nil,
            initial_obsolete: [],
            current_writer: nil,
            closed_logs: [],
            opts: nil

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

  def stop(p), do: GenServer.stop(p)

  @spec create(
          name :: GenServer.name(),
          log_num :: non_neg_integer()
        ) ::
          {:ok, ExWal.LogWriter.t()} | {:error, reason :: any()}
  def create(name, log_num) do
    GenServer.call(name, {:create, log_num})
  end

  @spec obsolete(
          name :: GenServer.name(),
          min_log_num :: non_neg_integer(),
          recycle? :: boolean()
        ) ::
          {:ok, [Models.Deletable.t()]} | {:error, reason :: any()}
  def obsolete(name, min_log_num, recycle?) do
    GenServer.call(name, {:obsolete, min_log_num, recycle?})
  end

  # ---------------- server ---------------

  @impl GenServer
  def init({name, recycler, dynamic_sup, registry, opts}) do
    state = %__MODULE__{
      name: name,
      recycler: recycler,
      dynamic_sup: dynamic_sup,
      registry: registry,
      monitor: nil,
      opts: opts
    }

    %Options{secondary: sc} = opts

    sc
    |> test_secondary_dir()
    |> case do
      :ok ->
        {:ok, state, {:continue, :monitor}}

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
  def handle_continue(:monitor, state) do
    %__MODULE__{opts: opts} = state

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
      {:continue, :initialize}
    }
  end

  def handle_continue(:initialize, state) do
    %__MODULE__{recycler: recycler, opts: opts} = state

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

    s = self()

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
              ob,
              s
            }
          }
        )

      ExWal.LogWriter.Failover.get(name)
    end

    {:ok, w} = Monitor.new_writer(monitor, writer_create_func)

    {:reply, {:ok, w}, %__MODULE__{state | current_writer: {log_num, w}}}
  end

  def handle_call({:create, _log_num}, _, state), do: {:reply, {:error, "previous writer not closed"}, state}

  def handle_call({:obsolete, _, true} = m, _, %__MODULE__{} = state) do
    {_, min_log_num, _} = m

    %__MODULE__{
      closed_logs: cl,
      initial_obsolete: to_delete,
      recycler: recycler,
      opts: opts
    } = state

    %Options{primary: p} = opts

    # Recycle only the primary at index=0, if there was no failover,
    # and synchronously closed. It may not be safe to recycle a file that is
    # still being written to. And recycling when there was a failover may
    # fill up the recycler with smaller log files. The restriction regarding
    # index=0 is because logRecycler.Peek only exposes the
    # DiskFileNum, and we need to use that to construct the path -- we could
    # remove this restriction by changing the logRecycler interface, but we
    # don't bother.
    can_recycle_f = fn
      %Models.VirtualLog{segments: [seg]} ->
        %Models.Segment{index: i, dir: dir} = seg
        i == 0 and dir == p[:dir]

      _ ->
        false
    end

    log_to_deletable = fn
      %Models.VirtualLog{log_num: log_num, segments: segs} ->
        Enum.map(segs, fn %Models.Segment{index: i, dir: dir, fs: fs} ->
          %Models.Deletable{
            log_num: log_num,
            fs: fs,
            path: Path.join(dir, Models.VirtualLog.filename(log_num, i))
          }
        end)
    end

    may_delete =
      fn
        log, true, d ->
          recycler
          |> Recycler.add(log)
          |> if do
            d
          else
            [log_to_deletable.(log) | d]
          end

        log, false, d ->
          [log_to_deletable.(log) | d]
      end

    to_delete =
      cl
      |> Enum.filter(fn %Models.VirtualLog{log_num: log_num} -> log_num < min_log_num end)
      |> Enum.reduce(to_delete, fn %Models.VirtualLog{} = log, acc ->
        may_delete.(log, can_recycle_f.(log), acc)
      end)
      |> List.flatten()

    cl = Enum.reject(cl, fn %Models.VirtualLog{log_num: log_num} -> log_num < min_log_num end)

    {:reply, {:ok, to_delete}, %__MODULE__{state | initial_obsolete: [], closed_logs: cl}}
  end

  @impl GenServer
  def handle_info({:writer_shutdown, %Models.VirtualLog{log_num: a}} = n, %__MODULE__{current_writer: {b, _}} = state)
      when a == b do
    %__MODULE__{closed_logs: cl} = state
    {:writer_shutdown, log} = n
    {:noreply, %__MODULE__{state | current_writer: nil, closed_logs: [log | cl]}}
  end

  def handle_info({:writer_shutdown, _, _}, state), do: {:noreply, state}

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
