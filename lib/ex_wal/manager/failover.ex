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

  @type t :: %__MODULE__{
          name: GenServer.name(),
          recycler: ExWal.Recycler.t(),
          dynamic_sup: GenServer.name(),
          registry: GenServer.name(),
          dir_handles: %{
            primary: ExWal.LogWriter.t(),
            secondary: ExWal.LogWriter.t()
          },
          monitor: pid() | GenServer.name(),
          init_obsolete: [Models.Deletable.t()]
        }

  defstruct name: nil, recycler: nil, dynamic_sup: nil, registry: nil, dir_handles: %{}, monitor: nil, init_obsolete: []

  def start_link({name, recycler, dynamic_sup, registry, opts}) do
    GenServer.start_link(
      __MODULE__,
      {name, recycler, dynamic_sup, registry, opts},
      name: name
    )
  end

  # ---------------- server ---------------

  def init({name, recycler, dynamic_sup, registry, opts}) do
    state = %__MODULE__{
      name: name,
      recycler: recycler,
      dynamic_sup: dynamic_sup,
      registry: registry,
      dir_handles: %{},
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

  def handle_continue({:monitor, opts}, state) do
    %__MODULE__{registry: registry, name: name} = state

    {:ok, ob} = Obeserver.start_link({:via, Registry, {registry, {:observer, name}}})

    {:ok, m} =
      with do
        %Options{primary: pr, secondary: sc} = opts

        Monitor.start_link({
          [
            primary: %DirAndFile{dir: pr[:dir]},
            secondary: %DirAndFile{dir: sc[:dir]}
          ],
          ob
        })
      end

    {:noreply, %__MODULE__{state | monitor: m}, {:continue, {:initialize, opts}}}
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

    {:noreply, %__MODULE__{state | init_obsolete: init_obsolete_primary ++ init_obsolete_secondary}}
  end

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
end
