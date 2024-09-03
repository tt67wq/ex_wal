defmodule ExWal.Manager.Failover do
  @moduledoc false

  use GenServer, restart: :transient

  alias ExWal.Manager.Options
  alias ExWal.Monitor
  alias ExWal.Monitor.DirAndFile
  alias ExWal.Obeserver

  defstruct name: nil, recycler: nil, dynamic_sup: nil, registry: nil, dir_handles: %{}, monitor: nil

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
    %Options{primary: pr, secondary: sc} = opts
    %__MODULE__{registry: registry, name: name} = state

    {:ok, ob} = Obeserver.start_link({:via, Registry, {registry, {:observer, name}}})

    {:ok, m} =
      Monitor.start_link({
        [
          primary: %DirAndFile{dir: pr[:dir]},
          secondary: %DirAndFile{dir: sc[:dir]}
        ],
        ob
      })

    {:noreply, %__MODULE__{state | monitor: m}}
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
end
