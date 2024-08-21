defmodule ExWal.Core do
  @moduledoc false

  use Agent

  alias ExWal.Manager

  @type t :: %__MODULE__{
          name: Agent.name(),
          dynamic_sup: atom(),
          registry: atom(),
          recycler: atom(),
          fs: ExWal.FS.t()
        }

  defstruct name: nil, dynamic_sup: nil, registry: nil, recycler: nil, fs: nil

  @spec start_link({
          name :: Agent.name(),
          dynamic_sup :: atom(),
          registry :: atom(),
          recycler :: atom(),
          fs :: ExWal.FS.t()
        }) :: Agent.on_start()
  def start_link({name, dynamic_sup, registry, recycler, fs}) do
    Agent.start_link(__MODULE__, :init, [{name, dynamic_sup, registry, recycler, fs}], name: name)
  end

  @spec manager(
          name :: Agent.name(),
          dirname :: binary(),
          mode :: :standalone
        ) ::
          {:ok, Manager.t()} | {:error, reason :: any()}
  def manager(name, dirname, mode) do
    Agent.get(name, __MODULE__, :handle_manager, [dirname, mode])
  end

  # ---------------- handler -----------------

  def init({name, dynamic_sup, registry, recycler, fs}) do
    %__MODULE__{name: name, dynamic_sup: dynamic_sup, registry: registry, recycler: recycler, fs: fs}
  end

  def handle_manager(state, dirname, mode)

  def handle_manager(state, dirname, :standalone) do
    %__MODULE__{
      registry: registry,
      dynamic_sup: dynamic_sup,
      recycler: recycler,
      fs: fs
    } = state

    # start sub registry
    sub_registry = {:via, Registry, {registry, {:manager_registry, dirname}}}
    {:ok, _} = DynamicSupervisor.start_child(dynamic_sup, {Registry, name: sub_registry})

    manager_name = {:via, Registry, {registry, {:manager, dirname}}}

    {:ok, _} =
      DynamicSupervisor.start_child(
        dynamic_sup,
        {
          Manager.Standalone,
          {
            manager_name,
            recycler,
            dynamic_sup,
            sub_registry,
            fs,
            dirname
          }
        }
      )

    {:ok, Manager.Standalone.get(manager_name)}
  end

  def handle_manager(_state, _dirname, _mode), do: {:error, :unsupported_mode}
end
