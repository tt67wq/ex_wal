defmodule ExWal.Core do
  @moduledoc false

  use Agent

  alias ExWal.Manager
  alias ExWal.Recycler

  @type t :: %__MODULE__{
          name: Agent.name(),
          dynamic_sup: atom(),
          registry: atom(),
          fs: ExWal.FS.t()
        }

  defstruct name: nil, dynamic_sup: nil, registry: nil, fs: nil

  # @spec start_link({
  #         name :: Agent.name(),
  #         dynamic_sup :: atom(),
  #         registry :: atom(),
  #         recycler :: atom(),
  #         fs :: ExWal.FS.t()
  #       }) :: Agent.on_start()
  def start_link({name, dynamic_sup, registry, fs}) do
    Agent.start_link(__MODULE__, :init, [{name, dynamic_sup, registry, fs}], name: name)
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

  def init({name, dynamic_sup, registry, fs}) do
    %__MODULE__{name: name, dynamic_sup: dynamic_sup, registry: registry, fs: fs}
  end

  def handle_manager(state, dirname, mode)

  def handle_manager(state, dirname, :standalone) do
    %__MODULE__{
      registry: registry,
      dynamic_sup: dynamic_sup,
      fs: fs
    } = state

    # start sub registry
    sub_registry = {:via, Registry, {registry, {:manager_registry, dirname}}}
    {:ok, _} = DynamicSupervisor.start_child(dynamic_sup, {Registry, name: sub_registry})

    # recycler
    recycler =
      with do
        rn = {:via, Registry, {registry, {:recycler, dirname}}}
        {:ok, _} = Recycler.ETS.start_link(rn)
        Recycler.ETS.get(rn)
      end

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
            %Manager.Options{primary: [fs: fs, dir: dirname]}
          }
        }
      )

    {:ok, Manager.Standalone.get(manager_name)}
  end

  def handle_manager(_state, _dirname, _mode), do: {:error, :unsupported_mode}
end
