defmodule ExWal.Core do
  @moduledoc false

  use Agent

  alias ExWal.Manager
  alias ExWal.Models

  @type t :: %__MODULE__{
          name: Agent.name(),
          dynamic_sup: atom(),
          registry: atom()
        }

  defstruct name: nil, dynamic_sup: nil, registry: nil

  @spec start_link({
          name :: Agent.name(),
          dynamic_sup :: atom(),
          registry :: atom()
        }) :: Agent.on_start()
  def start_link({name, dynamic_sup, registry}) do
    Agent.start_link(__MODULE__, :init, [{name, dynamic_sup, registry}], name: name)
  end

  @spec manager(
          name :: Agent.name(),
          mode :: :standalone | :failover,
          manager_name :: term(),
          opts :: ExWal.Manager.Options.t()
        ) :: {:ok, ExWal.Manager.t()} | {:error, reason :: term()}
  def manager(name, mode, manager_name, opts) do
    Agent.get(name, __MODULE__, :handle_manager, [mode, manager_name, opts])
  end

  def open_for_read(name, vlog) do
    Agent.get(name, __MODULE__, :handle_open_for_read, [vlog])
  end

  # ---------------- handler -----------------

  def init({name, dynamic_sup, registry}) do
    %__MODULE__{name: name, dynamic_sup: dynamic_sup, registry: registry}
  end

  def handle_manager(state, mode, manager_name, opts)

  def handle_manager(state, :standalone, manager_name, opts) do
    %__MODULE__{
      registry: registry,
      dynamic_sup: dynamic_sup
    } = state

    manager_name = {:via, Registry, {registry, {:manager, manager_name}}}

    {:ok, _} =
      DynamicSupervisor.start_child(
        dynamic_sup,
        {
          Manager.Standalone,
          {
            manager_name,
            dynamic_sup,
            registry,
            opts
          }
        }
      )

    {:ok, Manager.Standalone.get(manager_name)}
  end

  def handle_manager(state, :failover, manager_name, opts) do
    %__MODULE__{registry: registry, dynamic_sup: dynamic_sup} = state

    manager_name = {:via, Registry, {registry, {:manager, manager_name}}}

    {:ok, _} =
      DynamicSupervisor.start_child(
        dynamic_sup,
        {
          Manager.Failover,
          {
            manager_name,
            dynamic_sup,
            registry,
            opts
          }
        }
      )

    {:ok, Manager.Failover.get(manager_name)}
  end

  def handle_manager(_state, mode, _manager_name, _opts),
    do: raise(ExWal.Exception, message: "unsupported mode", details: mode)

  def handle_manager(_state, _dirname, _mode), do: {:error, :unsupported_mode}

  def handle_open_for_read(state, vlog)
  # single
  def handle_open_for_read(state, %Models.VirtualLog{segments: [_]} = vlog) do
    %Models.VirtualLog{log_num: log_num, segments: [seg]} = vlog
    %__MODULE__{registry: registry, dynamic_sup: dynamic_sup} = state

    reader_name = {:via, Registry, {registry, {:single_reader, log_num}}}

    {:ok, _} =
      DynamicSupervisor.start_child(dynamic_sup, {ExWal.LogReader.Single, {reader_name, log_num, seg}})

    {:ok, ExWal.LogReader.Single.get(reader_name)}
  end

  # multi
  def handle_open_for_read(state, vlog) do
    %__MODULE__{registry: registry, dynamic_sup: dynamic_sup} = state
    %Models.VirtualLog{log_num: log_num} = vlog

    reader_name = {:via, Registry, {registry, {:virtual_reader, log_num}}}

    {:ok, _} =
      DynamicSupervisor.start_child(dynamic_sup, {ExWal.LogReader.Virtual, {reader_name, registry, vlog}})

    {:ok, ExWal.LogReader.Virtual.get(reader_name)}
  end
end
