defmodule ExWal.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(name, config) do
    Supervisor.start_link(__MODULE__, {name, config}, name: supervisor_name(name))
  end

  @doc false
  @impl Supervisor
  def init({name, _config}) do
    children =
      [
        {DynamicSupervisor, name: dynamic_sup_name(name), strategy: :one_for_one},
        {Registry, name: registry_name(name, :fs), keys: :unique},
        {Registry, name: registry_name(name, :core), keys: :unique},
        {
          ExWal.FS.Syncing,
          {
            syncing_name(name),
            %ExWal.FS.Default{},
            dynamic_sup_name(name),
            registry_name(name, :fs)
          }
        },
        {
          ExWal.Core,
          {
            name,
            dynamic_sup_name(name),
            registry_name(name, :core),
            ExWal.FS.Syncing.init(
              syncing_name(name),
              %ExWal.FS.Default{},
              dynamic_sup_name(name),
              registry_name(name, :fs)
            )
          }
        }
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp supervisor_name(name) do
    Module.concat(name, Supervisor)
  end

  defp registry_name(name, domain) do
    Module.concat([name, Registry, domain])
  end

  defp dynamic_sup_name(name) do
    Module.concat(name, DynamicSupervisor)
  end

  defp syncing_name(name) do
    Module.concat(name, FS.Syncing)
  end
end
