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
        {Registry, name: registry_name(name), keys: :unique},
        {ExWal.FS.Syncing,
         {
           syncing_fs_name(name),
           %ExWal.FS.Default{},
           dynamic_sup_name(name),
           registry_name(name)
         }},
        {
          ExWal.Core,
          {
            name,
            dynamic_sup_name(name),
            registry_name(name)
          }
        }
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def syncing_fs(name) do
    ExWal.FS.Syncing.init(
      syncing_fs_name(name),
      %ExWal.FS.Default{},
      dynamic_sup_name(name),
      registry_name(name)
    )
  end

  defp supervisor_name(name) do
    Module.concat(name, Supervisor)
  end

  defp registry_name(name) do
    Module.concat(name, Registry)
  end

  defp dynamic_sup_name(name) do
    Module.concat(name, DynamicSupervisor)
  end

  defp syncing_fs_name(name) do
    Module.concat(name, FS.Syncing)
  end
end
