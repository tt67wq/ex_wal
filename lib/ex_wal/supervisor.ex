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
        {ExWal.Recycler.ETS, {recycler_name(name)}},
        {Registry, name: registry_name(name)}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp supervisor_name(name) do
    Module.concat(name, Supervisor)
  end

  defp recycler_name(name) do
    Module.concat(name, Recycler)
  end

  defp registry_name(name) do
    Module.concat(name, Registry)
  end
end
