defmodule ExWal.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(name, config) do
    Supervisor.start_link(__MODULE__, {name, config}, name: supervisor_name(name))
  end

  @doc false
  @impl Supervisor
  def init({name, config}) do
    children =
      [
        {ExWal.LRU, {lru_name(name), 1024, []}},
        {ExWal.Core, {config, lru_name(name), name}}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp supervisor_name(name) do
    Module.concat(name, Supervisor)
  end

  defp lru_name(name) do
    Module.concat(name, LRU)
  end
end
