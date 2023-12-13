defmodule ExWal.Debug do
  @moduledoc """
  Debug tools
  """
  require Logger

  def debug(msg), do: tap(msg, fn msg -> Logger.debug("[DEBUGING!!!!] => #{inspect(msg)}") end)

  def stacktrace(msg) do
    tap(msg, fn msg ->
      self()
      |> Process.info(:current_stacktrace)
      |> then(fn {:current_stacktrace, stacktrace} -> stacktrace end)
      # ignore the first two stacktrace
      |> Enum.drop(2)
      |> Enum.map_join("\n", fn {mod, fun, arity, [file: file, line: line]} ->
        "\t#{mod}.#{fun}/#{arity} #{file}:#{line}"
      end)
      |> then(fn stacktrace ->
        Logger.debug("[DEBUGING!!!!] => #{inspect(msg)} \n#{stacktrace}")
      end)
    end)
  end
end
