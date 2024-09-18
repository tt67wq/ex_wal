defmodule AppTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS.Default
  alias ExWal.LogReader
  alias ExWal.LogWriter
  alias ExWal.Manager
  alias ExWal.Test.App

  @path_primary "./tmp/monitor_test/primary"
  @path_secondary "./tmp/monitor_test/secondary"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path_primary)
    File.rm_rf(@path_secondary)
    :ok
  end

  setup_all do
    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_dynamic_sup})
    start_supervised!({App, []})
    [fs: App.syncing_fs()]
  end

  test "main", %{fs: fs} do
    {:ok, m} =
      App.manager(:standalone, "test", %ExWal.Manager.Options{
        primary: [
          fs: fs,
          dir: @path_primary
        ]
      })

    assert {:ok, writer} = Manager.create(m, 2)

    # [length: 10, max_length: 1000]
    # |> StreamData.binary()
    # |> Enum.take(10_000)
    1..50
    |> Enum.map(fn x ->
      s =
        x
        |> Integer.to_string()
        |> String.pad_leading(4, "0")

      "Hello Elixir! I am a developer. I love Elixir #{s}."
    end)
    |> Enum.each(fn data -> LogWriter.write_record(writer, data) end)

    Process.sleep(1000)

    assert {:ok, [log | _]} = Manager.list(m)

    assert {:ok, reader} = App.open_for_read(log)

    keep_reading(reader)

    LogWriter.stop(writer)
    LogReader.stop(reader)

    Manager.close(m)
  end

  defp keep_reading(reader) do
    case LogReader.next(reader) do
      :eof ->
        :eof

      {:error, _reason} ->
        LogReader.recovery(reader)

      # raise ExWal.Exception, message: "read failed: #{inspect(reason)}"

      bin ->
        IO.puts(bin)
        keep_reading(reader)
    end
  end
end
