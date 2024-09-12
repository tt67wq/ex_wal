defmodule Manager.StandaloneTest do
  @moduledoc false

  use ExUnit.Case

  alias ExWal.FS.Default
  alias ExWal.FS.Syncing
  alias ExWal.LogReader
  alias ExWal.LogWriter
  alias ExWal.Manager
  alias ExWal.Manager.Options

  require Logger

  @path "./tmp/manager_test"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path)
    :ok
  end

  setup_all do
    default = %Default{}
    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_dynamic_sup})
    start_supervised!({Syncing, {:test_fs, default, :test_dynamic_sup, :test_registry}})

    fs = Syncing.init(:test_fs, default, :test_dynamic_sup, :test_registry)

    start_supervised!({
      ExWal.Core,
      {
        :test_core,
        :test_dynamic_sup,
        :test_registry
      }
    })

    [fs: fs]
  end

  test "main", %{fs: fs} do
    opts = %Options{
      primary: %{
        fs: fs,
        dir: @path
      }
    }

    {:ok, m} = ExWal.Core.manager(:test_core, :standalone, "standalone_manager", opts)

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

    assert {:ok, reader} = ExWal.Core.open_for_read(:test_core, log)

    keep_reading(reader)

    ExWal.LogWriter.stop(writer)
    ExWal.LogReader.stop(reader)
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
