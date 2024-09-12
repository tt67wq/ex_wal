defmodule MonitorTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default
  alias ExWal.FS.Syncing
  alias ExWal.LogWriter
  alias ExWal.Monitor
  alias ExWal.Monitor.DirAndFile

  @path_primary "./tmp/monitor_test/primary"
  @path_secondary "./tmp/monitor_test/secondary"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path_primary)
    File.rm_rf(@path_secondary)
    :ok
  end

  setup_all do
    default = %Default{}
    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_dynamic_sup})
    start_supervised!({Syncing, {:test_fs, default, :test_dynamic_sup, :test_registry}})
    [fs: Syncing.init(:test_fs, default, :test_dynamic_sup, :test_registry)]
  end

  test "main", %{fs: fs} do
    FS.mkdir_all(fs, @path_primary)
    FS.mkdir_all(fs, @path_secondary)

    dirs = [
      primary: %DirAndFile{
        dir: @path_primary,
        fs: fs
      },
      secondary: %DirAndFile{
        dir: @path_secondary,
        fs: fs
      }
    ]

    p = start_supervised!({Monitor, {dirs}})

    assert {:ok, writer} =
             Monitor.new_writer(p, fn dir, fs ->
               {:ok, _} = LogWriter.Failover.start_link({:test_writer, :test_registry, fs, dir, 1, nil})
               LogWriter.Failover.get(:test_writer)
             end)

    [length: 10, max_length: 1000]
    |> StreamData.binary()
    |> Enum.take(10000)
    |> Enum.each(fn data ->
      assert {:ok, _} = LogWriter.write_record(writer, data)
    end)

    Process.sleep(1000)
  end
end
