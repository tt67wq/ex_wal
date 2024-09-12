defmodule Writer.FailoverTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default
  alias ExWal.FS.Syncing
  alias ExWal.LogWriter
  # alias ExWal.Models

  @path "./tmp/log_writer_test"

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
    [fs: Syncing.init(:test_fs, default, :test_dynamic_sup, :test_registry)]
  end

  test "write record", %{fs: fs} do
    FS.mkdir_all(fs, @path)
    # {name, registry, fs, dir, log_num, manager}
    start_supervised!({LogWriter.Failover, {:test_writter, :test_registry, fs, @path, 0, nil}})

    writter = LogWriter.Failover.get(:test_writter)

    assert {:ok, 5} = LogWriter.write_record(writter, "aaaaa")
    Process.sleep(100)
    assert {:ok, 32} = LogWriter.write_record(writter, "bbbbb")
    assert {:ok, 48} = LogWriter.write_record(writter, "ccccc")
  end
end
