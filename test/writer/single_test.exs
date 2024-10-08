defmodule Writer.SingleTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default
  alias ExWal.FS.Syncing
  alias ExWal.LogWriter
  alias ExWal.Models

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
    filename = Models.VirtualLog.filename(0, 0)
    {:ok, file} = FS.create(fs, Path.join(@path, filename))
    start_supervised!({LogWriter.Single, {:test_writter, file, 0}})

    writter = LogWriter.Single.get(:test_writter)

    assert {:ok, 16} = LogWriter.write_record(writter, "aaaaa")
    assert {:ok, 32} = LogWriter.write_record(writter, "bbbbb")
    assert {:ok, 48} = LogWriter.write_record(writter, "ccccc")
    Process.sleep(100)
  end
end
