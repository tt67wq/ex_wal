defmodule File.SyncingTest do
  @moduledoc false

  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default
  alias ExWal.FS.Syncing

  @path "./tmp/file_test"

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
    %{fs: Syncing.get(:test_fs)}
  end

  test "write", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, file} = FS.create(fs, Path.join(@path, "for_write.txt"))
    assert :ok == ExWal.File.write(file, "hello")
    assert :ok == ExWal.File.close(file)
  end

  test "read_write", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, file} = FS.create(fs, Path.join(@path, "for_read_write.txt"))
    assert :ok == ExWal.File.write(file, "hello")
    assert :ok == ExWal.File.write(file, " world")
    assert :ok == ExWal.File.close(file)
    assert {:ok, file} = FS.open_read_write(fs, Path.join(@path, "for_read_write.txt"))
    assert {:ok, "hello "} = ExWal.File.read(file, 6)
    assert {:ok, "world"} = ExWal.File.read(file, 5)
  end
end
