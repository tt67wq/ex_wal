defmodule FS.SyncingTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default
  alias ExWal.FS.Syncing

  require Logger

  @path "./tmp/fs_test"

  setup :cleanup

  defp cleanup(_ctx) do
    File.rm_rf(@path)
    :ok
  end

  setup_all do
    default = %Default{}
    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_dynamic_sup})
    start_supervised!({Syncing, {:test_fs, default, :test_dynamic_sup, :test_registry}})
    [fs: Syncing.get(:test_fs)]
  end

  test "create", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_create.txt"))
  end

  test "open_read_write", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.open_read_write(fs, Path.join(@path, "for_open_read_write.txt"))
  end
end
