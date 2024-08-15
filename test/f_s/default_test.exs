defmodule FS.DefaultTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default

  require Logger

  @path "./tmp/fs_test"

  setup :cleanup

  defp cleanup(_ctx) do
    File.rm_rf(@path)
    :ok
  end

  setup_all do
    [fs: %Default{}]
  end

  test "create", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_create.txt"))
  end

  test "link", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_link.txt"))
    assert :ok = FS.link(fs, Path.join(@path, "for_link.txt"), Path.join(@path, "for_link2.txt"))
  end

  test "remove", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_remove.txt"))
    assert :ok = FS.remove(fs, Path.join(@path, "for_remove.txt"))
  end

  test "open_read_write", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, file} = FS.create(fs, Path.join(@path, "for_open_read_write.txt"))
    assert :ok = ExWal.File.close(file)
    assert {:ok, _file} = FS.open_read_write(fs, Path.join(@path, "for_open_read_write.txt"))
  end

  test "remove_all", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_remove_all.txt"))
    assert :ok = FS.remove_all(fs, @path)
  end

  test "rename", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_rename.txt"))
    assert :ok = FS.rename(fs, Path.join(@path, "for_rename.txt"), Path.join(@path, "for_rename2.txt"))
  end

  @tag exec: true
  test "reuse_for_write", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, file} = FS.create(fs, Path.join(@path, "for_reuse_for_write.txt"))
    assert :ok == ExWal.File.write(file, "hello world")
    assert :ok = ExWal.File.close(file)

    assert {:ok, file} =
             FS.reuse_for_write(
               fs,
               Path.join(@path, "for_reuse_for_write.txt"),
               Path.join(@path, "for_reuse_for_write2.txt")
             )

    assert :ok = ExWal.File.write(file, "hello")
    assert :ok = ExWal.File.close(file)
    assert {:ok, file} = FS.open(fs, Path.join(@path, "for_reuse_for_write2.txt"))
    assert {:ok, "hello"} = ExWal.File.read(file, 10)
  end

  test "mkdir_all", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, Path.join(@path, "for_mkdir_all"))
  end

  test "lock", %{fs: fs} do
    assert true = FS.lock(fs, "for_lock.txt")
  end

  test "list", %{fs: fs} do
    path = Path.join(@path, "for_list")
    assert :ok == FS.mkdir_all(fs, path)
    assert {:ok, []} = FS.list(fs, path)
    assert {:ok, _} = FS.create(fs, Path.join(path, "for_list.txt"))
    assert {:ok, ["for_list.txt"]} = FS.list(fs, path)
  end

  test "stat", %{fs: fs} do
    assert :ok == FS.mkdir_all(fs, @path)
    assert {:ok, _} = FS.create(fs, Path.join(@path, "for_stat.txt"))
    assert {:ok, %{size: 0}} = FS.stat(fs, Path.join(@path, "for_stat.txt"))
  end
end
