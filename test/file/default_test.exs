defmodule File.DefaultTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Default

  @path "./tmp/file_test"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path)
    :ok
  end

  setup_all do
    [fs: %Default{}]
  end

  test "close", %{fs: fs} do
    FS.mkdir_all(fs, @path)
    {:ok, file} = FS.create(fs, Path.join(@path, "for_close.txt"))
    assert :ok = ExWal.File.close(file)
  end

  test "write & read", %{fs: fs} do
    FS.mkdir_all(fs, @path)
    {:ok, file} = FS.create(fs, Path.join(@path, "for_wr.txt"))
    assert :eof = ExWal.File.read(file, 5)
    assert :ok = ExWal.File.write(file, "hello")
    assert :ok = ExWal.File.close(file)

    {:ok, file} = FS.open_read_write(fs, Path.join(@path, "for_wr.txt"))
    assert {:ok, "hello"} = ExWal.File.read(file, 5)
    assert {:ok, "hello"} = ExWal.File.read_at(file, 5, 0)
    assert :ok = ExWal.File.write_at(file, " world", 5)
    assert {:ok, " worl"} = ExWal.File.read_at(file, 5, 5)
  end

  test "stat", %{fs: fs} do
    FS.mkdir_all(fs, @path)
    {:ok, file} = FS.create(fs, Path.join(@path, "for_stat.txt"))
    assert {:ok, %{size: 0, mode: 0o100666, mtime: _}} = ExWal.File.stat(file)
    assert :ok = ExWal.File.write(file, "hello")
    assert {:ok, %{size: 5, mode: 0o100666, mtime: _}} = ExWal.File.stat(file)
    assert :ok = ExWal.File.close(file)
  end

  test "sync", %{fs: fs} do
    FS.mkdir_all(fs, @path)
    {:ok, file} = FS.create(fs, Path.join(@path, "for_sync.txt"))
    assert :ok = ExWal.File.write(file, "hello")
    assert :ok = ExWal.File.sync(file)
  end
end
