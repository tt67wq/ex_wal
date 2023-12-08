defmodule ExWalTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.Models.Entry

  setup do
    opts = [
      path: "./tmp/wal",
      segment_size: 1024,
      name: :test
    ]

    File.rm_rf!(opts[:path])
    [opts: opts]
  end

  describe "start_link" do
    test "start_link with empty path", %{opts: opts} do
      start_supervised!({ExWal, opts})
      assert 0 == ExWal.last_index(opts[:name])

      entries =
        1..100
        |> Enum.map(fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)
    end

    test "start_link with existing path", %{opts: opts} do
      # create a segment file
      start_supervised!({ExWal, opts}, restart: :temporary)

      entries =
        1..100
        |> Enum.map(fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      assert 100 == ExWal.last_index(opts[:name])

      ExWal.stop(opts[:name])

      # start with existing path
      start_supervised!({ExWal, opts})
      assert 100 == ExWal.last_index(opts[:name])

      entries =
        1..100
        |> Enum.map(fn i -> %Entry{index: i + 100, data: "Hello Elixir #{i + 100}"} end)

      :ok = ExWal.write(opts[:name], entries)
      assert 200 == ExWal.last_index(opts[:name])
    end
  end

  describe "write" do
    test "write multiple entries", %{opts: opts} do
      start_supervised!({ExWal, opts})
      assert 0 == ExWal.last_index(opts[:name])

      entries =
        1..100
        |> Enum.map(fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)
      assert 100 == ExWal.last_index(opts[:name])
    end
  end

  describe "read" do
    test "read not_exists", %{opts: opts} do
      start_supervised!({ExWal, opts})

      assert {:error, :index_not_found} == ExWal.read(opts[:name], 100)
    end

    test "read one", %{opts: opts} do
      start_supervised!({ExWal, opts})

      :ok = ExWal.write(opts[:name], [%Entry{index: 1, data: "Hello Elixir 1"}])

      assert {:ok, "Hello Elixir 1"} == ExWal.read(opts[:name], 1)
    end

    test "read recent data", %{opts: opts} do
      start_supervised!({ExWal, opts})

      entries =
        1..100
        |> Enum.map(fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      1..100
      |> Enum.each(fn idx ->
        assert {:ok, "Hello Elixir #{idx}"} == ExWal.read(opts[:name], idx)
      end)
    end

    test "read cold data", %{opts: opts} do
      start_supervised!({ExWal, opts})

      entries =
        1..1000
        |> Enum.map(fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      1..1000
      |> Enum.each(fn idx ->
        assert {:ok, "Hello Elixir #{idx}"} == ExWal.read(opts[:name], idx)
      end)
    end
  end
end
