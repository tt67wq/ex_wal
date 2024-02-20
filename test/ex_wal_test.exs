defmodule ExWalTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.Models.Entry

  require Logger

  setup do
    opts = [
      path: "./tmp/test",
      segment_size: 1024,
      name: :test
    ]

    File.rm_rf!(opts[:path])
    [opts: opts]
  end

  describe "start_link" do
    test "start_link with empty path", %{opts: opts} do
      start_supervised!({ExWal, opts})
      assert -1 == ExWal.last_index(opts[:name])

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)
    end

    test "start_link with existing path", %{opts: opts} do
      # create a segment file
      start_supervised!({ExWal, opts}, restart: :temporary)

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      assert 100 == ExWal.last_index(opts[:name])
    end
  end

  describe "write" do
    test "write multiple entries", %{opts: opts} do
      start_supervised!({ExWal, opts})
      assert -1 == ExWal.last_index(opts[:name])

      max_idx = 1000

      entries =
        Enum.map(0..max_idx, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)
      assert max_idx == ExWal.last_index(opts[:name])

      files = Path.wildcard(opts[:path] <> "/*")

      assert ExWal.segment_count(opts[:name]) == Enum.count(files)
    end
  end

  describe "read" do
    test "read not_exists", %{opts: opts} do
      start_supervised!({ExWal, opts})

      assert {:error, :index_not_found} == ExWal.read(opts[:name], 0)
      assert {:error, :index_not_found} == ExWal.read(opts[:name], 100)
    end

    test "read random", %{opts: opts} do
      start_supervised!({ExWal, opts})

      max_idx = 1000

      entries =
        Enum.map(0..max_idx, fn i -> %Entry{index: i, data: "Hello Elixir #{i}", cache: "Cache #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      0..max_idx
      |> StreamData.integer()
      |> Enum.take(500)
      |> Enum.each(fn idx ->
        {:ok, %Entry{index: idx, data: data, cache: cache}} = ExWal.read(opts[:name], idx)
        assert "Hello Elixir #{idx}" == data

        unless is_nil(cache) do
          assert "Cache #{idx}" == cache
        end
      end)
    end
  end

  test "last_index", %{opts: opts} do
    start_supervised!({ExWal, opts})

    assert -1 == ExWal.last_index(opts[:name])

    assert :ok == ExWal.write(opts[:name], [%Entry{index: 0, data: "Hello Elixir 1"}])
    assert 0 == ExWal.last_index(opts[:name])

    entries =
      Enum.map(1..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

    :ok = ExWal.write(opts[:name], entries)

    assert 100 == ExWal.last_index(opts[:name])
  end

  describe "truncate before" do
    test "truncate invalid index", %{opts: opts} do
      start_supervised!({ExWal, opts})

      assert {:error, :index_out_of_range} == ExWal.truncate_before(opts[:name], 100)

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      assert {:error, :index_out_of_range} == ExWal.truncate_before(opts[:name], 101)
      assert {:error, :index_out_of_range} == ExWal.truncate_before(opts[:name], -1)

      assert :ok == ExWal.truncate_before(opts[:name], 10)
      assert 10 == ExWal.first_index(opts[:name])
      assert {:error, :index_out_of_range} == ExWal.truncate_before(opts[:name], 9)
    end

    test "chaos", %{opts: opts} do
      start_supervised!({ExWal, opts})

      max_entries = 1000
      test_round = 500

      0..(max_entries - 1)
      |> StreamData.integer()
      |> Enum.take(test_round)
      |> Enum.each(fn to_truncate ->
        Logger.info("to_truncate: #{to_truncate}")

        entries =
          Enum.map(0..max_entries, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

        :ok = ExWal.write(opts[:name], entries)

        :ok = ExWal.truncate_before(opts[:name], to_truncate)

        assert to_truncate == ExWal.first_index(opts[:name])

        Enum.each(to_truncate..max_entries, fn idx ->
          {:ok, %Entry{index: idx, data: data, cache: cache}} = ExWal.read(opts[:name], idx)
          assert "Hello Elixir #{idx}" == data

          unless is_nil(cache) do
            assert "Cache #{idx}" == cache
          end
        end)

        if to_truncate - 1 > 0 do
          Enum.each(1..(to_truncate - 1), fn idx ->
            assert {:error, :index_not_found} = ExWal.read(opts[:name], idx)
          end)
        end

        files = Path.wildcard(opts[:path] <> "/*")

        assert ExWal.segment_count(opts[:name]) == Enum.count(files)

        ExWal.clear(opts[:name])
      end)
    end
  end

  describe "truncate after" do
    @tag :run
    test "truncate invalid index", %{opts: opts} do
      start_supervised!({ExWal, opts})

      assert {:error, :index_out_of_range} == ExWal.truncate_after(opts[:name], 100)

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = ExWal.write(opts[:name], entries)

      assert {:error, :index_out_of_range} == ExWal.truncate_after(opts[:name], 101)

      assert :ok == ExWal.truncate_after(opts[:name], 10)
      assert 10 == ExWal.last_index(opts[:name])
      assert {:error, :index_out_of_range} == ExWal.truncate_after(opts[:name], 11)

      assert :ok == ExWal.truncate_after(opts[:name], -1)
      assert -1 == ExWal.last_index(opts[:name])
    end

    @tag :run
    test "chaos", %{opts: opts} do
      start_supervised!({ExWal, opts})

      max_entries = 1000
      test_round = 500

      1..max_entries
      |> StreamData.integer()
      |> Enum.take(test_round)
      |> Enum.each(fn to_truncate ->
        Logger.info("to_truncate: #{to_truncate}")

        entries =
          Enum.map(0..max_entries, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

        :ok = ExWal.write(opts[:name], entries)

        :ok = ExWal.truncate_after(opts[:name], to_truncate)

        assert to_truncate == ExWal.last_index(opts[:name])

        Enum.each(1..to_truncate, fn idx ->
          {:ok, %Entry{index: idx, data: data, cache: cache}} = ExWal.read(opts[:name], idx)
          assert "Hello Elixir #{idx}" == data

          unless is_nil(cache) do
            assert "Cache #{idx}" == cache
          end
        end)

        if to_truncate + 1 <= max_entries do
          Enum.each((to_truncate + 1)..max_entries, fn idx ->
            assert {:error, :index_not_found} = ExWal.read(opts[:name], idx)
          end)
        end

        files = Path.wildcard(opts[:path] <> "/*")

        assert ExWal.segment_count(opts[:name]) == Enum.count(files)

        ExWal.clear(opts[:name])
      end)
    end
  end
end
