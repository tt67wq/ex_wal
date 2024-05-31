defmodule ExWalTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.Models.Entry
  alias ExWal.Test.App

  require Logger

  @path "./tmp/test"

  defp cleanup(_ctx) do
    File.rm_rf!(@path)
    :ok
  end

  defp add_config(_ctx) do
    Application.put_env(:app, ExWal.Test.App, path: @path)
    :ok
  end

  describe "start_link" do
    setup [:cleanup, :add_config]

    test "start_link with empty path" do
      start_supervised!(App)
      assert -1 == App.last_index()

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = App.write(entries)
    end

    test "start_link with existing path" do
      # create a segment file
      start_supervised!(App)

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = App.write(entries)

      assert 100 == App.last_index()
    end
  end

  describe "write" do
    setup [:cleanup, :add_config]

    test "write multiple entries" do
      start_supervised!(App)
      assert -1 == App.last_index()

      max_idx = 1000

      entries =
        Enum.map(0..max_idx, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = App.write(entries)
      assert max_idx == App.last_index()

      files = Path.wildcard(@path <> "/*")

      assert App.segment_count() == Enum.count(files)
    end
  end

  describe "read" do
    setup [:cleanup, :add_config]

    test "read not_exists" do
      start_supervised!(App)

      assert {:error, :index_not_found} == App.read(0)
      assert {:error, :index_not_found} == App.read(100)
    end

    test "read random" do
      start_supervised!(App)

      max_idx = 1000

      entries =
        Enum.map(0..max_idx, fn i -> %Entry{index: i, data: "Hello Elixir #{i}", cache: "Cache #{i}"} end)

      :ok = App.write(entries)

      0..max_idx
      |> StreamData.integer()
      |> Enum.take(500)
      |> Enum.each(fn idx ->
        {:ok, %Entry{index: idx, data: data, cache: cache}} = App.read(idx)
        assert "Hello Elixir #{idx}" == data

        unless is_nil(cache) do
          assert "Cache #{idx}" == cache
        end
      end)
    end
  end

  describe "index" do
    setup [:cleanup, :add_config]

    test "last_index" do
      start_supervised!(App)

      assert -1 == App.last_index()

      assert :ok == App.write([%Entry{index: 0, data: "Hello Elixir 1"}])
      assert 0 == App.last_index()

      entries =
        Enum.map(1..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = App.write(entries)

      assert 100 == App.last_index()
    end
  end

  describe "truncate before" do
    setup [:cleanup, :add_config]

    test "truncate invalid index" do
      start_supervised!(App)

      assert {:error, :index_out_of_range} == App.truncate_before(100)

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = App.write(entries)

      assert {:error, :index_out_of_range} == App.truncate_before(101)
      assert {:error, :index_out_of_range} == App.truncate_before(-1)

      assert :ok == App.truncate_before(10)
      assert 10 == App.first_index()
      assert {:error, :index_out_of_range} == App.truncate_before(9)
    end

    test "chaos" do
      start_supervised!(App)

      max_entries = 1000
      test_round = 500

      0..(max_entries - 1)
      |> StreamData.integer()
      |> Enum.take(test_round)
      |> Enum.each(fn to_truncate ->
        Logger.info("to_truncate: #{to_truncate}")

        entries =
          Enum.map(0..max_entries, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

        :ok = App.write(entries)

        :ok = App.truncate_before(to_truncate)

        assert to_truncate == App.first_index()

        Enum.each(to_truncate..max_entries, fn idx ->
          {:ok, %Entry{index: idx, data: data, cache: cache}} = App.read(idx)
          assert "Hello Elixir #{idx}" == data

          unless is_nil(cache) do
            assert "Cache #{idx}" == cache
          end
        end)

        if to_truncate - 1 > 0 do
          Enum.each(1..(to_truncate - 1), fn idx ->
            assert {:error, :index_not_found} = App.read(idx)
          end)
        end

        files = Path.wildcard(@path <> "/*")

        assert App.segment_count() == Enum.count(files)

        App.clear()
      end)
    end
  end

  describe "truncate after" do
    setup [:cleanup, :add_config]

    test "truncate invalid index" do
      start_supervised!(App)

      assert {:error, :index_out_of_range} == App.truncate_after(100)

      entries =
        Enum.map(0..100, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

      :ok = App.write(entries)

      assert {:error, :index_out_of_range} == App.truncate_after(101)

      assert :ok == App.truncate_after(10)
      assert 10 == App.last_index()
      assert {:error, :index_out_of_range} == App.truncate_after(11)

      assert :ok == App.truncate_after(-1)
      assert -1 == App.last_index()
    end

    test "chaos" do
      start_supervised!(App)

      max_entries = 1000
      test_round = 500

      1..max_entries
      |> StreamData.integer()
      |> Enum.take(test_round)
      |> Enum.each(fn to_truncate ->
        Logger.info("to_truncate: #{to_truncate}")

        entries =
          Enum.map(0..max_entries, fn i -> %Entry{index: i, data: "Hello Elixir #{i}"} end)

        :ok = App.write(entries)

        :ok = App.truncate_after(to_truncate)

        assert to_truncate == App.last_index()

        Enum.each(1..to_truncate, fn idx ->
          {:ok, %Entry{index: idx, data: data, cache: cache}} = App.read(idx)
          assert "Hello Elixir #{idx}" == data

          unless is_nil(cache) do
            assert "Cache #{idx}" == cache
          end
        end)

        if to_truncate + 1 <= max_entries do
          Enum.each((to_truncate + 1)..max_entries, fn idx ->
            assert {:error, :index_not_found} = App.read(idx)
          end)
        end

        files = Path.wildcard(@path <> "/*")

        assert App.segment_count() == Enum.count(files)

        App.clear()
      end)
    end
  end
end
