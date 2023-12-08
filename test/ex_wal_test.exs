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
    end
  end
end
