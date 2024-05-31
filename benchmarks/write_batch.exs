data_dir = "./tmp/wal_write"

cleanup = fn ->
  with {:ok, files} <- File.ls(data_dir) do
    for file <- files, do: File.rm(Path.join(data_dir, file))
    File.rmdir(data_dir)
  end
end

small = "small value"
{:ok, one_kb} = File.read("benchmarks/data/1k")
{:ok, one_mb} = File.read("benchmarks/data/1m")
{:ok, ten_mb} = File.read("benchmarks/data/10m")

small_segment_size = 16 * 1024 * 1024
big_segment_size = 128 * 1024 * 1024
huge_segment_size = 1024 * 1024 * 1024

defmodule BenchmarkApp do
  @moduledoc false
  use ExWal, otp_app: :bench
end

Benchee.run(
  %{
    "ExWal.write/2" => fn entries ->
      BenchmarkApp.write(entries)
    end
  },
  inputs: %{
    "small * 10 value, nosync" => {small, [path: data_dir, nosync: true, segment_size: small_segment_size]},
    "small * 10 value" => {small, [path: data_dir, segment_size: small_segment_size]},
    "1KB * 10 value" => {one_kb, [path: data_dir, segment_size: small_segment_size]},
    "1MB * 10 value" => {one_mb, [path: data_dir, segment_size: big_segment_size]},
    "10MB * 10 value" => {ten_mb, [path: data_dir, segment_size: huge_segment_size]}
  },
  before_scenario: fn {datas, options} ->
    Application.put_env(:bench, BenchmarkApp, options)
    {:ok, _} = BenchmarkApp.start_link()
    datas
  end,
  before_each: fn value ->
    idx = BenchmarkApp.last_index()

    entries =
      1..10
      |> Enum.with_index(idx + 1)
      |> Enum.map(fn {_, i} -> ExWal.Models.Entry.new(i, value) end)

    entries
  end,
  after_scenario: fn _ ->
    IO.puts("#{BenchmarkApp.last_index()} entries written to WAL.")
    BenchmarkApp.clear()
    cleanup.()
  end
)
