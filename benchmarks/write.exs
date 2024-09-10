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
big_segment_size = 256 * 1024 * 1024
huge_segment_size = 1024 * 1024 * 1024

defmodule BenchmarkApp do
  @moduledoc false
  use ExWal, otp_app: :bench
end

Application.put_env(:bench, BenchmarkApp, [])
{:ok, _} = BenchmarkApp.start_link()
{:ok, manager} = BenchmarkApp.manager(data_dir, :standalone)

Benchee.run(
  %{
    "write" => fn {writer, data} ->
      ExWal.LogWriter.write_record(writer, data, timeout: 10_000)
    end
  },
  inputs: %{
    "small value" => {manager, small, [idx: 1]},
    "1KB value" => {manager, one_kb, [idx: 2]},
    "1MB value" => {manager, one_mb, [idx: 3]},
    "10MB value" => {manager, ten_mb, [idx: 4]}
  },
  before_scenario: fn {manager, data, options} ->
    {:ok, writer} = ExWal.Manager.create(manager, options[:idx])
    {writer, data}
  end,
  before_each: fn {writer, data} ->
    {writer, data}
  end,
  after_scenario: fn {writer, _} ->
    ExWal.LogWriter.stop(writer)
  end
)

ExWal.Manager.close(manager)
cleanup.()
