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

Benchee.run(
  %{
    "write" => fn {_, writer, data} ->
      ExWal.LogWriter.write_record(writer, data, timeout: 10000)
    end
  },
  inputs: %{
    "small value, nosync" => {small, []},
    "small value" => {small, []},
    "1KB value" => {one_kb, []},
    "1MB value" => {one_mb, []},
    "10MB value" => {ten_mb, []}
  },
  before_scenario: fn {data, options} ->
    Application.put_env(:bench, BenchmarkApp, options)
    {:ok, _} = BenchmarkApp.start_link()
    {:ok, manager} = BenchmarkApp.manager(data_dir, :standalone)
    {:ok, writer} = ExWal.Manager.create(manager, 1)
    {manager, writer, data}
  end,
  before_each: fn {manager, writer, data} ->
    {manager, writer, data}
  end,
  after_scenario: fn {manager, writer, _} ->
    ExWal.LogWriter.stop(writer)
    ExWal.Manager.close(manager)
    cleanup.()
  end
)
