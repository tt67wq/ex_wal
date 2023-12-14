data_dir = "./tmp/wal_write"

small = "small value"
{:ok, one_kb} = File.read("benchmarks/data/1k")
{:ok, one_mb} = File.read("benchmarks/data/1m")
{:ok, ten_mb} = File.read("benchmarks/data/10m")

small_segment_size = 16 * 1024 * 1024
big_segment_size = 128 * 1024 * 1024
huge_segment_size = 1024 * 1024 * 1024

Benchee.run(
  %{
    "ExWal.write/2" => fn {wal, entries} ->
      ExWal.write(wal, entries)
    end
  },
  inputs: %{
    "small * 10 value, nosync" =>
      {small, [path: data_dir, name: :small_nosync, nosync: true, segment_size: small_segment_size]},
    "small * 10 value" => {small, [path: data_dir, name: :small, segment_size: small_segment_size]},
    "1KB * 10 value" => {one_kb, [path: data_dir, name: :one_kb, segment_size: small_segment_size]},
    "1MB * 10 value" => {one_mb, [path: data_dir, name: :one_mb, segment_size: big_segment_size]},
    "10MB * 10 value" => {ten_mb, [path: data_dir, name: :ten_mb, segment_size: huge_segment_size]}
  },
  before_scenario: fn {datas, options} ->
    {:ok, _} = ExWal.start_link(options)
    {datas, options[:name]}
  end,
  before_each: fn {value, name} ->
    idx = ExWal.last_index(name)

    entries =
      1..10
      |> Enum.with_index(idx + 1)
      |> Enum.map(fn {_, i} -> ExWal.Models.Entry.new(i, value) end)

    {name, entries}
  end,
  after_scenario: fn {_value, name} ->
    IO.puts("#{ExWal.last_index(name)} entries written to WAL.")
    ExWal.clear(name)
    ExWal.stop(name)
  end
)
