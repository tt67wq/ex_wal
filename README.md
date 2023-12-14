# ExWal

<!-- MDOC !-->

**ExWal is a project that aims to provide a solution for managing write-ahead log (WAL) in Elixir.**

ExWal is a GenServer-based module that provides functionality for working with a Write-Ahead Log (WAL). 

The module includes utilities for managing WAL files, segments, blocks, and entries. It also supports options for customizing the WAL path, synchronization behavior, segment size, segment cache size, and the underlying store module.

## Installation

The package can be installed by adding `ex_wal` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_wal, "~> 0.1.1"}
  ]
end
```

## Design
This project has heavily borrowed the code experience from the project at [tidwall/wal](https://github.com/tidwall/wal). In terms of design, WAL packages log entries into individual segments for maintenance. I take the last segment being written as the "hot" part, and while writing hot data, corresponding files will be appended. When the volume of "hot" data exceeds the configured value, we will transfer the hot data to the "cold" part and reopen a new "hot" segment.

In the process of reading, in order to avoid frequent opening of cold data, I added an LRU cache to accelerate the reading speed of local data.

In the operations of maintaining Segment and corresponding log entry, there are a considerable number of operations that find field based on index. However, the performance of [List](https://hexdocs.pm/elixir/List.html) provided by Elixir is not ideal in this situation, so we choose to use the [array](https://www.erlang.org/doc/man/array) module of erlang to store segment and block.

## Usage

```elixir
Supervisor.start_link(
  [
    {ExWal,
     [
       name: :wal_test,
       path: "/tmp/wal_test",
       nosync: false,
       # 4k per segment
       segment_size: 4 * 1024,
       # cache max 5 segments
       segment_cache_size: 5
     ]}
  ],
  strategy: :one_for_one
)

latest = ExWal.last_index(:wal_test)
Logger.info("latest: #{latest}")

# write 10k entries
entries =
  Enum.map((latest + 1)..(latest + 10_000), fn i -> Entry.new(i, "Hello Elixir #{i}") end)

:ok = ExWal.write(:wal_test, entries)

latest = ExWal.last_index(:wal_test)
Logger.info("latest: #{latest}") # should be latest + 10_000

# read
{:ok, ret} = ExWal.read(:wal_test, latest - 10)
Logger.info("idx: #{latest - 10}, content: #{ret}")

# truncate before
:ok = ExWal.truncate_before(:wal_test, latest - 100)
first = ExWal.first_index(:wal_test)
Logger.info("first: #{first}") # should be latest - 100

# truncate after
:ok = ExWal.truncate_after(:wal_test, latest - 5)
latest = ExWal.last_index(:wal_test)
Logger.info("latest: #{latest}") # should be latest - 5
```


## Benchmark

See [benchmarks/report.md](benchmarks/report.md) for more details.

-----
Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ex_wal>.

