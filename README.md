# ExWal

<!-- MDOC !-->

**ExWal is a project that aims to provide a solution for managing write-ahead log (WAL) in Elixir.**

ExWal is a GenServer-based module that provides functionality for working with a Write-Ahead Log (WAL). 

The module includes utilities for managing WAL files, segments, blocks, and entries. It also supports options for customizing the WAL path, synchronization behavior, segment size, segment cache size, and the underlying store module.

For more information on how to use ExWal, refer to the [documentation](https://hex.pm/docs/publish) and examples.

## Installation

The package can be installed by adding `ex_wal` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_wal, "~> 0.1.0"}
  ]
end
```

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

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ex_wal>.

