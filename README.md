# ExWal

<!-- MDOC !-->

**ExWal is a project that aims to provide a solution for managing write-ahead log (WAL) in Elixir.**

## Installation

The package can be installed by adding `ex_wal` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_wal, "~> 0.2"}
  ]
end
```

## Design
The design of this project borrows from the implementation of WAL in the [Pebble](https://github.com/cockroachdb/pebble) project.


## Usage

1. Prepare a instance
```Elixir
defmodule MyApp do
  use ExWal, otp_app: :my_app
end
```

2. Config this instance
```Elixir
config :my_app, MyApp,
  path: "/tmp/wal_test",
  nosync: false,
  # 4k per segment
  segment_size: 4 * 1024,
  # cache max 5 segments
  segment_cache_size: 5
```

3. Add it to supervised tree
```elixir
Supervisor.start_link(
  [
    MyApp
  ],
  strategy: :one_for_one
)
```

4. Enjoy your journey

```Elixir
latest = MyApp.last_index()
Logger.info("latest: #{latest}")

# write 10k entries
entries =
  Enum.map((latest + 1)..(latest + 10_000), fn i -> Entry.new(i, "Hello Elixir #{i}") end)

:ok = MyApp.write(entries)

latest = MyApp.last_index()
Logger.info("latest: #{latest}") # should be latest + 10_000

# read
{:ok, ret} = MyApp.read(latest - 10)
Logger.info("idx: #{latest - 10}, content: #{ret}")

# truncate before
:ok = MyApp.truncate_before(latest - 100)
first = MyApp.first_index()
Logger.info("first: #{first}") # should be latest - 100

# truncate after
:ok = MyApp.truncate_after(latest - 5)
latest = MyApp.last_index()
Logger.info("latest: #{latest}") # should be latest - 5

# reinit wal
:ok = MyApp.reinit()

# clear all wal data
:ok = MyApp.clear()
```


## Benchmark

See [benchmarks/report.md](benchmarks/report.md) for more details.

-----
Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ex_wal>.

