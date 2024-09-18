# ExWal

<!-- MDOC !-->

**ExWal is a project that aims to provide a solution for managing write-ahead log (WAL) in Elixir.**

## Installation

The package can be installed by adding `ex_wal` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_wal, "~> 0.3"}
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

2. Choose your file system implementation
ExWal provides 2 file system implementations:
- `ExWal.FS.Default`
- `ExWal.FS.Syncing`

`ExWal.FS.Syncing` provider better write performance. You can use `MyApp.syncing_fs/0` to get a `ExWal.FS.Syncing` instance.


3. Add it to supervised tree
```elixir
Supervisor.start_link(
  [
    MyApp
  ],
  strategy: :one_for_one
)
```

4. Get a manager

ExWal provides 2 manager implementations:
- `ExWal.Manager.Standalone`
- `ExWal.Manager.Failover`

You can use `MyApp.manager/3` to get a `ExWal.Manager` instance.

```Elixir
# get a standalone manager
{:ok, m} =
      MyApp.manager(:standalone, "my-manager", %ExWal.Manager.Options{
        primary: [
          fs: MyApp.syncing_fs(),
          dir: "my-primary-dir-path"
        ]
      })

# get a failover manager
{:ok, m} =
      MyApp.manager(:failover, "my-manager", %ExWal.Manager.Options{
        primary: [
          fs: MyApp.syncing_fs(),
          dir: "my-primary-dir-path"
        ],
        secondary: [
          fs: MyApp.syncing_fs(),
          dir: "my-secondary-dir-path"
        ]
      })
```

5. Create a WAL writer

Manager provides a `create/2` function to create a WAL writer. Writer is a instance which implements `ExWal.LogWriter` protocol.

```Elixir
{:ok, m} =
      MyApp.manager(:standalone, "my-manager", %ExWal.Manager.Options{
        primary: [
          fs: MyApp.syncing_fs(),
          dir: "my-primary-dir-path"
        ]
      })

{:ok, writer} = ExWal.Manager.create(m, 1)

1..50
|> Enum.map(fn x ->
  s =
    x
    |> Integer.to_string()
    |> String.pad_leading(4, "0")

  "Hello Elixir! I am a developer. I love Elixir #{s}."
end)
|> Enum.each(fn data -> LogWriter.write_record(writer, data) end)
```

6. Create a WAL reader

Manager provides a `list/2` function to list all WAL files. You can use `MyApp.open_for_read/1` function to create a Reader.  Reader is a instance which implements `ExWal.LogReader` protocol.

```Elixir
{:ok, m} =
      MyApp.manager(:standalone, "my-manager", %ExWal.Manager.Options{
        primary: [
          fs: MyApp.syncing_fs(),
          dir: "my-primary-dir-path"
        ]
      })

{:ok, [log | _]} = ExWal.Manager.list(m)
{:ok, reader} = MyApp.open_for_read(log)

reader
|> Enum.reduce_while(fn reader ->
  case LogReader.next(reader) do
    :eof ->
      {:halt, :ok}

    {:error, _reason} ->
      LogReader.recovery(reader)
      {:cont, reader}

    # raise ExWal.Exception, message: "read failed: #{inspect(reason)}"

    bin ->
      IO.puts(bin)
      {:cont, reader}
  end
end)
```


## Benchmark

See [benchmarks/report.md](benchmarks/report.md) for more details.

-----
Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ex_wal>.

