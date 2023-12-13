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

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ex_wal>.

