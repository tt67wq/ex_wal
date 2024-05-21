defmodule ExWal.Models.Config do
  @moduledoc """
  Configure for Exwal
  """

  @schema [
    path: [
      type: :string,
      doc: "The path of the WAL. Default is `./wal`.",
      default: "./wal"
    ],
    nosync: [
      type: :boolean,
      doc: "If true, WAL will not sync to disk. Default is false.",
      default: false
    ],
    segment_size: [
      type: :integer,
      doc: "The size of each segment file. Default is 16MB.",
      default: 16 * 1024 * 1024
    ],
    segment_cache_size: [
      type: :integer,
      doc: "The size of the segment cache. Default is 100.",
      default: 2
    ],
    name: [
      type: :atom,
      doc: "The name of the WAL. Default is `wal`.",
      default: __MODULE__
    ]
  ]

  @type wal_option_schema_t :: [unquote(NimbleOptions.option_typespec(@schema))]

  def doc do
    NimbleOptions.docs(@schema)
  end

  def validate!(opts) do
    NimbleOptions.validate!(opts, @schema)
  end
end
