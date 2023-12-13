alias ExWal.Models.Entry

require Logger

Mix.install([
  {:ex_wal, path: "../ex_wal"}
])

Supervisor.start_link(
  [
    {ExWal,
     [
       name: :wal_test,
       path: "/tmp/wal_test",
       nosync: false,
       # 4k
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
Logger.info("latest: #{latest}")

# read
{:ok, ret} = ExWal.read(:wal_test, latest - 10)
Logger.info("idx: #{latest - 10}, content: #{ret}")

# truncate before
:ok = ExWal.truncate_before(:wal_test, latest - 100)
first = ExWal.first_index(:wal_test)
Logger.info("first: #{first}")

# truncate after
:ok = ExWal.truncate_after(:wal_test, latest - 5)
latest = ExWal.last_index(:wal_test)
Logger.info("latest: #{latest}")

ExWal.clear(:wal_test)
ExWal.stop(:wal_test)
