alias ExWal.Models.Entry

require Logger

Mix.install([
  {:ex_wal, path: "../ex_wal"}
])

defmodule SampleApp do
  @moduledoc false

  use ExWal, otp_app: :sample
end

Application.put_env(:sample, SampleApp, path: "/tmp/exwal_sample")

Supervisor.start_link(
  [
    SampleApp
  ],
  strategy: :one_for_one
)

latest = SampleApp.last_index()
Logger.info("latest: #{latest}")

# write 10k entries
entries =
  Enum.map((latest + 1)..(latest + 10_000), fn i -> Entry.new(i, "Hello Elixir #{i}") end)

:ok = SampleApp.write(entries)

latest = SampleApp.last_index()
Logger.info("latest: #{latest}")

# read
{:ok, ret} = SampleApp.read(latest - 10)
Logger.info("idx: #{latest - 10}, content: #{ret}")

# truncate before
:ok = SampleApp.truncate_before(latest - 100)
first = SampleApp.first_index()
Logger.info("first: #{first}")

# truncate after
:ok = SampleApp.truncate_after(latest - 5)
latest = SampleApp.last_index()
Logger.info("latest: #{latest}")

SampleApp.clear()
