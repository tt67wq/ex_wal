alias ExWal.Models.Entry

require Logger

Mix.install([
  {:ex_wal, path: "../ex_wal"}
])

defmodule ProfileApp do
  @moduledoc false

  use ExWal, otp_app: :profile
end

segment_size = 128 * 1024 * 1024
config = [path: "/tmp/exwal_profile", segment_size: segment_size]

Application.put_env(:profile, ProfileApp, config)

Supervisor.start_link(
  [
    ProfileApp
  ],
  strategy: :one_for_one,
  name: :demo
)

data = "Anytime you apply a rule too universally, it turns into an anti-pattern"

:eprof.start_profiling([self()])

Enum.each(1..1000, fn _ ->
  latest = ProfileApp.last_index()
  Logger.info("latest: #{latest}")
  entries = Enum.map((latest + 1)..(latest + 3000), fn i -> Entry.new(i, data) end)

  :ok = ProfileApp.write(entries)

  latest = ProfileApp.last_index()
  Logger.info("latest: #{latest}")
end)

# :ok = ProfileApp.clear()

Supervisor.stop(:demo)

:eprof.stop_profiling()
:eprof.analyze()
