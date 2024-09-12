defmodule Manager.FailoverTest do
  @moduledoc false

  use ExUnit.Case

  alias ExWal.FS.Default
  alias ExWal.FS.Syncing
  alias ExWal.LogReader
  alias ExWal.LogWriter
  alias ExWal.Manager.Failover
  alias ExWal.Manager.Options
  alias ExWal.Models

  require Logger

  @path_primary "./tmp/monitor_test/primary"
  @path_secondary "./tmp/monitor_test/secondary"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path_primary)
    File.rm_rf(@path_secondary)
    :ok
  end

  setup_all do
    default = %Default{}
    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_dynamic_sup})
    start_supervised!({Syncing, {:test_fs, default, :test_dynamic_sup, :test_registry}})

    fs = Syncing.init(:test_fs, default, :test_dynamic_sup, :test_registry)

    start_supervised!({
      Failover,
      {
        :test_manager,
        :test_dynamic_sup,
        :test_registry,
        %Options{
          primary: [
            fs: fs,
            dir: @path_primary
          ],
          secondary: [
            fs: fs,
            dir: @path_secondary
          ],
          max_num_recyclable_logs: 10
        }
      }
    })

    :ok
  end

  test "main" do
    assert {:ok, writer} = Failover.create(:test_manager, 1)
    assert {:ok, _} = LogWriter.write_record(writer, "test manager")
    assert {:ok, [%Models.VirtualLog{log_num: 1}]} = Failover.list(:test_manager)
    assert {:ok, reader} = Failover.open_for_read(:test_manager, 1)
    assert "test manager" = LogReader.next(reader)
    assert :eof = LogReader.next(reader)
    ExWal.LogWriter.stop(writer)
    ExWal.LogReader.stop(reader)
  end
end
