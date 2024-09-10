defmodule Manager.StandaloneTest do
  @moduledoc false

  use ExUnit.Case

  alias ExWal.FS.Default
  alias ExWal.FS.Syncing
  alias ExWal.LogReader
  alias ExWal.LogWriter
  alias ExWal.Manager.Options
  alias ExWal.Manager.Standalone

  @path "./tmp/manager_test"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path)
    :ok
  end

  setup_all do
    default = %Default{}
    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_dynamic_sup})
    start_supervised!({Syncing, {:test_fs, default, :test_dynamic_sup, :test_registry}})

    start_supervised!({
      ExWal.Manager.Standalone,
      {
        :test_manager,
        :test_dynamic_sup,
        :test_registry,
        %Options{
          primary: [
            fs: Syncing.get(:test_fs),
            dir: @path
          ]
        }
      }
    })

    :ok
  end

  test "main" do
    assert {:ok, writer} = Standalone.create(:test_manager, 1)
    assert {:ok, _} = LogWriter.write_record(writer, "test manager")
    assert {:ok, [%ExWal.Models.VirtualLog{log_num: 1}]} = Standalone.list(:test_manager)
    assert {:ok, reader} = Standalone.open_for_read(:test_manager, 1)
    assert "test manager" = LogReader.next(reader)
    assert :eof = LogReader.next(reader)
  end
end
