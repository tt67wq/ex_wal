defmodule File.SyncingTest do
  @moduledoc false

  use ExUnit.Case

  alias ExWal.FS
  alias ExWal.FS.Syncing

  @path "./tmp/file_test"

  setup :cleanup

  defp cleanup(_) do
    File.rm_rf(@path)
    :ok
  end

  setup_all do
  end
end
