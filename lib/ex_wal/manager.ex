defprotocol ExWal.Manager do
  @moduledoc """
  The manager protocol for the WAL.
  """
  def list(impl)

  def obsolete(impl, min_log_num, recycle?)

  def create(impl, log_num)

  def close(impl)
end

defmodule ExWal.Manager.Standalone do
  @moduledoc false

  defstruct name: nil, recycler: nil
end
