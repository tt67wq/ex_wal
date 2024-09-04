defprotocol ExWal.Manager do
  @moduledoc """
  The manager protocol for the WAL.
  """
  @spec list(impl :: t()) :: {:ok, [ExWal.Models.VirtualLog.t()]} | {:error, reason :: any()}
  def list(impl)

  @spec obsolete(
          impl :: t(),
          min_log_num :: ExWal.Models.VirtualLog.log_num(),
          recycle? :: boolean()
        ) ::
          {:ok, [ExWal.Models.Deletable.t()]} | {:error, reason :: any()}
  def obsolete(impl, min_log_num, recycle?)

  @spec create(impl :: t(), log_num :: ExWal.Models.VirtualLog.log_num()) ::
          {:ok, ExWal.LogWriter.t()} | {:error, reason :: any()}
  def create(impl, log_num)

  @spec close(impl :: t()) :: :ok | {:error, reason :: any()}
  def close(impl)
end
