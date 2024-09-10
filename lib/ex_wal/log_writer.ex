defprotocol ExWal.LogWriter do
  @moduledoc """
  Writer writes to a virtual WAL. A Writer in standalone mode maps to a
  single LogWriter. In failover mode, it can failover across multiple
  physical log files.
  """

  @doc """
  write_record writes a complete record. The record is asynchronously persisted to the underlying writer.
  """
  @spec write_record(
          impl :: t(),
          bytes :: binary(),
          opts :: keyword()
        ) ::
          {:ok, written_bytes :: non_neg_integer()} | {:error, reason :: term()}
  def write_record(impl, bytes, opts \\ [])

  @doc """
  stop stops the writer.
  """
  @spec stop(impl :: t()) :: :ok
  def stop(impl)
end
