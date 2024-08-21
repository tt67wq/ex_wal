defprotocol ExWal.LogWriter do
  @spec write_record(impl :: t(), bytes :: binary()) ::
          {:ok, written_bytes :: non_neg_integer()} | {:error, reason :: term()}
  def write_record(impl, bytes)

  @spec stop(impl :: t()) :: :ok
  def stop(impl)
end
