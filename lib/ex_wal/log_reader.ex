defprotocol ExWal.LogReader do
  @spec next(t()) :: {:ok, bytes :: binary()} | :eof | {:error, reason :: term()}
  def next(impl)

  @spec recovery(t()) :: :ok
  def recovery(impl)
end
