defprotocol ExWal.LogReader do
  @moduledoc """
  Reader reads a virtual WAL.
  """

  @doc """
  next returns a binary in the next record. It returns io.EOF if there
  are no more records.
  """
  @spec next(t()) :: {:ok, bytes :: binary()} | :eof | {:error, reason :: term()}
  def next(impl)

  @spec recovery(t()) :: :ok
  def recovery(impl)
end
