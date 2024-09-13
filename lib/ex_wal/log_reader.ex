defprotocol ExWal.LogReader do
  @moduledoc """
  A LogReader takes an ordered sequence of physical WAL files
  ("segments"), providing a merged view of the WAL's logical contents.
  It's responsible for filtering duplicate records which may be shared by the tail of a segment file and the head of its
  successor.
  """

  @doc """
  next returns a next binary written to wal.
  It returns io.EOF if there are no more records.
  """
  @spec next(t()) :: {:ok, bytes :: binary()} | :eof | {:error, reason :: term()}
  def next(impl)

  @doc """
  recovery clear the rest part of current block and move to next block.
  """
  @spec recovery(t()) :: :ok
  def recovery(impl)

  @spec stop(t()) :: :ok
  def stop(impl)
end
