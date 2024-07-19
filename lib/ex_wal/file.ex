defprotocol ExWal.File do
  @moduledoc """
  File protocol
  """

  @spec close(t()) :: :ok | {:error, reason :: term}
  def close(impl)

  @doc """
  read reads n bytes from the file
  """
  @spec read(impl :: t(), n_bytes :: non_neg_integer()) :: {:ok, bytes :: binary()} | :eof | {:error, reason :: term}
  def read(impl, n_bytes)

  @doc """
  read_at reads n bytes from the file at the given offset of datasource
  """
  @spec read_at(impl :: t(), n_bytes :: non_neg_integer(), offset :: non_neg_integer()) ::
          {:ok, bytes :: binary()} | :eof | {:error, reason :: term}
  def read_at(impl, n_bytes, offset)

  @doc """
  write writes bytes to the file
  """
  @spec write(impl :: t(), bytes :: binary()) :: :ok | {:error, reason :: term}
  def write(impl, bytes)

  @doc """
  write_at writes bytes to the file to the underlying data stream
  """
  @spec write_at(impl :: t(), bytes :: binary(), offset :: non_neg_integer()) :: :ok | {:error, reason :: term}
  def write_at(impl, bytes, offset)

  @doc """
  prealloc optionally preallocates storage for `length` at `offset`
  within the file. Implementations may choose to do nothing.
  """
  @spec prealloc(impl :: t(), offset :: non_neg_integer(), length :: non_neg_integer()) :: :ok | {:error, reason :: term}
  def prealloc(impl, offset, length)

  @spec stat(impl :: t()) :: {:ok, stat :: File.Stat.t()} | {:error, reason :: term}
  def stat(impl)

  @spec sync(impl :: t()) :: :ok | {:error, reason :: term}
  def sync(impl)

  @doc """
  sync_to requests that a prefix of the file's data be synced to stable
  storage. The caller passes provides a `length`, indicating how many bytes
  to sync from the beginning of the file. sync_to is a no-op for
  directories, and therefore always returns false.
  """
  @spec sync_to(impl :: t(), length :: non_neg_integer()) :: {:ok, full_sync? :: boolean()} | {:error, reason :: term}
  def sync_to(impl, length)

  @doc """
  prefetch signals the OS (on supported platforms) to fetch the next length
  bytes in file after offset into cache. Any
  subsequent reads in that range will not issue disk IO.
  """
  @spec prefetch(impl :: t(), offset :: non_neg_integer(), length :: non_neg_integer()) :: :ok | {:error, reason :: term}
  def prefetch(impl, offset, length)

  @doc """
  fd returns the raw file descriptor when a File is backed by an :file.
  It can be used for specific functionality like Prefetch.
  Returns error if not supported.
  """
  @spec fd(impl :: t()) :: File.io_device() | {:error, reason :: term}
  def fd(impl)
end
