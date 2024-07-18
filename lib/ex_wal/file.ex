defprotocol ExWal.File do
  @moduledoc """
  File protocol
  """

  def close(impl)

  @doc """
  read reads n bytes from the file
  """
  def read(impl, n_bytes)

  @doc """
  read_at reads n bytes from the file at the given offset of datasource
  """
  def read_at(impl, n_bytes, offset)

  @doc """
  write writes bytes to the file
  """
  def write(impl, bytes)

  @doc """
  write_at writes bytes to the file to the underlying data stream
  """
  def write_at(impl, bytes, offset)

  def stat(impl)

  def sync(impl)

  @doc """
  sync_to requests that a prefix of the file's data be synced to stable
  storage. The caller passes provides a `length`, indicating how many bytes
  to sync from the beginning of the file. sync_to is a no-op for
  directories, and therefore always returns false.
  """
  def sync_to(impl, length)

  @doc """
  prefetch signals the OS (on supported platforms) to fetch the next length
  bytes in file after offset into cache. Any
  subsequent reads in that range will not issue disk IO.
  """
  def prefetch(impl, offset, length)

  @doc """
  fd returns the raw file descriptor when a File is backed by an :file.
  It can be used for specific functionality like Prefetch.
  Returns error if not supported.
  """
  def fd(impl)
end
