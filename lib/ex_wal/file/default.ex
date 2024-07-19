defmodule ExWal.File.Default do
  @moduledoc false

  defstruct fd: nil
end

defimpl ExWal.File, for: ExWal.File.Default do
  alias ExWal.File.Default

  @type reason_t :: File.posix() | :badarg | :terminated

  @spec close(ExWal.File.t()) :: :ok | {:error, reason :: reason_t()}
  def close(%Default{fd: fd}) do
    :file.close(fd)
  end

  @spec read(ExWal.File.t(), n_bytes :: non_neg_integer) :: {:ok, binary :: binary()} | :eof | {:error, reason :: term}
  def read(%Default{fd: fd}, n_bytes) do
    :file.read(fd, n_bytes)
  end

  @spec read_at(ExWal.File.t(), n_bytes :: non_neg_integer, offset :: non_neg_integer) ::
          {:ok, binary :: binary()} | :eof | {:error, reason :: term}
  def read_at(%Default{fd: fd}, n_bytes, offset) do
    :file.pread(fd, offset, n_bytes)
  end

  @spec write(ExWal.File.t(), data :: binary) :: :ok | {:error, reason :: reason_t()}
  def write(%Default{fd: fd}, data) do
    :file.write(fd, data)
  end

  @spec write_at(ExWal.File.t(), data :: binary, offset :: non_neg_integer) :: :ok | {:error, reason :: reason_t()}
  def write_at(%Default{fd: fd}, data, offset) do
    :file.pwrite(fd, offset, data)
  end

  @spec prealloc(ExWal.File.t(), offset :: non_neg_integer, length :: non_neg_integer) :: :ok
  def prealloc(%Default{fd: fd}, offset, length), do: :file.allocate(fd, offset, length)

  @spec stat(ExWal.File.t()) :: {:ok, File.Stat.t()} | {:error, reason :: File.posix() | :badarg}
  def stat(%Default{fd: fd}) do
    with {:ok, info} <- :file.read_file_info(fd), do: {:ok, File.Stat.from_record(info)}
  end

  @spec sync(ExWal.File.t()) :: :ok | {:error, reason :: reason_t()}
  def sync(%Default{fd: fd}) do
    :file.sync(fd)
  end

  @spec sync_to(ExWal.File.t(), length :: non_neg_integer) ::
          {:ok, full_sync? :: boolean()} | {:error, reason :: reason_t()}
  def sync_to(%Default{fd: fd}, _length) do
    # Ensures that any buffers kept by the operating system (not by the Erlang runtime system) are written to disk.
    # In many ways it resembles fsync but it does not update some of the metadata of the file, such as the access time.
    # On some platforms this function has no effect.
    # Applications that access databases or log files often write a tiny data fragment (for example, one line in a log file)
    # and then call fsync() immediately to ensure that the written data is physically stored on the hard disk.
    # Unfortunately, fsync() always initiates two write operations: one for the newly written data and another one to update the modification time stored in the inode.
    # If the modification time is not a part of the transaction concept, fdatasync() can be used to avoid unnecessary inode disk write operations.
    # Available only in some POSIX systems, this call results in a call to fsync(), or has no effect in systems not providing the fdatasync() syscall.
    fd
    |> :file.datasync()
    |> case do
      :ok ->
        {:ok, true}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec prefetch(ExWal.File.t(), offset :: non_neg_integer, length :: non_neg_integer) :: {:error, reason :: term()}
  def prefetch(%Default{}, _offset, _length) do
    {:error, :not_supported}
  end

  @spec fd(ExWal.File.t()) :: File.io_device()
  def fd(%Default{fd: fd}), do: fd
end
