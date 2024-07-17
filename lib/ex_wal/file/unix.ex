defmodule ExWal.File.Unix do
  @moduledoc false

  defstruct fd: nil
end

defimpl ExWal.File, for: ExWal.File.Unix do
  alias ExWal.File.Unix

  @type reason_t :: File.posix() | :badarg | :terminated

  @spec close(ExWal.File.t()) :: :ok | {:error, reason :: reason_t()}
  def close(%Unix{fd: fd}) do
    File.close(fd)
  end

  @spec read(ExWal.File.t(), n_bytes :: non_neg_integer) :: {:ok, binary :: binary()} | :eof | {:error, reason :: term}
  def read(%Unix{fd: fd}, n_bytes) do
    :file.read(fd, n_bytes)
  end

  @spec read_at(ExWal.File.t(), n_bytes :: non_neg_integer, offset :: non_neg_integer) ::
          {:ok, binary :: binary()} | :eof | {:error, reason :: term}
  def read_at(%Unix{fd: fd}, n_bytes, offset) do
    :file.pread(fd, offset, n_bytes)
  end

  @spec write(ExWal.File.t(), data :: binary) :: :ok | {:error, reason :: reason_t()}
  def write(%Unix{fd: fd}, data) do
    :file.write(fd, data)
  end

  @spec write_at(ExWal.File.t(), data :: binary, offset :: non_neg_integer) :: :ok | {:error, reason :: reason_t()}
  def write_at(%Unix{fd: fd}, data, offset) do
    :file.pwrite(fd, offset, data)
  end

  @spec stat(ExWal.File.t()) :: {:ok, File.Stat.t()} | {:error, reason :: File.posix() | :badarg}
  def stat(%Unix{fd: fd}) do
    with {:ok, info} <- :file.read_file_info(fd), do: {:ok, File.Stat.from_record(info)}
  end

  @spec sync(ExWal.File.t()) :: :ok | {:error, reason :: reason_t()}
  def sync(%Unix{fd: fd}) do
    :file.sync(fd)
  end

  @spec sync_to(ExWal.File.t(), length :: non_neg_integer) ::
          {:ok, boolean :: boolean()} | {:error, reason :: reason_t(), boolean :: boolean()}
  def sync_to(%Unix{fd: fd}, _length) do
    fd
    |> :file.sync()
    |> case do
      :ok -> {:ok, true}
      {:error, reason} -> {:error, reason, false}
    end
  end

  @spec prefetch(ExWal.File.t(), offset :: non_neg_integer, length :: non_neg_integer) :: {:error, reason :: term()}
  def prefetch(%Unix{}, _offset, _length) do
    {:error, :not_supported}
  end

  @spec fd(ExWal.File.t()) :: File.io_device()
  def fd(%Unix{fd: fd}), do: fd
end
