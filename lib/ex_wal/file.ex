defmodule ExWal.Store.File do
  @moduledoc false

  defstruct path: ""
end

defmodule ExWal.StoreHandler.File do
  @moduledoc false

  defstruct handler: nil
end

defmodule ExWal.StoreManager.File do
  @moduledoc false

  defstruct []
end

defimpl ExWal.Store, for: ExWal.Store.File do
  alias ExWal.Store

  @spec open(Store.t(), Keyword.t()) :: {:ok, ExWal.StoreHandler.t()} | {:error, term()}
  def open(%Store.File{path: path}, opts \\ []) do
    mode = Keyword.get(opts, :mode, 0o640)

    path
    |> :file.open([:append, :raw, :binary])
    |> case do
      {:ok, io} ->
        :ok = :file.change_mode(path, mode)
        {:ok, %ExWal.StoreHandler.File{handler: io}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec read_all(Store.t()) :: {:ok, binary()} | {:error, term()}
  def read_all(%Store.File{path: path}) do
    :file.read_file(path)
  end

  def write_all(%Store.File{path: path}, data) do
    :file.write_file(path, data)
  end
end

defimpl ExWal.StoreHandler, for: ExWal.StoreHandler.File do
  def append(%ExWal.StoreHandler.File{handler: handler}, data) do
    :file.write(handler, data)
  end

  def sync(%ExWal.StoreHandler.File{handler: handler}) do
    :file.sync(handler)
  end

  def close(%ExWal.StoreHandler.File{handler: handler}) do
    :file.close(handler)
  end
end

defimpl ExWal.StoreManager, for: ExWal.StoreManager.File do
  @spec new_store(ExWal.StoreManager.t(), String.t()) :: ExWal.Store.t()
  def new_store(_, path) do
    %ExWal.Store.File{path: path}
  end

  def mkdir(_, path, opts) do
    mode = Keyword.get(opts, :mode, 0o755)
    File.mkdir_p!(path)
    File.chmod!(path, mode)
  end

  def rename(_, old_path, new_path) do
    File.rename!(old_path, new_path)
  end

  def rm(_, path) do
    File.rm!(path)
  end

  def wildcard(_, path) do
    Path.wildcard(path)
  end

  def dir?(_, path) do
    File.dir?(path)
  end
end

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
