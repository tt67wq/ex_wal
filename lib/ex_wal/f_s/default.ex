defmodule ExWal.FS.Default do
  @moduledoc """
  Default implementation of the `ExWal.FS` protocols.
  """

  defstruct []
end

defimpl ExWal.FS, for: ExWal.FS.Default do
  @spec create(impl :: ExWal.FS.t(), name :: String.t()) :: {:ok, file :: ExWal.File.t()} | {:error, reason :: term}
  def create(_, name) do
    name
    |> File.exists?()
    |> if do
      File.rm(name)
    end

    with {:ok, f} <- File.open(name, [:write, :binary, :read, :exclusive, {:read_ahead, 4096}]),
         :ok <- File.chmod(name, 0o666) do
      {:ok, %ExWal.File.Default{fd: f}}
    end
  end

  @spec link(impl :: ExWal.FS.t(), old_name :: String.t(), new_name :: String.t()) ::
          :ok | {:error, reason :: File.posix()}
  def link(_, old_name, new_name) do
    File.ln(old_name, new_name)
  end

  @spec remove(impl :: ExWal.FS.t(), name :: String.t()) :: :ok | {:error, reason :: File.posix()}
  def remove(_, name) do
    File.rm(name)
  end

  @spec open(impl :: ExWal.FS.t(), name :: String.t(), opts :: Keyword.t()) ::
          {:ok, file :: ExWal.File.t()} | {:error, reason :: term}
  def open(_, name, _opts) do
    with {:ok, f} <- File.open(name, [:read, :binary, {:read_ahead, 4096}]),
         :ok <- File.chmod(name, 0o666) do
      {:ok, %ExWal.File.Default{fd: f}}
    end
  end

  @spec open_read_write(impl :: ExWal.FS.t(), name :: String.t(), opts :: Keyword.t()) ::
          {:ok, file :: ExWal.File.t()} | {:error, reason :: term}
  def open_read_write(_, name, _opts) do
    with {:ok, f} <- File.open(name, [:append, :binary, :read, {:read_ahead, 4096}]),
         :ok <- File.chmod(name, 0o666) do
      {:ok, %ExWal.File.Default{fd: f}}
    end
  end

  def open_dir(_, _name) do
    {:error, :not_supported}
  end

  @spec remove_all(impl :: ExWal.FS.t(), name :: String.t()) :: :ok | {:error, reason :: File.posix()}
  def remove_all(_, name) do
    :file.del_dir_r(name)
  end

  @spec rename(impl :: ExWal.FS.t(), old_name :: String.t(), new_name :: String.t()) ::
          :ok | {:error, reason :: File.posix()}
  def rename(_, old_name, new_name) do
    File.rename(old_name, new_name)
  end

  @spec reuse_for_write(impl :: ExWal.FS.t(), old_name :: String.t(), new_name :: String.t()) ::
          {:ok, file :: ExWal.File.t()} | {:error, reason :: term}
  def reuse_for_write(_, old_name, new_name) do
    with :ok <- File.rename(old_name, new_name),
         {:ok, f} <- File.open(new_name, [:write, :binary, :raw, {:read_ahead, 4096}]),
         :ok <- File.chmod(new_name, 0o666) do
      {:ok, %ExWal.File.Default{fd: f}}
    end
  end

  @spec mkdir_all(impl :: ExWal.FS.t(), name :: String.t()) :: :ok | {:error, reason :: File.posix()}
  def mkdir_all(_, name) do
    File.mkdir_p(name)
  end

  @spec lock(impl :: ExWal.FS.t(), name :: String.t()) :: boolean()
  def lock(_, name) do
    :global.set_lock({{__MODULE__, name}, self()}, [node()], 0)
  end

  @spec list(impl :: ExWal.FS.t(), name :: String.t()) :: {:ok, [binary()]} | {:error, reason :: File.posix()}
  def list(_, name) do
    name
    |> File.ls()
    |> case do
      {:ok, files} -> {:ok, files}
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec stat(impl :: ExWal.FS.t(), name :: String.t()) :: {:ok, File.Stat.t()} | {:error, reason :: File.posix()}
  def stat(_, name) do
    File.stat(name)
  end
end
