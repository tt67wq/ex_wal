defmodule ExWal.Store do
  @moduledoc """
  storage behavior
  """

  @type t :: struct()
  @type path :: String.t()
  @type handler :: pid() | :file.fd()
  @type permision :: non_neg_integer()
  @type mode :: [:read | :write | :binary | :append | :directory]

  @callback new(opts :: keyword()) :: t()
  @callback open(t(), path(), mode(), permision()) :: {:ok, handler()} | {:error, term()}
  @callback read_all(t(), path()) :: {:ok, binary()} | {:error, term()}
  @callback write_all(t(), path(), binary()) :: :ok | {:error, term()}
  @callback append(t(), handler(), binary()) :: :ok | {:error, term()}
  @callback sync(t(), handler()) :: :ok | {:error, term()}
  @callback close(t(), handler()) :: :ok | {:error, term()}
  @callback mkdir(t(), path(), permision()) :: :ok
  @callback rename(t(), path(), path()) :: :ok | {:error, term()}
  @callback rm(t(), path()) :: :ok | {:error, term()}

  @spec open(t(), path(), mode(), permision()) :: {:ok, handler()} | {:error, term()}
  def open(store, path, mode, permission), do: delegate(store, :open, [path, mode, permission])

  @spec read_all(t(), path()) :: {:ok, binary()} | {:error, term()}
  def read_all(store, path), do: delegate(store, :read_all, [path])

  @spec write_all(t(), path(), binary()) :: :ok | {:error, term()}
  def write_all(store, path, data), do: delegate(store, :write_all, [path, data])

  @spec append(t(), handler(), binary()) :: :ok | {:error, term()}
  def append(store, handler, data), do: delegate(store, :append, [handler, data])

  @spec sync(t(), handler()) :: :ok | {:error, term()}
  def sync(store, handler), do: delegate(store, :sync, [handler])

  @spec close(t(), handler()) :: :ok | {:error, term()}
  def close(store, handler), do: delegate(store, :close, [handler])

  @spec mkdir(t(), path(), permision()) :: :ok
  def mkdir(store, path, permission), do: delegate(store, :mkdir, [path, permission])

  @spec rename(t(), path(), path()) :: :ok | {:error, term()}
  def rename(store, source, destionation), do: delegate(store, :rename, [source, destionation])

  @spec rm(t(), path()) :: :ok | {:error, term()}
  def rm(store, path), do: delegate(store, :rm, [path])

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])
end

defmodule ExWal.Store.File do
  @moduledoc """
  file storage
  """

  @behaviour ExWal.Store

  defstruct []

  @type t :: %__MODULE__{}

  def new(_opts), do: %__MODULE__{}

  def open(_store, path, mode, permission) do
    path
    |> File.open(mode)
    |> case do
      {:ok, io} ->
        File.chmod!(path, permission)
        {:ok, io}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def read_all(_, path), do: File.read(path)

  def write_all(_, path, binary), do: File.write(path, binary)

  def append(_, handler, data), do: :file.write(handler, data)

  def sync(_, handler), do: :file.sync(handler)

  def close(_, handler), do: :file.close(handler)

  def mkdir(_, path, permision) do
    File.mkdir_p!(path)
    File.chmod!(path, permision)
  end

  def rename(_, source, destionation), do: File.rename(source, destionation)

  def rm(_, path), do: File.rm(path)
end
