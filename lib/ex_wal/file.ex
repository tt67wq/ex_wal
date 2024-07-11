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
