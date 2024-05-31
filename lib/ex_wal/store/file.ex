defmodule ExWal.Store.File do
  @moduledoc """
  Implements the `ExWal.Store` behaviour for files.
  """
  @behaviour ExWal.Store

  def open(_, path, opts \\ []) do
    mode = Keyword.get(opts, :mode, 0o640)

    path
    |> :file.open([:append, :raw, :binary])
    |> case do
      {:ok, io} ->
        :ok = :file.change_mode(path, mode)
        {:ok, io}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def read_all(_, path), do: File.read(path)

  def write_all(_, path, binary), do: File.write(path, binary)

  def append(_, handler, data), do: :file.write(handler, data)

  def sync(_, handler), do: :file.datasync(handler)

  def close(_, handler), do: :file.close(handler)

  def mkdir(_, path, opts \\ []) do
    mode = Keyword.get(opts, :mode, 0o750)
    File.mkdir_p!(path)
    File.chmod!(path, mode)
  end

  def rename(_, source, destionation), do: File.rename(source, destionation)

  def rm(_, path), do: File.rm(path)

  def wildcard(_, path) do
    Path.wildcard(path)
  end

  def dir?(_, path) do
    File.dir?(path)
  end
end
