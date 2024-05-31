defmodule ExWal.Store do
  @moduledoc """
  storage behavior
  """
  alias ExWal.Typespecs

  @type t :: term()
  @type path :: String.t()

  @callback open(t(), path(), Typespecs.opts()) :: {:ok, Typespecs.handler()} | {:error, term()}
  @callback read_all(t(), path()) :: {:ok, binary()} | {:error, term()}
  @callback write_all(t(), path(), binary()) :: :ok | {:error, term()}
  @callback append(t(), Typespecs.handler(), binary()) :: :ok | {:error, term()}
  @callback sync(t(), Typespecs.handler()) :: :ok | {:error, term()}
  @callback close(t(), Typespecs.handler()) :: :ok | {:error, term()}
  @callback mkdir(t(), path(), Typespecs.opts()) :: :ok
  @callback rename(t(), path(), path()) :: :ok | {:error, term()}
  @callback rm(t(), path()) :: :ok | {:error, term()}
  @callback wildcard(t(), path()) :: [path()]
  @callback dir?(t(), path()) :: boolean()

  defp do_apply({module, name}, method, args), do: apply(module, method, [name | args])

  def open(mm, path, opts \\ []), do: do_apply(mm, :open, [path, opts])
  def read_all(mm, path), do: do_apply(mm, :read_all, [path])
  def write_all(mm, path, data), do: do_apply(mm, :write_all, [path, data])
  def append(mm, handler, data), do: do_apply(mm, :append, [handler, data])
  def sync(mm, handler), do: do_apply(mm, :sync, [handler])
  def close(mm, handler), do: do_apply(mm, :close, [handler])
  def mkdir(mm, path, opts \\ []), do: do_apply(mm, :mkdir, [path, opts])
  def rename(mm, old_path, new_path), do: do_apply(mm, :rename, [old_path, new_path])
  def rm(mm, path), do: do_apply(mm, :rm, [path])
  def wildcard(mm, path), do: do_apply(mm, :wildcard, [path])
  def dir?(mm, path), do: do_apply(mm, :dir?, [path])
end
