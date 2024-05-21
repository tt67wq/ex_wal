defmodule ExWal.Store do
  @moduledoc """
  storage behavior
  """
  alias ExWal.Typespecs

  @type t :: Typespecs.name()
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

  defmacro __using__(_) do
    quote do
      @before_compile ExWal.Store

      Module.register_attribute(__MODULE__, :store_impl, [])
      Module.register_attribute(__MODULE__, :store_name, [])
    end
  end

  defmacro __before_compile__(env) do
    impl = Module.get_attribute(env.module, :store_impl)
    name = Module.get_attribute(env.module, :store_name)
    [gen_funcs(impl, name)]
  end

  defp gen_funcs(impl, name) do
    quote do
      def open(path, opts \\ []), do: apply(unquote(impl), :open, [unquote(name), path, opts])
      def read_all(path), do: apply(unquote(impl), :read_all, [unquote(name), path])
      def write_all(path, data), do: apply(unquote(impl), :write_all, [unquote(name), path, data])
      def append(handler, data), do: apply(unquote(impl), :append, [unquote(name), handler, data])
      def sync(handler), do: apply(unquote(impl), :sync, [unquote(name), handler])
      def close(handler), do: apply(unquote(impl), :close, [unquote(name), handler])
      def mkdir(path, opts \\ []), do: apply(unquote(impl), :mkdir, [unquote(name), path, opts])
      def rename(old_path, new_path), do: apply(unquote(impl), :rename, [unquote(name), old_path, new_path])
      def rm(path), do: apply(unquote(impl), :rm, [unquote(name), path])
    end
  end
end
