defmodule ExWal.LRU do
  @moduledoc ~S"""
  This modules implements a simple LRU cache, using 2 ets tables for it.

  For using it, you need to start it:

      iex> LRU.start_link({:my_cache, 1000})

  Or add it to your supervisor tree, like: `worker(LRU, [:my_cache, 1000])`

  ## Using

      iex> LRU.start_link({:my_cache, 1000})
      {:ok, #PID<0.60.0>}

      iex> LRU.put(:my_cache, "id", "value")
      :ok

      iex> LRU.get(:my_cache, "id", touch = false)
      "value"

  To take some action when old keys are evicted from the cache when it is full,
  you can pass an `:evict_fn` option to `LRU.start_link/3`. This is
  helpful for cleaning up processes that depend on values in the cache, or
  logging, or instrumentation of cache evictions etc.

      iex> evict = fn(key,value) -> IO.inspect("#{key}=#{value} evicted") end
      iex> LRU.start_link(:my_cache, 10, evict_fn: evict)
      {:ok, #PID<0.60.0>}

  ## Design

  First ets table save the key values pairs, the second save order of inserted elements.
  """

  use Agent

  # use GenServer

  defstruct table: nil, ttl_table: nil, size: 0, evict_fn: nil

  @type t :: %__MODULE__{
          table: atom(),
          ttl_table: atom(),
          size: non_neg_integer(),
          evict_fn: nil | (atom(), atom() -> any())
        }

  @doc """
  Creates an LRU of the given size as part of a supervision tree with a registered name

  ## Options

    * `:evict_fn` - function that accepts (key, value) and takes some action when keys are
      evicted when the cache is full.

  """
  @spec start_link({atom(), non_neg_integer(), Keyword.t()}) :: Agent.on_start()
  def start_link({name, size, opts}) do
    Agent.start_link(__MODULE__, :init, [name, size, opts], name: name)
  end

  @spec stop(atom() | pid()) :: :ok
  def stop(name_or_pid) do
    Agent.stop(name_or_pid)
  end

  @doc """
  Stores the given `value` under `key` in `cache`. If `cache` already has `key`, the stored
  `value` is replaced by the new one. This updates the order of LRU cache.
  """
  @spec put(atom(), any(), any(), non_neg_integer()) :: :ok
  def put(name, key, value, timeout \\ 5000), do: Agent.get(name, __MODULE__, :handle_put, [key, value], timeout)

  @doc """
  Updates a `value` in `cache`. If `key` is not present in `cache` then nothing is done.
  `touch` defines, if the order in LRU should be actualized. The function assumes, that
  the element exists in a cache.
  """
  @spec update(atom(), any(), any(), boolean(), non_neg_integer()) :: :ok
  def update(name, key, value, touch \\ true, timeout \\ 5000)

  def update(name, key, value, touch, timeout) do
    if :ets.update_element(name, key, {3, value}) do
      touch && Agent.get(name, __MODULE__, :handle_touch, [key], timeout)
    end

    :ok
  end

  @doc """
  Returns the `value` associated with `key` in `cache`. If `cache` does not contain `key`,
  returns nil. `touch` defines, if the order in LRU should be actualized.
  """
  @spec get(atom(), any(), boolean(), non_neg_integer()) :: any()
  def get(name, key, touch \\ true, timeout \\ 5000)

  def get(name, key, touch, timeout) do
    case :ets.lookup(name, key) do
      [{_, _, value}] ->
        touch && Agent.get(name, __MODULE__, :handle_touch, [key], timeout)
        value

      [] ->
        nil
    end
  end

  @doc """
  Selects and returns the first value in the cache that matches the given condition.

  ## Examples

      iex> LRU.select(:my_cache, fn value -> value > 10 end)
      15

  ### Parameters

  - `name` - The name of the cache.
  - `match_fun` - A function that takes a value and returns a boolean indicating whether it matches the condition.
  - `timeout` (optional) - The timeout value in milliseconds for the operation. Defaults to 5000.

  ### Returns

  The first value in the cache that matches the given condition, or `nil` if no match is found.

  """
  @spec select(atom(), (any() -> boolean()), non_neg_integer()) :: any()
  def select(name, match_fun, timeout \\ 5000) do
    name
    |> :ets.first()
    |> select(name, match_fun, timeout)
  end

  defp select(:"$end_of_table", _, _, _), do: nil

  defp select(key, name, match_fun, timeout) do
    [{_, _, value}] = :ets.lookup(name, key)

    if match_fun.(value) do
      Agent.get(name, __MODULE__, :handle_touch, [key], timeout)
      value
    else
      next_k = :ets.next(name, key)
      select(next_k, name, match_fun, timeout)
    end
  end

  @doc """
  Removes the entry stored under the given `key` from cache.
  """
  @spec delete(atom(), any(), non_neg_integer()) :: :ok
  def delete(name, key, timeout \\ 5000), do: Agent.get(name, __MODULE__, :handle_delete, [key], timeout)

  @doc """
  Removes all entries from cache.
  """
  @spec clear(atom(), non_neg_integer()) :: :ok
  def clear(name, timeout \\ 5000), do: Agent.get(name, __MODULE__, :handle_clear, [], timeout)

  @doc false
  @spec init(atom(), non_neg_integer(), Keyword.t()) :: t()
  def init(name, size, opts \\ []) do
    ttl_table = :"#{name}_ttl"
    :ets.new(ttl_table, [:named_table, :ordered_set])
    :ets.new(name, [:named_table, :public, {:read_concurrency, true}])
    evict_fn = Keyword.get(opts, :evict_fn)
    %__MODULE__{ttl_table: ttl_table, table: name, size: size, evict_fn: evict_fn}
  end

  @doc false
  def handle_put(%__MODULE__{table: table} = state, key, value) do
    delete_ttl(state, key)
    uniq = insert_ttl(state, key)
    :ets.insert(table, {key, uniq, value})
    clean_oversize(state)
    :ok
  end

  @doc false
  def handle_touch(%__MODULE__{table: table} = state, key) do
    delete_ttl(state, key)
    uniq = insert_ttl(state, key)
    :ets.update_element(table, key, [{2, uniq}])
    :ok
  end

  @doc false
  def handle_delete(%{table: table} = state, key) do
    delete_ttl(state, key)
    :ets.delete(table, key)
    :ok
  end

  def handle_clear(%__MODULE__{table: table, ttl_table: ttl_table}) do
    :ets.delete_all_objects(table)
    :ets.delete_all_objects(ttl_table)
    :ok
  end

  defp delete_ttl(%__MODULE__{ttl_table: ttl_table, table: table}, key) do
    case :ets.lookup(table, key) do
      [{_, old_uniq, _}] ->
        :ets.delete(ttl_table, old_uniq)

      _ ->
        nil
    end
  end

  defp insert_ttl(%__MODULE__{ttl_table: ttl_table}, key) do
    uniq = :erlang.unique_integer([:monotonic])
    :ets.insert(ttl_table, {uniq, key})
    uniq
  end

  defp clean_oversize(%__MODULE__{ttl_table: ttl_table, table: table, size: size} = state) do
    if :ets.info(table, :size) > size do
      oldest_tstamp = :ets.first(ttl_table)
      [{_, old_key}] = :ets.lookup(ttl_table, oldest_tstamp)
      :ets.delete(ttl_table, oldest_tstamp)
      call_evict_fn(state, old_key)
      :ets.delete(table, old_key)
      true
    else
      false
    end
  end

  defp call_evict_fn(%__MODULE__{evict_fn: nil}, _old_key), do: nil

  defp call_evict_fn(%__MODULE__{evict_fn: evict_fn, table: table}, key) do
    [{_, _, value}] = :ets.lookup(table, key)
    evict_fn.(key, value)
  end
end
