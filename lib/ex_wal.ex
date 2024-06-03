defmodule ExWal do
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  @external_resource "README.md"

  defmacro __using__(opts) do
    quote do
      alias ExWal.Core
      alias ExWal.Models.Entry
      alias ExWal.Typespecs

      @type ok_t(ret) :: {:ok, ret}
      @type err_t() :: {:error, term()}

      def init(config) do
        {:ok, config}
      end

      defoverridable init: 1

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def start_link(config \\ []) do
        otp_app = unquote(opts[:otp_app])

        {:ok, cfg} =
          otp_app
          |> Application.get_env(__MODULE__, config)
          |> init()

        ExWal.Supervisor.start_link(__MODULE__, cfg)
      end

      defp delegate(method, args), do: apply(Core, method, [__MODULE__ | args])

      @doc """
      Stop WAL.

      ## Examples

          iex> :ok = stop()
      """
      @spec stop() :: :ok
      def stop() do
        delegate(:stop, [])
      end

      @doc """
      Write entries to WAL, the entries must be strictly consecutive and incremental,
      and the index of the first entry must be WAL's last_index + 1.

      ## Examples

          iex> last_index = last_index(:wal_name)
          iex> enties = last_index..(last_index+10) |> Enum.map(fn i -> Entry.new(i, "some data") end)
          iex> :ok = write(:wal_name, entries)
      """
      @spec write([Entry.t()], non_neg_integer()) :: :ok
      def write(entries, timeout \\ 5000) do
        delegate(:write, [entries, timeout])
      end

      @doc """
      Read entry content from WAL by index. if index is not found, return {:error, :index_not_found}

      ## Examples

          iex> {:ok, data} = read(:wal_name, 1)
      """
      @spec read(Typespecs.index()) :: {:ok, Entry.t()} | {:error, :index_not_found}
      def read(index, timeout \\ 5000) do
        delegate(:read, [index, timeout])
      end

      @doc """
      Get the last index of WAL
      """
      @spec last_index() :: Typespecs.index()
      def last_index, do: delegate(:last_index, [])

      @doc """
      Get the first index of WAL
      """
      @spec first_index() :: Typespecs.index()
      def first_index, do: delegate(:first_index, [])

      @doc """
      Get the segment count of WAL
      """
      @spec segment_count() :: non_neg_integer()
      def segment_count, do: delegate(:segment_count, [])

      @doc """
      Truncates the write-ahead log (WAL) after a specific index.

      ## Examples

          iex> truncate_after(:my_wal, 10)
          :ok

      ## Parameters

        * `index` - The index after which to truncate the WAL.
        * `timeout` (optional) - The timeout value in milliseconds (default: 5000).

      ## Returns

      - `:ok` - If the truncation is successful.
      - `{:error, :index_out_of_range}` - If the provided index is out of range.

      """
      @spec truncate_after(Typespecs.index(), non_neg_integer()) :: :ok | {:error, :index_out_of_range}
      def truncate_after(index, timeout \\ 5000) do
        delegate(:truncate_after, [index, timeout])
      end

      @doc """
      Truncates the write-ahead log (WAL) before a specific index.

      ## Examples

          iex> truncate_before(:my_wal, 10)
          :ok

      ## Parameters

        * `index` - The index before which to truncate the WAL.
        * `timeout` (optional) - The timeout value in milliseconds (default: 5000).

      ## Returns

      - `:ok` - If the truncation is successful.
      - `{:error, :index_out_of_range}` - If the provided index is out of range.

      """
      @spec truncate_before(Typespecs.index(), non_neg_integer()) :: :ok | {:error, :index_out_of_range}
      def truncate_before(index, timeout \\ 5000) do
        delegate(:truncate_before, [index, timeout])
      end

      @doc """

      Synchronize the write-ahead log (WAL) to disk.

      ## Examples

          iex> sync(:my_wal)
          :ok

      ## Returns

      - `:ok` - If the synchronization is successful.
      - `{:error, any()}` - If the synchronization fails.

      """
      @spec sync() :: :ok | {:error, any()}
      def sync do
        delegate(:sync, [])
      end

      @doc """
      Clears the write-ahead log (WAL) by removing all entries.

      ## Examples

          iex> clear(:my_wal)
          :ok

      ## Returns

      - `:ok` - If the clearing is successful.

      """
      @spec clear() :: :ok
      def clear, do: delegate(:clear, [])

      @doc """
      Reinit the write-ahead log (WAL)

      ## Examples

          iex> reinit(:my_wal)
          :ok

      ## Returns

      - `:ok` - If the clearing is successful.

      """
      @spec reinit() :: :ok
      def reinit, do: delegate(:reinit, [])
    end
  end
end
