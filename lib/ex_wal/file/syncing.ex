defmodule ExWal.File.Syncing do
  @moduledoc false

  use Agent, restart: :transient

  @type t :: %__MODULE__{
          name: String.t(),
          file: ExWal.File.t(),
          offset: non_neg_integer,
          sync_offset: non_neg_integer
        }

  defstruct name: nil, file: nil, offset: 0, sync_offset: 0

  # 1MB
  @sync_range_buffer Bitwise.bsl(1, 20)
  # 4KB
  @sync_range_alignment Bitwise.bsl(4, 10)
  # 4k
  @bytes_per_sync Bitwise.bsl(4, 10)

  def start_link({name, file}) do
    Agent.start_link(__MODULE__, :init, [name, file], name: name)
  end

  @spec get(Agent.name()) :: t()
  def get(name), do: Agent.get(name, fn state -> state end)

  def close(name) do
    Agent.get(name, __MODULE__, :handle_close, [])
    Agent.stop(name, :shutdown)
  end

  def write(name, bytes), do: Agent.get_and_update(name, __MODULE__, :handle_write, [bytes])

  # ---------------- handler ------------

  def init(name, file) do
    %__MODULE__{name: name, file: file}
  end

  def handle_write(%__MODULE__{file: file, offset: offset} = state, bytes) do
    file
    |> ExWal.File.write(bytes)
    |> case do
      :ok ->
        state = %__MODULE__{state | offset: offset + byte_size(bytes)}
        {:ok, may_sync(state)}

      {:error, _reason} = err ->
        {err, state}
    end
  end

  def handle_close(%__MODULE__{file: file}) do
    ExWal.File.sync(file)
    ExWal.File.close(file)
  end

  defp may_sync(state)
  # From the RocksDB source:
  #
  #   We try to avoid sync to the last 1MB of data. For two reasons:
  #   (1) avoid rewrite the same page that is modified later.
  #   (2) for older version of OS, write can block while writing out
  #       the page.
  #   Xfs does neighbor page flushing outside of the specified ranges. We
  #   need to make sure sync range is far from the write offset.
  defp may_sync(%__MODULE__{offset: offset} = state) when offset < @sync_range_buffer, do: state

  defp may_sync(%__MODULE__{offset: offset} = state) do
    %__MODULE__{file: file} = state
    sync_to_offset = offset - @sync_range_buffer
    sync_to_offset = sync_to_offset - rem(sync_to_offset, @sync_range_alignment)

    if syncable?(sync_to_offset, state) do
      {:ok, full_sync?} = ExWal.File.sync_to(file, sync_to_offset)
      set_offset(state, (full_sync? && offset) || sync_to_offset)
    else
      state
    end
  end

  defp syncable?(0, _state), do: false

  defp syncable?(sync_to_offset, state) do
    %__MODULE__{sync_offset: sync_offset} = state
    sync_to_offset - sync_offset >= @bytes_per_sync
  end

  defp set_offset(state, offset)
  defp set_offset(%__MODULE__{sync_offset: sync_offset} = state, offset) when sync_offset >= offset, do: state

  defp set_offset(state, offset) do
    %__MODULE__{state | sync_offset: offset}
  end
end

defimpl ExWal.File, for: ExWal.File.Syncing do
  alias ExWal.File.Syncing

  def close(%Syncing{name: name}), do: Syncing.close(name)

  def read(%Syncing{file: file}, n_bytes), do: ExWal.File.read(file, n_bytes)

  def read_at(%Syncing{file: file}, n_bytes, offset), do: ExWal.File.read_at(file, n_bytes, offset)

  def write(%Syncing{name: name}, bytes), do: Syncing.write(name, bytes)

  def write_at(%Syncing{file: file}, data, offset), do: ExWal.File.write_at(file, data, offset)

  def prealloc(%Syncing{file: file}, offset, length), do: ExWal.File.prealloc(file, offset, length)

  def stat(%Syncing{file: file}), do: ExWal.File.stat(file)

  def sync(%Syncing{file: file}), do: ExWal.File.sync(file)

  def sync_to(%Syncing{file: file}, length), do: ExWal.File.sync_to(file, length)

  def prefetch(%Syncing{file: file}, offset, length), do: ExWal.File.prefetch(file, offset, length)

  def fd(%Syncing{file: file}), do: ExWal.File.fd(file)
end
