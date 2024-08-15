defprotocol ExWal.LogWriter do
  @spec write_record(impl :: t(), bytes :: binary()) ::
          {:ok, written_bytes :: non_neg_integer()} | {:error, reason :: term()}
  def write_record(impl, bytes)

  @spec stop(impl :: t()) :: :ok
  def stop(impl)
end

defmodule ExWal.LogWriter.Single do
  @moduledoc """
  log writer writes records to a WAL file.
  """

  use GenServer, restart: :transient

  alias ExWal.Constant.Record
  alias ExWal.Models
  alias ExWal.Models.Block

  require ExWal.Constant.Record
  require Logger

  @recyclable_full_chunk_type Record.recyclable_full_chunk_type()
  @recyclable_first_chunk_type Record.recyclable_first_chunk_type()
  @recyclable_middle_chunk_type Record.recyclable_middle_chunk_type()
  @recyclable_last_chunk_type Record.recyclable_last_chunk_type()

  @block_size 32 * 1024
  @legacy_header_size 7
  @recyclable_header_size @legacy_header_size + 4

  @type t :: %__MODULE__{
          name: GenServer.name(),
          log_num: non_neg_integer(),
          file: ExWal.File.t(),
          block: Block.t(),
          block_num: non_neg_integer(),
          flush?: boolean(),
          pending: [Block.t()],
          sync_f: GenServer.from()
        }

  defstruct name: nil, log_num: nil, file: nil, block: nil, block_num: 0, flush?: false, pending: [], sync_f: nil

  def start_link({name, file, log_num}) do
    GenServer.start_link(__MODULE__, {name, file, log_num}, name: name)
  end

  def get(name), do: GenServer.call(name, :self)

  def stop(name) do
    GenServer.stop(name)
  end

  def write_record(name, bytes) do
    GenServer.call(name, {:sync_write, bytes})
  end

  # ------------ server ------------
  @impl GenServer
  def init({name, file, log_num}) do
    {:ok, %__MODULE__{name: name, log_num: log_num, file: file, block: %Block{}}}
  end

  @impl GenServer

  def terminate(reason, %__MODULE__{name: name} = state) do
    Logger.warning("log writer #{inspect(name)} is terminated with reason: #{inspect(reason)}")
    response_on_closing(state)
  end

  @impl GenServer
  def handle_continue(:flush, %__MODULE__{flush?: false} = state), do: {:noreply, state}

  def handle_continue(:flush, %__MODULE__{flush?: true, pending: []} = state) do
    %__MODULE__{file: file, block: block} = state

    :ok = ExWal.File.write(file, Block.flushable(block))

    may_response(state)
    {:noreply, %__MODULE__{state | block: Block.flush_to_written(block), flush?: false}}
  end

  def handle_continue(:flush, %__MODULE__{flush?: true} = state) do
    %__MODULE__{file: file, pending: pending} = state

    pending
    |> Enum.reverse()
    |> Enum.each(fn block ->
      ExWal.File.write(file, Block.flushable(block))
    end)

    may_response(state)

    {:noreply, %__MODULE__{state | flush?: false, pending: []}}
  end

  @impl GenServer
  def handle_call({:sync_write, p}, from, state) do
    state = emit_fragment(state, 0, p)
    {:noreply, %__MODULE__{state | flush?: true, sync_f: from}, {:continue, :flush}}
  end

  def handle_call(:self, _, state), do: {:reply, state, state}

  # ------------ private methods ------------

  defp emit_fragment(state, n, p)
  defp emit_fragment(state, n, <<>>) when n > 0, do: state

  defp emit_fragment(state, n, p) do
    %__MODULE__{log_num: log_num, block: block} = state
    %Block{written: written} = block
    rest_room = @block_size - written - @recyclable_header_size

    type = recyclable_flag(rest_room >= byte_size(p), n == 0)
    payload = binary_slice(p, 0, rest_room)
    <<_::bytes-size(byte_size(payload)), rest_p::binary>> = p

    buf =
      type
      |> Models.RecyclableRecord.new(
        log_num,
        payload
      )
      |> Models.RecyclableRecord.encode()

    block = Block.append(block, buf)

    state
    |> may_queue_block(block, @block_size - written < @recyclable_header_size)
    |> emit_fragment(n + 1, rest_p)
  end

  defp may_queue_block(state, block, no_room?)

  defp may_queue_block(state, block, false) do
    %__MODULE__{state | block: block}
  end

  defp may_queue_block(state, block, true) do
    block = Block.fullfill(block, @block_size)
    %__MODULE__{block_num: block_num} = state

    %__MODULE__{
      state
      | block: %Block{},
        pending: block,
        flush?: true,
        block_num: block_num + 1
    }
  end

  defp recyclable_flag(last?, first?)
  defp recyclable_flag(true, true), do: @recyclable_full_chunk_type
  defp recyclable_flag(true, false), do: @recyclable_last_chunk_type
  defp recyclable_flag(false, true), do: @recyclable_first_chunk_type
  defp recyclable_flag(false, false), do: @recyclable_middle_chunk_type

  defp written_offset(%__MODULE__{block_num: block_num, block: %Block{written: written}}),
    do: block_num * @block_size + written

  defp may_response(%__MODULE__{sync_f: nil}), do: :ok

  defp may_response(m) do
    %__MODULE__{sync_f: from} = m
    GenServer.reply(from, {:ok, written_offset(m)})
  end

  defp response_on_closing(%__MODULE__{sync_f: nil}), do: :ok

  defp response_on_closing(m) do
    %__MODULE__{sync_f: from} = m
    GenServer.reply(from, {:error, :closed})
  end
end

defimpl ExWal.LogWriter, for: ExWal.LogWriter.Single do
  alias ExWal.LogWriter.Single

  def write_record(%Single{name: name}, bytes) do
    Single.write_record(name, bytes)
  end

  def stop(%Single{name: name}) do
    Single.stop(name)
  end
end
