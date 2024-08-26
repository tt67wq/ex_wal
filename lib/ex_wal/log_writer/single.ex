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
          pending: %{non_neg_integer() => Block.t()}
        }

  defstruct name: nil, log_num: nil, file: nil, block: nil, block_num: 0, pending: %{}

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
    {:ok, %__MODULE__{name: name, log_num: log_num, file: file, block: Block.new()}}
  end

  @impl GenServer

  def terminate(reason, %__MODULE__{name: name}) do
    Logger.warning("log writer #{inspect(name)} is terminated with reason: #{inspect(reason)}")
  end

  @impl GenServer
  def handle_call({:sync_write, p}, _from, state) do
    state = emit_fragment(state, 0, p)
    flush(state)
    {:reply, {:ok, written_offset(state)}, state}
  end

  def handle_call(:self, _, state), do: {:reply, state, state}

  @impl GenServer
  def handle_info({_task, {:flush_task, ret}}, state) do
    {:ok, flushed_pending_nums, {hot_block, hot_block_num}} = ret
    %__MODULE__{pending: pending, block: block, block_num: block_num} = state

    new_pending = Map.drop(pending, flushed_pending_nums)

    state =
      if hot_block_num == block_num do
        # still hot
        block = Block.flush_to(block, hot_block.written)
        %__MODULE__{state | block: block, pending: new_pending}
      else
        # already in pending
        b =
          new_pending
          |> Map.fetch!(hot_block_num)
          |> Block.flush_to(hot_block.written)

        %__MODULE__{state | pending: Map.put(new_pending, hot_block_num, b)}
      end

    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ------------ private methods ------------

  defp flush(state) do
    %__MODULE__{block: block, block_num: block_num, pending: pending, file: file} = state

    ns =
      pending
      |> Map.keys()
      |> Enum.sort(:desc)

    to_flush =
      Enum.map(ns, fn n -> Map.fetch!(pending, n) end)

    flush_content =
      [Block.flushable(block) | to_flush]
      |> Enum.reverse()
      |> IO.iodata_to_binary()

    Task.async(fn ->
      :ok = ExWal.File.write(file, flush_content)
      {:flush_task, {:ok, ns, {block, block_num}}}
    end)
  end

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
    %__MODULE__{block_num: block_num, pending: pending} = state

    %__MODULE__{
      state
      | block: Block.new(),
        pending: Map.put_new(pending, block_num, block),
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
