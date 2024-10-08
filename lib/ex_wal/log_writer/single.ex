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

  @block_size Record.block_size()
  # 11
  @recyclable_header_size Record.recyclable_header_size()

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

  def write_record(name, bytes, opts \\ []) do
    GenServer.call(name, {:sync_write, bytes}, Keyword.get(opts, :timeout, 5000))
  end

  # ------------ server ------------
  @impl GenServer
  def init({name, file, log_num}) do
    {:ok, %__MODULE__{name: name, log_num: log_num, file: file, block: Block.new()}}
  end

  @impl GenServer

  def terminate(:normal, _state) do
    # state
    # |> emit_eof_trailer()
    # |> sync_flush(nil)
    :ok
  end

  def terminate(reason, _state) do
    Logger.error("Single writer terminate: #{inspect(reason)}")
  end

  @impl GenServer
  def handle_call({:sync_write, p}, from, state) do
    state =
      state
      |> emit_fragment(0, p)
      |> flush(from)

    {:noreply, state}
  end

  def handle_call(:self, _, state), do: {:reply, state, state}

  @impl GenServer
  def handle_info({_task, {:flush_task, ret}}, state) do
    {:ok, flushed_pending_nums, hot_block, hot_block_num, from} = ret
    %Models.Block{written: hw} = hot_block
    %__MODULE__{pending: pending, block: block, block_num: block_num} = state

    new_pending = Map.drop(pending, flushed_pending_nums)

    state =
      if hot_block_num == block_num do
        # still hot
        block = Block.flush_to(block, hw)
        %__MODULE__{state | block: block, pending: new_pending}
      else
        # already in pending
        b =
          new_pending
          |> Map.fetch!(hot_block_num)
          |> Block.flush_to(hw)

        %__MODULE__{state | pending: Map.put(new_pending, hot_block_num, b)}
      end

    from
    |> is_nil()
    |> unless do
      GenServer.reply(from, {:ok, written_offset(state)})
    end

    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ------------ private methods ------------

  defp flush(state, from) do
    %__MODULE__{block: block, block_num: block_num, pending: pending, file: file} = state

    ns =
      pending
      |> Map.keys()
      |> Enum.sort(:desc)

    to_flush =
      Enum.map(ns, fn n -> pending |> Map.fetch!(n) |> Block.flushable() end)

    flush_content =
      [Block.flushable(block) | to_flush]
      |> Enum.reverse()
      |> IO.iodata_to_binary()

    Task.async(fn ->
      :ok = ExWal.File.write(file, flush_content)
      {:flush_task, {:ok, ns, block, block_num, from}}
    end)

    %__MODULE__{state | block: Block.flush_all(block)}
  end

  defp emit_fragment(state, n, p)
  defp emit_fragment(state, n, <<>>) when n > 0, do: state

  defp emit_fragment(%__MODULE__{block: %Block{written: written}} = state, n, p)
       when @block_size - @recyclable_header_size - written > 0 do
    %__MODULE__{log_num: log_num, block: block} = state
    rest_room = @block_size - @recyclable_header_size - written

    type = recyclable_flag(rest_room >= byte_size(p), n == 0)
    payload = binary_slice(p, 0, rest_room)
    payload_size = byte_size(payload)
    <<_::bytes-size(payload_size), rest_p::binary>> = p

    buf =
      type
      |> Models.RecyclableRecord.new(
        log_num,
        payload
      )
      |> Models.RecyclableRecord.encode()

    block = Block.append(block, buf)

    state
    |> may_queue_block(block, false)
    |> emit_fragment(n + 1, rest_p)
  end

  defp emit_fragment(state, n, p) do
    %__MODULE__{block: block} = state

    state
    |> may_queue_block(block, true)
    |> emit_fragment(n + 1, p)
  end

  # defp emit_eof_trailer(state) do
  #   %__MODULE__{block: block, log_num: log_num} = state

  #   trailer = %Models.RecyclableRecord{
  #     crc: 0,
  #     size: 0,
  #     type: @recyclable_full_chunk_type,
  #     # copy from pebble, don't know why
  #     log_number: log_num + 1,
  #     payload: <<>>
  #   }

  #   Block.append(block, Models.RecyclableRecord.encode(trailer))

  #   %__MODULE__{state | block: block}
  # end

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

  def write_record(%Single{name: name}, bytes, opts) do
    Single.write_record(name, bytes, opts)
  end

  def stop(%Single{name: name}) do
    Single.stop(name)
  end
end
