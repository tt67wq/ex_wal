defmodule ExWal.LogWriter.Failover do
  @moduledoc """
  FailoverWriter is the implementation of LogWriter in failover mode.
  """

  use GenServer, restart: :transient

  alias ExWal.FS
  alias ExWal.LogWriter
  alias ExWal.LogWriter.Single
  alias ExWal.Models

  require Logger

  @max_log_writer 10

  defstruct name: nil,
            registry: nil,
            fs: nil,
            dir: "",
            log_num: 0,
            writers: %{
              s: %{},
              cnt: 0
            },
            q: [],
            logical_offset: %{
              offset: 0,
              latest_writer: nil,
              latest_log_size: 0,
              estimated_offset?: false
            }

  def start_link({name, registry, fs, dir, log_num}) do
    GenServer.start_link(
      __MODULE__,
      {name, registry, fs, dir, log_num},
      name: name
    )
  end

  @spec stop(name :: GenServer.name()) :: :ok | {:error, reason :: any()}
  def stop(name) do
    GenServer.stop(name)
  end

  # ---------------- server -----------------

  def init({name, registry, fs, dir, log_num}) do
    {
      :ok,
      %__MODULE__{
        name: name,
        registry: registry,
        fs: fs,
        dir: dir,
        log_num: log_num
      },
      {:continue, {:switch_dir, dir}}
    }
  end

  def terminate(reason, state) do
    may_log_reason(reason)
    close_writers(state)
  end

  def handle_continue({:switch_dir, dir}, state) do
    switch_dir(dir, state)
  end

  def handle_call({:sync_writes, _} = input, _, %__MODULE__{writers: %{cnt: 0}} = state) do
    %__MODULE__{logical_offset: logical_offset, q: q} = state
    {:sync_writes, p} = input
    %{offset: offset} = logical_offset
    offset = offset + byte_size(p)

    {:reply, {:ok, offset}, %__MODULE__{state | logical_offset: %{logical_offset | offset: offset}, q: [p | q]}}
  end

  # estimate
  def handle_call({:sync_writes, _} = input, _, %__MODULE__{logical_offset: %{estimated_offset?: true}} = state) do
    %__MODULE__{writers: writers, logical_offset: logical_offset} = state
    writer = get_latest_writer(writers)
    {:sync_writes, p} = input

    {:ok, of} = LogWriter.write_record(writer, p)
    logical_offset = %{logical_offset | offset: of, latest_writer: writer}
    {:reply, {:ok, of}, %__MODULE__{state | logical_offset: logical_offset}}
  end

  def handle_call({:sync_writes, _} = input, _, state) do
    %__MODULE__{writers: writers, logical_offset: logical_offset} = state
    writer = get_latest_writer(writers)
    {:sync_writes, p} = input
    %{latest_log_size: ls, offset: offset} = logical_offset

    {:ok, of} = LogWriter.write_record(writer, p)
    delta = of - ls
    logical_offset = %{logical_offset | latest_log_size: of, latest_writer: writer, offset: offset + delta}
    {:reply, {:ok, logical_offset.offset}, %__MODULE__{state | logical_offset: logical_offset}}
  end

  def handle_info({_task, {:switch_dir_notify, notify}}, state) do
    {:ok, new_dir, writer, writer_idx} = notify
    %__MODULE__{writers: writers, q: q} = state
    may_write_buffer(writer, q)
    %{s: s, cnt: cnt} = writers
    writers = %{s: Map.put(s, writer_idx, writer), cnt: cnt + 1}
    {:noreply, %__MODULE__{state | dir: new_dir, writers: writers}}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ----------------- private funcs -----------------

  defp switch_dir(new_dir, state)
  defp switch_dir(_, %__MODULE__{writers: %{cnt: @max_log_writer}} = state), do: {:stop, :normal, state}

  defp switch_dir(new_dir, state) do
    %__MODULE__{fs: fs, log_num: log_num, registry: registry, writers: %{idx: idx}} = state
    # create writer asynchonously
    log_name = Path.join(new_dir, Models.VirtualLog.filename(log_num, idx))
    writer_name = {:via, Registry, {registry, {:writer, log_num, idx}}}

    Task.async(fn ->
      {:ok, file} = FS.create(fs, log_name)
      {:ok, _} = Single.start_link({writer_name, file, log_num})
      {:switch_dir_notify, {:ok, new_dir, Single.get(writer_name), idx}}
    end)

    {:noreply, state}
  end

  defp close_writers(state) do
    %__MODULE__{writers: {writers, _}} = state
    Enum.each(writers, &Single.stop/1)
  end

  defp may_log_reason(:normal), do: :pass
  defp may_log_reason(reason), do: Logger.error("FailoverWriter terminate: #{inspect(reason)}")

  defp get_latest_writer(writers)
  defp get_latest_writer(%{cnt: 0}), do: nil

  defp get_latest_writer(writers) do
    %{s: s, cnt: cnt} = writers
    Map.get(s, cnt - 1)
  end

  defp may_write_buffer(writer, p)
  defp may_write_buffer(_writer, []), do: :pass

  defp may_write_buffer(writer, p) do
    LogWriter.write_record(writer, p)
  end
end
