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
            writers: [],
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

  def handle_call({:sync_writes, _} = input, _, %__MODULE__{writers: []} = state) do
    %__MODULE__{logical_offset: logical_offset} = state
    {:sync_writes, p} = input
    %{offset: offset} = logical_offset
    offset = offset + byte_size(p)
    {:reply, {:ok, offset}, %__MODULE__{state | logical_offset: %{logical_offset | offset: offset}}}
  end

  # estimate
  def handle_call({:sync_writes, _} = input, _, %__MODULE__{logical_offset: %{estimated_offset?: true}} = state) do
    %__MODULE__{writers: [writer | _], logical_offset: logical_offset} = state
    {:sync_writes, p} = input

    {:ok, of} = LogWriter.write_record(writer, p)
    logical_offset = %{logical_offset | offset: of, latest_writer: writer}
    {:reply, {:ok, of}, %__MODULE__{state | logical_offset: logical_offset}}
  end

  def handle_call({:sync_writes, _} = input, _, state) do
    %__MODULE__{writers: [writer | _], logical_offset: logical_offset} = state
    {:sync_writes, p} = input
    %{latest_log_size: ls, offset: offset} = logical_offset

    {:ok, of} = LogWriter.write_record(writer, p)
    delta = of - ls
    logical_offset = %{logical_offset | latest_log_size: of, latest_writer: writer, offset: offset + delta}
    {:reply, {:ok, logical_offset.offset}, %__MODULE__{state | logical_offset: logical_offset}}
  end

  def handle_info({_task, {:switch_dir_notify, {:ok, new_dir, writer_name}}}, state) do
    %__MODULE__{writers: writers} = state
    {:noreply, %__MODULE__{state | dir: new_dir, writers: [writer_name | writers]}}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ----------------- private funcs -----------------

  defp switch_dir(new_dir, state)
  defp switch_dir(_, %__MODULE__{writers: {_, @max_log_writer}} = state), do: {:stop, :normal, state}

  defp switch_dir(new_dir, %__MODULE__{writers: {_writers, idx}} = state) do
    %__MODULE__{fs: fs, log_num: log_num, registry: registry} = state
    # create writer asynchonously
    log_name = Path.join(new_dir, Models.VirtualLog.filename(log_num, idx))
    writer_name = {:via, Registry, {registry, {:writer, log_num, idx}}}

    Task.async(fn ->
      {:ok, file} = FS.create(fs, log_name)
      {:ok, _} = Single.start_link({writer_name, file, log_num})
      {:switch_dir_notify, {:ok, new_dir, writer_name}}
    end)

    {:noreply, state}
  end

  defp close_writers(state) do
    %__MODULE__{writers: {writers, _}} = state
    Enum.each(writers, &Single.stop/1)
  end

  defp may_log_reason(:normal), do: :pass
  defp may_log_reason(reason), do: Logger.error("FailoverWriter terminate: #{inspect(reason)}")
end
