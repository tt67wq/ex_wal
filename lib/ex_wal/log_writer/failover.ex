defmodule ExWal.LogWriter.Failover.LogicalOffset do
  @moduledoc false
  defstruct offset: 0, latest_log_size: 0, estimated_offset?: false
end

defmodule ExWal.LogWriter.Failover do
  @moduledoc """
  FailoverWriter is the implementation of LogWriter in failover mode.
  """

  use GenServer, restart: :transient

  alias ExWal.FS
  alias ExWal.LogWriter
  alias ExWal.LogWriter.Failover.LogicalOffset
  alias ExWal.LogWriter.Single
  alias ExWal.Models
  alias ExWal.Obeserver

  require Logger

  @max_log_writer 10

  defstruct name: nil,
            registry: nil,
            fs: nil,
            dir: "",
            log_num: 0,
            writers: %{
              # index => single_writer
              s: %{},
              cnt: 0
            },
            q: [],
            observer: nil,
            logical_offset: %LogicalOffset{
              offset: 0,
              latest_log_size: 0,
              estimated_offset?: false
            }

  def start_link({name, registry, fs, dir, log_num, observer}) do
    GenServer.start_link(
      __MODULE__,
      {name, registry, fs, dir, log_num, observer},
      name: name
    )
  end

  def get(name), do: %__MODULE__{name: name}

  @spec write_record(name :: GenServer.name(), bytes :: binary()) ::
          {:ok, written_offset :: non_neg_integer()} | {:error, reason :: any()}
  def write_record(name, bytes) do
    GenServer.call(name, {:sync_writes, bytes})
  end

  @spec stop(name :: GenServer.name()) :: :ok | {:error, reason :: any()}
  def stop(name) do
    GenServer.stop(name)
  end

  @spec switch_dir(name :: GenServer.name(), new_dir :: binary()) :: :ok
  def switch_dir(name, new_dir), do: GenServer.call(name, {:switch_dir, new_dir})

  # ---------------- server -----------------

  def init({name, registry, fs, dir, log_num, observer}) do
    {
      :ok,
      %__MODULE__{
        name: name,
        registry: registry,
        fs: fs,
        dir: dir,
        log_num: log_num,
        observer: observer
      },
      {:continue, {:switch_dir, dir}}
    }
  end

  def terminate(reason, state) do
    may_log_reason(reason)
    close_writers(state)
  end

  def handle_continue({:switch_dir, dir}, state) do
    dir
    |> do_switch_dir(state)
    |> case do
      {:ok, state} ->
        {:noreply, state}

      {{:error, reason}, state} ->
        {:stop, reason, state}
    end
  end

  def handle_call({:switch_dir, dir}, _, state) do
    dir
    |> do_switch_dir(state)
    |> case do
      {:ok, state} ->
        {:reply, :ok, state}

      {{:error, reason}, state} ->
        {:stop, reason, state}
    end
  end

  def handle_call({:sync_writes, _} = input, _, %__MODULE__{writers: %{cnt: 0}} = state) do
    %__MODULE__{logical_offset: logical_offset, q: q} = state
    {:sync_writes, p} = input
    %{offset: offset} = logical_offset
    offset = offset + byte_size(p)

    {
      :reply,
      {:ok, offset},
      %__MODULE__{state | logical_offset: %LogicalOffset{logical_offset | offset: offset}, q: [p | q]}
    }
  end

  # estimate
  def handle_call(
        {:sync_writes, _} = input,
        _,
        %__MODULE__{logical_offset: %LogicalOffset{estimated_offset?: true}} = state
      ) do
    %__MODULE__{writers: writers, logical_offset: logical_offset, observer: ob} = state
    writer = get_latest_writer(writers)
    {:sync_writes, p} = input

    Obeserver.record_start(ob)

    writer
    |> LogWriter.write_record(p)
    |> case do
      {:ok, of} ->
        Obeserver.record_end(ob, nil)

        {
          :reply,
          {:ok, of},
          %__MODULE__{state | logical_offset: %LogicalOffset{logical_offset | offset: of}}
        }

      {:error, reason} ->
        Obeserver.record_end(ob, reason)

        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:sync_writes, _} = input, _, state) do
    %__MODULE__{writers: writers, logical_offset: logical_offset, observer: ob} = state
    writer = get_latest_writer(writers)
    {:sync_writes, p} = input
    %LogicalOffset{latest_log_size: ls, offset: offset} = logical_offset

    Obeserver.record_start(ob)

    writer
    |> LogWriter.write_record(p)
    |> case do
      {:ok, of} ->
        Obeserver.record_end(ob, nil)
        delta = of - ls

        {
          :reply,
          {:ok, logical_offset.offset},
          %__MODULE__{
            state
            | logical_offset: %LogicalOffset{
                logical_offset
                | latest_log_size: of,
                  offset: offset + delta,
                  estimated_offset?: true
              }
          }
        }

      {:error, reason} ->
        Obeserver.record_end(ob, reason)
        {:reply, {:error, reason}, state}
    end
  end

  # def handle_call({:get_log, _, state}) do
  #   %__MODULE__{
  #     log_num: log_num,
  #     writers: %{
  #       s: s
  #     }
  #   } = state

  #   s
  #   |> Enum.map(fn {idx, w} ->
  #     end)

  #   {:reply, {:ok, %Models.VirtualLog{log_num: log_num}}, state}
  # end

  def handle_info({_task, {:switch_dir_notify, {:ok, _} = notify}}, state) do
    {:ok, {new_dir, writer, writer_idx}} = notify
    %__MODULE__{writers: writers, q: q, dir: old_dir} = state
    may_write_buffer(writer, q)

    %{s: s, cnt: cnt} = writers
    writers = %{s: Map.put(s, writer_idx, writer), cnt: cnt + 1}
    new_dir = if writer_idx >= cnt, do: new_dir, else: old_dir

    {:noreply, %__MODULE__{state | dir: new_dir, writers: writers}}
  end

  def handle_info({_task, {:switch_dir_notify, {:error, _} = _notify}}, state) do
    # {:error, reason} = notify
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ----------------- private funcs -----------------

  defp do_switch_dir(new_dir, state)
  defp do_switch_dir(_, %__MODULE__{writers: %{cnt: @max_log_writer}} = state), do: {{:error, :normal}, state}

  defp do_switch_dir(new_dir, state) do
    %__MODULE__{
      fs: fs,
      log_num: log_num,
      registry: registry,
      writers: %{idx: idx}
    } = state

    log_name = Path.join(new_dir, Models.VirtualLog.filename(log_num, idx))
    writer_name = {:via, Registry, {registry, {:writer, log_num, idx}}}

    # create writer asynchonously
    Task.async(fn ->
      ret =
        fs
        |> FS.create(log_name)
        |> case do
          {:ok, file} ->
            {:ok, _} = Single.start_link({writer_name, file, log_num})
            {:ok, {new_dir, Single.get(writer_name), idx}}

          err ->
            err
        end

      {:switch_dir_notify, ret}
    end)

    {:ok, state}
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
    p
    |> Enum.reverse()
    |> IO.iodata_to_binary()
    |> then(fn x -> LogWriter.write_record(writer, x) end)
  end
end

defimpl ExWal.LogWriter, for: ExWal.LogWriter.Failover do
  alias ExWal.LogWriter.Failover

  def write_record(%Failover{name: name}, bytes) do
    Failover.write_record(name, bytes)
  end

  def stop(%Failover{name: name}) do
    Failover.stop(name)
  end
end
