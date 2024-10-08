defmodule ExWal.LogWriter.Failover.LogicalOffset do
  @moduledoc false
  defstruct offset: 0, latest_log_size: 0, no_estimated_offset?: false
end

defmodule ExWal.LogWriter.Failover.WriterAndRecorder do
  @moduledoc false
  defstruct w: nil, observer: nil, dir: "", fs: nil

  @type t :: %__MODULE__{
          w: ExWal.LogWriter.t(),
          observer: pid() | GenServer.name(),
          dir: String.t(),
          fs: ExWal.FS.t()
        }
end

defmodule ExWal.LogWriter.Failover do
  @moduledoc """
  FailoverWriter is the implementation of LogWriter in failover mode.
  """

  use GenServer, restart: :transient

  alias ExWal.FS
  alias ExWal.LogWriter
  alias ExWal.LogWriter.Failover.LogicalOffset
  alias ExWal.LogWriter.Failover.WriterAndRecorder
  alias ExWal.LogWriter.Single
  alias ExWal.Models
  alias ExWal.Observer

  require Logger

  @max_log_writer 10

  @type t :: %__MODULE__{
          name: GenServer.name(),
          registry: GenServer.name(),
          fs: ExWal.FS.t(),
          log_num: non_neg_integer(),
          writers: %{
            # index => WriterAndRecorder
            s: %{(index :: non_neg_integer()) => wr :: WriterAndRecorder.t()},
            cnt: non_neg_integer()
          },
          q: [iodata()],
          logical_offset: %LogicalOffset{
            offset: non_neg_integer(),
            latest_log_size: non_neg_integer(),
            no_estimated_offset?: boolean()
          },
          manager: pid() | GenServer.name()
        }

  defstruct name: nil,
            registry: nil,
            fs: nil,
            log_num: 0,
            writers: %{
              # index => WriterAndRecorder
              s: %{},
              cnt: 0
            },
            q: [],
            logical_offset: %LogicalOffset{
              offset: 0,
              latest_log_size: 0,
              no_estimated_offset?: false
            },
            manager: nil

  @spec start_link({
          name :: GenServer.name(),
          registry :: GenServer.name(),
          fs :: ExWal.FS.t(),
          dir :: String.t(),
          log_num :: non_neg_integer(),
          manager :: pid() | nil
        }) :: GenServer.on_start()
  def start_link({name, registry, fs, dir, log_num, manager}) do
    GenServer.start_link(
      __MODULE__,
      {name, registry, fs, dir, log_num, manager},
      name: name
    )
  end

  def get(name), do: %__MODULE__{name: name}

  @spec write_record(
          name :: GenServer.name(),
          bytes :: binary(),
          opts :: keyword()
        ) ::
          {:ok, written_offset :: non_neg_integer()} | {:error, reason :: any()}
  def write_record(name, bytes, opts \\ []) do
    GenServer.call(name, {:sync_writes, bytes}, Keyword.get(opts, :timeout, 5000))
  end

  @spec stop(name :: GenServer.name()) :: :ok | {:error, reason :: any()}
  def stop(name) do
    GenServer.stop(name)
  end

  @spec switch_dir(name :: GenServer.name(), new_dir :: binary()) :: :ok
  def switch_dir(name, new_dir), do: GenServer.call(name, {:switch_dir, new_dir})

  @spec get_log(name :: GenServer.name()) :: {:ok, Models.VirtualLog.t()} | {:error, reason :: any()}
  def get_log(name), do: GenServer.call(name, :get_log)

  @spec current_observer(name :: GenServer.name()) :: pid() | GenServer.name()
  def current_observer(name), do: GenServer.call(name, :current_observer)

  # ---------------- server -----------------

  @impl GenServer
  def init({name, registry, fs, dir, log_num, manager}) do
    {
      :ok,
      %__MODULE__{
        name: name,
        registry: registry,
        fs: fs,
        log_num: log_num,
        manager: manager
      },
      {:continue, {:switch_dir, dir}}
    }
  end

  @impl GenServer
  def terminate(reason, %__MODULE__{manager: m} = state) do
    may_log_reason(reason)
    close_writers(state)

    m
    |> is_nil()
    |> unless do
      send(m, {:writer_shutdown, get_virtual_log(state)})
    end
  end

  @impl GenServer
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

  @impl GenServer
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
      %__MODULE__{
        state
        | logical_offset: %LogicalOffset{logical_offset | offset: offset},
          q: [p | q]
      }
    }
  end

  # estimate
  def handle_call(
        {:sync_writes, _} = input,
        _,
        %__MODULE__{logical_offset: %LogicalOffset{no_estimated_offset?: false}} = state
      ) do
    %__MODULE__{writers: writers, logical_offset: logical_offset} = state
    writer = get_latest_writer(writers)
    %WriterAndRecorder{w: w, observer: ob} = writer
    {:sync_writes, p} = input

    Observer.record_start(ob)

    w
    |> LogWriter.write_record(p)
    |> case do
      {:ok, of} ->
        Observer.record_end(ob, nil)

        {
          :reply,
          {:ok, of},
          %__MODULE__{
            state
            | logical_offset: %LogicalOffset{
                logical_offset
                | offset: of,
                  latest_log_size: of,
                  no_estimated_offset?: true
              }
          }
        }

      {:error, reason} ->
        Observer.record_end(ob, reason)

        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:sync_writes, _} = input, _, state) do
    %__MODULE__{writers: writers, logical_offset: logical_offset} = state
    writer = get_latest_writer(writers)
    {:sync_writes, p} = input
    %WriterAndRecorder{w: w, observer: ob} = writer
    %LogicalOffset{latest_log_size: ls, offset: offset} = logical_offset

    Observer.record_start(ob)

    w
    |> LogWriter.write_record(p)
    |> case do
      {:ok, of} ->
        Observer.record_end(ob, nil)
        delta = of - ls

        lo = %LogicalOffset{
          logical_offset
          | latest_log_size: of,
            offset: offset + delta
        }

        {
          :reply,
          {:ok, lo.offset},
          %__MODULE__{state | logical_offset: lo}
        }

      {:error, reason} ->
        Observer.record_end(ob, reason)
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:get_log, _, state) do
    {:reply, {:ok, get_virtual_log(state)}, state}
  end

  def handle_call(:current_observer, _, state) do
    %__MODULE__{writers: %{s: s, cnt: cnt}} = state
    %WriterAndRecorder{observer: ob} = Map.fetch!(s, cnt - 1)
    {:reply, ob, state}
  end

  @impl GenServer
  def handle_info({_task, {:switch_dir_notify, {:ok, _} = notify}}, state) do
    {:ok, {idx, wr}} = notify
    %__MODULE__{writers: writers, q: q} = state
    may_write_buffer(wr, q)

    %{s: s, cnt: cnt} = writers
    writers = %{s: Map.put(s, idx, wr), cnt: cnt + 1}

    {:noreply, %__MODULE__{state | writers: writers}}
  end

  def handle_info({_task, {:switch_dir_notify, {:error, _} = _notify}}, state) do
    # {:error, reason} = notify
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ----------------- private funcs -----------------

  defp do_switch_dir(new_dir, state)

  defp do_switch_dir(_, %__MODULE__{writers: %{cnt: @max_log_writer}} = state),
    do: {{:error, "exceeded switching limit"}, state}

  defp do_switch_dir(new_dir, state) do
    %__MODULE__{
      fs: fs,
      log_num: log_num,
      registry: registry,
      writers: %{cnt: idx}
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
            {:ok, observer} = Observer.start_link({:via, Registry, {registry, {:observer, log_num, idx}}})

            wr = %WriterAndRecorder{
              w: Single.get(writer_name),
              observer: observer,
              dir: new_dir,
              fs: fs
            }

            {:ok, {idx, wr}}

          err ->
            err
        end

      {:switch_dir_notify, ret}
    end)

    {:ok, state}
  end

  defp close_writers(state) do
    %__MODULE__{writers: %{s: s}} = state

    Enum.each(
      s,
      fn {_, %WriterAndRecorder{w: w, observer: ob}} ->
        LogWriter.stop(w)
        Observer.stop(ob)
      end
    )
  end

  defp may_log_reason(:normal), do: :pass
  defp may_log_reason(reason), do: Logger.error("FailoverWriter terminate: #{inspect(reason)}")

  @spec get_latest_writer(writers :: %{s: map(), cnt: non_neg_integer()}) :: WriterAndRecorder.t() | nil
  defp get_latest_writer(writers)
  defp get_latest_writer(%{cnt: 0}), do: nil

  defp get_latest_writer(writers) do
    %{s: s, cnt: cnt} = writers
    Map.get(s, cnt - 1)
  end

  defp may_write_buffer(wr, p)
  defp may_write_buffer(_, []), do: :pass

  defp may_write_buffer(wr, p) do
    %WriterAndRecorder{w: w, observer: ob} = wr

    p
    |> Enum.reverse()
    |> IO.iodata_to_binary()
    |> then(fn x ->
      Observer.record_start(ob)

      w
      |> LogWriter.write_record(x)
      |> case do
        {:ok, _} ->
          Observer.record_end(ob, nil)

        {:error, reason} ->
          Observer.record_end(ob, reason)
      end
    end)
  end

  defp get_virtual_log(%__MODULE__{log_num: log_num, writers: %{s: s}}) do
    segs =
      s
      |> Enum.map(fn {idx, wr} ->
        %WriterAndRecorder{dir: dir, fs: fs} = wr
        %Models.Segment{index: idx, dir: dir, fs: fs}
      end)
      |> Enum.sort_by(& &1.index)

    %Models.VirtualLog{log_num: log_num, segments: segs}
  end
end

defimpl ExWal.LogWriter, for: ExWal.LogWriter.Failover do
  alias ExWal.LogWriter.Failover

  def write_record(%Failover{name: name}, bytes, opts) do
    Failover.write_record(name, bytes, opts)
  end

  def stop(%Failover{name: name}) do
    Failover.stop(name)
  end
end
