defmodule ExWal.Monitor.DirAndFile do
  @moduledoc false
  defstruct dir: "", error_cnt: 0
end

defmodule ExWal.Monitor do
  @moduledoc false

  use GenServer, restart: :transient

  alias ExWal.LogWriter.Failover
  alias ExWal.Monitor.DirAndFile
  alias ExWal.Obeserver

  require Logger

  @unhealthy_sampling_interval 100
  @unhealthy_threashold 100

  defstruct last_fall_at: 0,
            dirs: %{
              primary: %DirAndFile{
                dir: "",
                error_cnt: 0
              },
              secondary: %DirAndFile{
                dir: "",
                error_cnt: 0
              }
            },
            observer: nil,
            writer: %{
              w: nil,
              type: :primary,
              latency_at_switch: 0,
              num_switch: 0
            }

  def start_link({dirs, observer}) do
    GenServer.start_link(__MODULE__, {dirs, observer})
  end

  def stop(p), do: GenServer.stop(p)

  def new_writer(p, writer_creator_fn), do: GenServer.call(p, {:new_writer, writer_creator_fn})

  def no_writer(p), do: GenServer.call(p, :no_writer)

  # ---------------- server ---------------
  def init({dirs, observer}) do
    {:ok, %__MODULE__{dirs: dirs, observer: observer}, @unhealthy_sampling_interval}
  end

  def terminate(reason, _state) do
    may_log_reason(reason)
  end

  def handle_info(:timeout, %{writer: nil} = state) do
    {:noreply, state, @unhealthy_sampling_interval}
  end

  def handle_info(:timeout, %{writer: %{w: nil}} = state) do
    {:noreply, state, @unhealthy_sampling_interval}
  end

  def handle_info(:timeout, state) do
    %__MODULE__{dirs: dirs, observer: ob} = state
    {latency, error} = Obeserver.stats(ob)
    {switch?, state} = switchable?(latency, error, dirs)

    {:noreply, may_switch(switch?, state), @unhealthy_sampling_interval}
  end

  def handle_call({:new_writer, _}, _from, %__MODULE__{writer: %{w: w}}) when not is_nil(w) do
    raise ExWal.Exception, message: "previous writer not closed"
  end

  def handle_call({:new_writer, writer_creator_fn}, _from, state) do
    %__MODULE__{writer: writer, dirs: dirs} = state
    %{type: type} = writer
    %DirAndFile{dir: dir} = Map.fetch!(dirs, type)
    writer = %{writer | w: writer_creator_fn.(dir)}

    {:reply, :ok, %__MODULE__{state | writer: writer}}
  end

  def handle_call(:no_writer, _, state) do
    %__MODULE__{writer: writer} = state
    writer = %{writer | w: nil}
    {:reply, :ok, %__MODULE__{state | writer: writer}}
  end

  defp may_log_reason(:normal), do: :pass
  defp may_log_reason(reason), do: Logger.error("FailoverWriter terminate: #{inspect(reason)}")

  defp switchable?(latency, error, state)

  defp switchable?(
         _latency,
         _error,
         %__MODULE__{writer: %{type: :primary}, dirs: %{secondary: %DirAndFile{error_cnt: x}}} = state
       )
       when x >= 2,
       do: {false, state}

  defp switchable?(latency, nil, state) when latency > @unhealthy_threashold do
    %__MODULE__{writer: writer} = state
    %{num_swtich: ns, latency_at_switch: l} = writer
    c = ns < 2 or latency > 2 * l

    if c do
      {true, %__MODULE__{state | writer: %{writer | latency_at_switch: latency}}}
    else
      {false, state}
    end
  end

  defp switchable?(_latency, _error, state) do
    %__MODULE__{dirs: dirs, writer: %{type: type}} = state
    i = %DirAndFile{error_cnt: c} = dirs[type]
    dirs = Map.put(dirs, type, %{i | error_cnt: c + 1})
    {true, %__MODULE__{state | dirs: dirs}}
  end

  defp may_switch(switch?, state)
  defp may_switch(false, state), do: state

  defp may_switch(true, state) do
    %__MODULE__{writer: writer, dirs: dirs} = state
    %{w: %Failover{name: n}, type: type, num_switch: ns} = writer
    writer = %{writer | type: switch_type(type), num_switch: ns + 1}
    %DirAndFile{dir: dir} = Map.get(dirs, type)
    :ok = Failover.switch_dir(n, dir)
    %__MODULE__{state | writer: writer}
  end

  defp switch_type(:primary), do: :secondary
  defp switch_type(:secondary), do: :primary
end
