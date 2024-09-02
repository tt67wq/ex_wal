defmodule ExWal.Manager.DirAndFile do
  @moduledoc false
  defstruct dir: "", error_cnt: 0
end

defmodule ExWal.Manager.Monitor do
  @moduledoc false

  use GenServer

  alias ExWal.LogWriter.Failover
  alias ExWal.Manager.DirAndFile

  require Logger

  @unhealthy_sampling_interval 100
  @unhealthy_threashold 100

  defstruct name: nil,
            last_fall_at: 0,
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
            writer: %{
              w: nil,
              type: :primary,
              latency_at_switch: 0,
              num_switch: 0
            }

  def start_link({name, dirs}) do
    GenServer.start_link(__MODULE__, {name, dirs}, name: name)
  end

  def stop(name) do
    GenServer.stop(name)
  end

  # ---------------- server ---------------
  def init({name, dirs}) do
    {:ok, %__MODULE__{name: name, dirs: dirs}, @unhealthy_sampling_interval}
  end

  def handle_info(:timeout, %{writer: nil} = state) do
    {:noreply, state, @unhealthy_sampling_interval}
  end

  def handle_info(:timeout, %{writer: %{w: nil}} = state) do
    {:noreply, state, @unhealthy_sampling_interval}
  end

  def handle_info(:timeout, state) do
    %__MODULE__{writer: writer, dirs: dirs} = state
    %{w: %Failover{name: name}} = writer
    {latency, error} = Failover.latency_and_error(name)
    {switch?, state} = switchable?(latency, error, dirs)

    {:noreply, may_switch(switch?, state), @unhealthy_sampling_interval}
  end

  def terminate(reason, _state) do
    may_log_reason(reason)
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
