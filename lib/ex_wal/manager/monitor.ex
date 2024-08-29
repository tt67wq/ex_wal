defmodule ExWal.Manager.Monitor do
  @moduledoc false

  use GenServer

  alias ExWal.LogWriter.Failover

  require Logger

  @unhealthy_sampling_interval 100
  @unhealthy_threashold 100

  defstruct name: nil,
            dirs: %{
              type: :primary,
              primary: %{
                error_cnt: 0
              },
              secondary: %{
                error_cnt: 0
              }
            },
            last_fall_at: 0,
            writer: %{
              w: nil,
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

  def handle_info(:timeout, state) do
    %__MODULE__{writer: writer, dirs: dirs} = state
    %{w: %Failover{name: name}} = writer
    {latency, error} = Failover.latency_and_error(name)
    switchable?(latency, error, dirs)
  end

  def terminate(reason, _state) do
    may_log_reason(reason)
  end

  defp may_log_reason(:normal), do: :pass
  defp may_log_reason(reason), do: Logger.error("FailoverWriter terminate: #{inspect(reason)}")

  defp switchable?(latency, error, state)

  defp switchable?(_latency, _error, %__MODULE__{dirs: %{type: :primary, secondary: %{error_cnt: x}}} = state)
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
end
