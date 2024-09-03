defmodule ExWal.Obeserver do
  @moduledoc false

  use Agent

  defstruct latency: 0, error: nil

  @type t :: %__MODULE__{
          latency: non_neg_integer(),
          error: any()
        }

  @type p :: GenServer.name() | pid()

  def start_link(name) do
    Agent.start_link(fn -> %__MODULE__{latency: 0, error: nil} end, name: name)
  end

  def record_start(p) do
    Agent.update(p, __MODULE__, :handle_record_start, [])
  end

  def record_end(p, error) do
    Agent.update(p, __MODULE__, :handle_record_end, [error])
  end

  def stats(p) do
    Agent.get(p, __MODULE__, :handle_stats, [])
  end

  # ---------------- handler ----------------
  def handle_record_start(state) do
    %__MODULE__{state | latency: System.monotonic_time()}
  end

  def handle_record_end(state, error) do
    %__MODULE__{state | latency: 0, error: error}
  end

  def handle_stats(%__MODULE__{latency: 0} = state) do
    %__MODULE__{error: e} = state
    {0, e}
  end

  def handle_stats(%__MODULE__{latency: latency} = state) do
    %__MODULE__{error: e} = state
    d = System.monotonic_time() - latency
    {System.convert_time_unit(d, :nanosecond, :millisecond), e}
  end
end
