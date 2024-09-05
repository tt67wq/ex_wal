defprotocol ExWal.Recycler do
  @moduledoc """
  The Recycler protocol defines the behaviour of a WAL file recycler.
  """

  @doc """
  Add attempts to recycle the log file specified by file. Returns true if
  the log file should not be deleted (i.e. the log is being recycled), and
  false otherwise.
  """
  @type log :: ExWal.Models.VirtualLog.t()
  @type log_num :: ExWal.Models.VirtualLog.log_num()

  @spec initialize(impl :: t(), capacity :: non_neg_integer()) :: :ok
  def initialize(impl, capacity)

  @spec add(impl :: t(), log :: log()) :: boolean()
  def add(impl, log)

  @doc """
  set_min sets the minimum log number that is allowed to be recycled.
  """
  @spec set_min(impl :: t(), log_num :: log_num()) :: :ok
  def set_min(impl, log_num)

  @doc """
  get_min returns the current minimum log number that is allowed to be recycled.
  """
  @spec get_min(impl :: t()) :: log_num()
  def get_min(impl)

  @doc """
  Peek returns the log at the head of the recycling queue, or the zero value
  fileInfo and false if the queue is empty.
  """
  @spec peek(impl :: t()) :: log() | nil
  def peek(impl)

  @doc """
  Pop removes the log number at the head of the recycling queue, enforcing
  that it matches the specified seq. An error is returned of the recycling
  queue is empty or the head log number does not match the specified one.
  """
  @spec pop(impl :: t()) :: log() | nil
  def pop(impl)

  @spec stop(impl :: t()) :: :ok
  def stop(impl)
end

defmodule ExWal.Recycler.ETS do
  @moduledoc """
  An ETS based WAL file recycler.
  """

  use Agent, restart: :transient

  alias ExWal.Models.VirtualLog

  @type log_num :: ExWal.Models.VirtualLog.log_num()
  @type log :: ExWal.Models.VirtualLog.t()

  @type t :: %__MODULE__{
          name: Agent.name(),
          logs: [log()],
          size: non_neg_integer(),
          capacity: non_neg_integer(),
          min: log_num(),
          max: log_num()
        }

  @type p :: GenServer.name() | pid()

  defstruct name: nil, logs: [], size: 0, capacity: 64, min: 0, max: 0

  def start_link(name) do
    Agent.start_link(__MODULE__, :init, [name], name: name)
  end

  def stop(name) do
    Agent.stop(name)
  end

  @spec get(name :: p()) :: t()
  def get(name), do: Agent.get(name, fn state -> state end)

  @spec initialize(name :: p(), capacity :: non_neg_integer()) :: :ok
  def initialize(name, capacity) do
    Agent.get_and_update(name, __MODULE__, :handle_initialize, [capacity])
  end

  @spec add(name :: p(), log :: log()) :: boolean()
  def add(name, log) do
    Agent.get_and_update(name, __MODULE__, :handle_add, [log])
  end

  @spec set_min(name :: p(), log_num :: log_num()) :: :ok
  def set_min(name, log_num) do
    Agent.get_and_update(name, __MODULE__, :handle_set_min, [log_num])
  end

  @spec get_min(name :: p()) :: log_num()
  def get_min(name) do
    Agent.get(name, __MODULE__, :handle_get_min, [])
  end

  @spec peek(name :: p()) :: log() | nil
  def peek(name) do
    Agent.get(name, __MODULE__, :handle_peek, [])
  end

  @spec pop(name :: p()) :: log() | nil
  def pop(name) do
    Agent.get_and_update(name, __MODULE__, :handle_pop, [])
  end

  # ------------- handlers -------------

  def init(name) do
    %__MODULE__{name: name, logs: []}
  end

  def handle_initialize(state, capacity) do
    {:ok, %__MODULE__{state | capacity: capacity}}
  end

  def handle_add(state, log)

  def handle_add(%__MODULE__{size: size, capacity: capacity} = state, _log) when size >= capacity, do: {false, state}
  def handle_add(%__MODULE__{min: min} = state, %VirtualLog{log_num: log_num}) when log_num < min, do: {false, state}

  def handle_add(%__MODULE__{max: max} = state, %VirtualLog{log_num: log_num}) when log_num <= max, do: {true, state}

  def handle_add(state, log) do
    %__MODULE__{logs: logs, size: size} = state
    %VirtualLog{log_num: log_num} = log

    state =
      %__MODULE__{
        logs: [log | logs],
        size: size + 1,
        max: log_num
      }

    {true, state}
  end

  def handle_set_min(state, log_num) do
    {:ok, %__MODULE__{state | min: log_num}}
  end

  def handle_get_min(%__MODULE__{min: min}), do: min

  def handle_peek(state)
  def handle_peek(%__MODULE__{size: 0}), do: nil

  def handle_peek(state) do
    %__MODULE__{logs: [log | _]} = state
    log
  end

  def handle_pop(state)
  def handle_pop(%__MODULE__{size: 0} = state), do: {nil, state}

  def handle_pop(state) do
    %__MODULE__{logs: [log | logs], size: size} = state
    {log, %__MODULE__{state | logs: logs, size: size - 1}}
  end
end

defimpl ExWal.Recycler, for: ExWal.Recycler.ETS do
  alias ExWal.Recycler.ETS

  def initialize(%ETS{name: name}, capacity) do
    ETS.initialize(name, capacity)
  end

  def add(%ETS{name: name}, log) do
    ETS.add(name, log)
  end

  def set_min(%ETS{name: name}, log_num) do
    ETS.set_min(name, log_num)
  end

  def get_min(%ETS{name: name}) do
    ETS.get_min(name)
  end

  def peek(%ETS{name: name}) do
    ETS.peek(name)
  end

  def pop(%ETS{name: name}) do
    ETS.pop(name)
  end

  def stop(%ETS{name: name}) do
    ETS.stop(name)
  end
end
