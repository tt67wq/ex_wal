defmodule ExWal.LogReader.Virtual do
  @moduledoc false

  alias ExWal.Models.Segment
  alias ExWal.Models.VirtualLog

  @type t :: %__MODULE__{
          name: Agent.name(),
          registry: atom(),
          virtual_log: VirtualLog.t(),
          reader: ExWal.LogReader.t()
        }
  defstruct name: nil, registry: nil, virtual_log: nil, reader: nil

  def start_link({name, registry, vlog}) do
    Agent.start_link(__MODULE__, :init, [name, registry, vlog], name: name)
  end

  def get(name), do: Agent.get(name, fn state -> state end)

  @spec next(Agent.name()) :: {:ok, binary()} | :eof | {:error, reason :: term()}
  def next(name), do: Agent.get_and_update(name, __MODULE__, :handle_next, [])

  @spec recovery(Agent.name()) :: :ok
  def recovery(name), do: Agent.get(name, __MODULE__, :handle_recovery, [])

  # ---------------- handlers ----------------

  def init(name, registry, vlog) do
    %__MODULE__{name: name, registry: registry, virtual_log: vlog}
  end

  def handle_next(state)

  def handle_next(%__MODULE__{reader: reader} = state) do
    reader
    |> ExWal.LogReader.next()
    |> case do
      {:ok, p} ->
        {{:ok, p}, state}

      :eof ->
        may_handle_next(state)

      err ->
        {err, state}
    end
  end

  def handle_recovery(state)

  def handle_recovery(%__MODULE__{reader: reader}) do
    ExWal.LogReader.recovery(reader)
  end

  defp next_file(state)
  defp next_file(%__MODULE__{reader: nil, virtual_log: %VirtualLog{segments: []}}), do: {:error, :eof}

  defp next_file(%__MODULE__{reader: nil} = state) do
    %__MODULE__{virtual_log: vlog, registry: registry} = state

    %VirtualLog{
      log_num: log_num,
      segments: [
        %Segment{index: index, dir: dir, fs: fs} | segs
      ]
    } = vlog

    filename = VirtualLog.filename(log_num, index)
    name = {:via, Registry, {registry, {:reader, filename}}}

    {:ok, _} = ExWal.LogReader.Single.start_link({name, log_num, Path.join(dir, filename), fs})

    {:ok, %__MODULE__{state | reader: name, virtual_log: %VirtualLog{vlog | segments: segs}}}
  end

  defp next_file(%__MODULE__{reader: reader} = state) do
    ExWal.LogReader.Single.stop(reader)
    next_file(%__MODULE__{state | reader: nil})
  end

  defp may_handle_next(state) do
    state
    |> next_file()
    |> case do
      {:ok, state} -> handle_next(state)
      err -> {err, state}
    end
  end
end

defimpl ExWal.LogReader, for: ExWal.LogReader.Virtual do
  alias ExWal.LogReader.Virtual

  def next(%Virtual{name: name}) do
    Virtual.next(name)
  end

  def recovery(%Virtual{name: name}) do
    Virtual.recovery(name)
  end
end
