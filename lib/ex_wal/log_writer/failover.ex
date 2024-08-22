defmodule ExWal.LogWriter.Failover do
  @moduledoc """
  FailoverWriter is the implementation of LogWriter in failover mode.
  """

  use GenServer

  alias ExWal.FS
  alias ExWal.LogWriter.Single
  alias ExWal.Models

  require Logger

  @max_log_writer 10

  defstruct name: nil, registry: nil, dynamic_sup: nil, fs: nil, dir: "", log_num: 0, q: [], writers: {[], 0}

  def start_link({name, registry, dynamic_sup, fs, dir, log_num}) do
    GenServer.start_link(
      __MODULE__,
      {name, registry, dynamic_sup, fs, dir, log_num},
      name: name
    )
  end

  @spec stop(name :: GenServer.name()) :: :ok | {:error, reason :: any()}
  def stop(name) do
    GenServer.stop(name)
  end

  # ---------------- server -----------------

  def init({name, registry, dynamic_sup, fs, dir, log_num}) do
    {
      :ok,
      %__MODULE__{
        name: name,
        registry: registry,
        dynamic_sup: dynamic_sup,
        fs: fs,
        dir: dir,
        log_num: log_num
      },
      {:continue, {:switch_dir, dir}}
    }
  end

  def terminate(:normal, state), do: close_writers(state)

  def terminate(reason, state) do
    Logger.error("FailoverWriter: terminate: #{inspect(reason)}")
    close_writers(state)
  end

  def handle_continue({:switch_dir, dir}, state) do
    switch_dir(dir, state)
  end

  # ----------------- private funcs -----------------

  defp switch_dir(new_dir, state)
  defp switch_dir(_, %__MODULE__{writers: {_, @max_log_writer}} = state), do: {:stop, :normal, state}

  defp switch_dir(new_dir, %__MODULE__{writers: {writers, idx}} = state) do
    %__MODULE__{fs: fs, log_num: log_num, registry: registry, dynamic_sup: dsp} = state
    # new single writer
    log_name = Path.join(new_dir, Models.VirtualLog.filename(log_num, idx))
    {:ok, file} = FS.create(fs, log_name)
    name = {:via, Registry, {registry, {:writer, log_num, idx}}}
    {:ok, _} = DynamicSupervisor.start_child(dsp, {Single, {name, file, log_num}})
    {:noreply, %__MODULE__{state | writers: {[name | writers], idx + 1}}}
  end

  defp close_writers(state) do
    %__MODULE__{writers: {writers, _}} = state
    Enum.each(writers, &Single.stop/1)
  end
end
