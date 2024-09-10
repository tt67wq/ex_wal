defmodule ExWal.LogReader.Single do
  @moduledoc false

  use Agent, restart: :transient

  alias ExWal.Constant.Record
  alias ExWal.Models
  alias ExWal.Models.RecyclableRecord

  require ExWal.Constant.Record

  @legacy_header_size Record.legacy_header_size()
  @block_size Record.block_size()

  @type t :: %__MODULE__{
          name: Agent.name(),
          log_num: non_neg_integer(),
          file: ExWal.File.t(),
          buf: binary()
        }

  defstruct name: nil, log_num: 0, file: nil, buf: <<>>

  def start_link({name, log_num, file_path, fs}) do
    Agent.start_link(__MODULE__, :init, [name, log_num, file_path, fs], name: name)
  end

  @spec get(Agent.name() | pid()) :: t()
  def get(name), do: Agent.get(name, fn state -> state end)

  @spec next(Agent.name() | pid()) :: {:ok, bytes :: binary()} | :eof | {:error, reason :: term()}
  def next(name) do
    Agent.get_and_update(name, __MODULE__, :handle_next, [])
  end

  @spec recovery(Agent.name()) :: :ok
  def recovery(name) do
    Agent.update(name, __MODULE__, :handle_recovery, [])
  end

  def stop(name) do
    Agent.get(name, __MODULE__, :handle_closed, [])
    Agent.stop(name)
  end

  # ------------- handlers -------------
  def init(name, log_num, file_path, fs) do
    {:ok, file} = ExWal.FS.open(fs, file_path, [])
    %__MODULE__{name: name, log_num: log_num, file: file}
  end

  def handle_next(state)

  def handle_next(%__MODULE__{buf: <<>>} = state) do
    %__MODULE__{file: file} = state

    file
    |> ExWal.File.read(@block_size)
    |> case do
      {:ok, buf} ->
        handle_next(%__MODULE__{state | buf: buf})

      :eof ->
        {:eof, state}

      err ->
        {err, state}
    end
  end

  def handle_next(%__MODULE__{buf: buf} = state) when byte_size(buf) > @legacy_header_size, do: iter_read(state, [])

  # ignore rest of the buffer
  def handle_next(state), do: handle_next(%__MODULE__{state | buf: <<>>})

  def handle_recovery(state)
  def handle_recovery(%__MODULE__{buf: <<>>} = state), do: state
  def handle_recovery(state), do: %__MODULE__{state | buf: <<>>}

  def handle_closed(%__MODULE__{file: file}) do
    ExWal.File.close(file)
  end

  defp iter_read(%__MODULE__{buf: buf} = state, acc) do
    buf
    |> Models.RecyclableRecord.parse()
    |> case do
      {:ok, %Models.RecyclableRecord{payload: p} = rec, rest} ->
        rec
        |> RecyclableRecord.last_chunk?()
        |> if do
          {make_resp([p | acc]), %__MODULE__{state | buf: rest}}
        else
          iter_read(%__MODULE__{state | buf: rest}, [p | acc])
        end

      {:ok, nil, _} ->
        {make_resp(acc), %__MODULE__{state | buf: <<>>}}

      err ->
        {err, state}
    end
  end

  defp make_resp([]), do: <<>>

  defp make_resp(chunks) do
    chunks
    |> Enum.reverse()
    |> IO.iodata_to_binary()
  end
end

defimpl ExWal.LogReader, for: ExWal.LogReader.Single do
  alias ExWal.LogReader.Single

  def next(%Single{name: name}) do
    Single.next(name)
  end

  def recovery(%Single{name: name}) do
    Single.recovery(name)
  end

  def stop(%Single{name: name}) do
    Single.stop(name)
  end
end
