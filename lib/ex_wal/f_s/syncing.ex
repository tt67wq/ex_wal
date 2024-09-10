defmodule ExWal.FS.Syncing do
  @moduledoc """
  Syncing file system.
  """

  use Agent

  alias ExWal.FS

  @type t :: %__MODULE__{
          name: GenServer.name(),
          fs: ExWal.FS.t(),
          dynamic: atom(),
          registry: atom()
        }

  defstruct name: nil, fs: nil, dynamic: nil, registry: nil

  def start_link({name, fs, dynamic, registry}) do
    Agent.start_link(__MODULE__, :init, [name, fs, dynamic, registry], name: name)
  end

  def create(name, fname), do: Agent.get(name, __MODULE__, :handle_create, [fname])

  def open_read_write(name, fname), do: Agent.get(name, __MODULE__, :handle_open_read_write, [fname])

  # ---------------- handlers -------------

  def init(name, fs, dynamic, registry) do
    %__MODULE__{name: name, fs: fs, dynamic: dynamic, registry: registry}
  end

  def handle_create(%__MODULE__{fs: fs, dynamic: dynamic, registry: registry}, name) do
    {:ok, file} = FS.create(fs, name)
    file_name = file_name(registry, name)
    {:ok, _} = DynamicSupervisor.start_child(dynamic, {ExWal.File.Syncing, {file_name, file, 1024 * 1024}})
    {:ok, ExWal.File.Syncing.get(file_name)}
  end

  def handle_open_read_write(%__MODULE__{fs: fs, dynamic: dynamic, registry: registry}, name) do
    {:ok, file} = FS.open_read_write(fs, name)
    file_name = file_name(registry, name)
    {:ok, _} = DynamicSupervisor.start_child(dynamic, {ExWal.File.Syncing, {file_name, file, 1024 * 1024}})
    {:ok, ExWal.File.Syncing.get(file_name)}
  end

  defp file_name(registry, name), do: {:via, Registry, {registry, {:file, name}}}
end

defimpl ExWal.FS, for: ExWal.FS.Syncing do
  alias ExWal.FS
  alias ExWal.FS.Syncing

  def create(%Syncing{name: name}, fname) do
    Syncing.create(name, fname)
  end

  def open_read_write(%Syncing{name: name}, fname, _opts) do
    Syncing.open_read_write(name, fname)
  end

  defp delegate(%Syncing{fs: fs}, method, args), do: apply(FS, method, [fs | args])

  def link(impl, old_name, new_name), do: delegate(impl, :link, [old_name, new_name])
  def remove(impl, name), do: delegate(impl, :remove, [name])
  def open(impl, name, opts), do: delegate(impl, :open, [name, opts])
  def open_dir(impl, name), do: delegate(impl, :open_dir, [name])
  def remove_all(impl, name), do: delegate(impl, :remove_all, [name])
  def rename(impl, old_name, new_name), do: delegate(impl, :rename, [old_name, new_name])
  def reuse_for_write(impl, old_name, new_name), do: delegate(impl, :reuse_for_write, [old_name, new_name])
  def mkdir_all(impl, name), do: delegate(impl, :mkdir_all, [name])
  def lock(impl, name), do: delegate(impl, :lock, [name])
  def list(impl, name), do: delegate(impl, :list, [name])
  def stat(impl, name), do: delegate(impl, :stat, [name])
end
