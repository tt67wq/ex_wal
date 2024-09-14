defmodule ExWal do
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  @external_resource "README.md"

  defmacro __using__(opts) do
    quote do
      alias ExWal.Core

      @type ok_t(ret) :: {:ok, ret}
      @type err_t() :: {:error, term()}

      def init(config) do
        {:ok, config}
      end

      defoverridable init: 1

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def start_link(config \\ []) do
        otp_app = unquote(opts[:otp_app])

        {:ok, cfg} =
          otp_app
          |> Application.get_env(__MODULE__, config)
          |> init()

        ExWal.Supervisor.start_link(__MODULE__, cfg)
      end

      defp delegate(method, args), do: apply(Core, method, [__MODULE__ | args])

      @doc """
      Get a manager of the WAL.

      ## Parameters

      - `mode`: `:standalone` or `:failover`
      - `manager_name`: the name of the manager
      - `opts`: the options of the manager, see `ExWal.Manager.Options.t()`

       Returns

      - `{:ok, ExWal.Manager.t()}`: the manager
      - `{:error, reason}`: the reason of the error
      """
      @spec manager(
              mode :: :standalone | :failover,
              manager_name :: term(),
              opts :: ExWal.Manager.Options.t()
            ) :: {:ok, ExWal.Manager.t()} | {:error, reason :: term()}
      def manager(mode, manager_name, opts), do: delegate(:manager, [mode, manager_name, opts])

      @doc """
      Open a reader for reading the virtual log.

       Parameters

      - `vlog`: the virtual log

       Returns

      - `{:ok, ExWal.LogReader.t()}`: the reader
      - `{:error, reason}`: the reason of the error
      """
      @spec open_for_read(vlog :: ExWal.Models.VirtualLog.t()) :: {:ok, ExWal.LogReader.t()} | {:error, reason :: term()}
      def open_for_read(vlog), do: delegate(:open_for_read, [vlog])
    end
  end
end
