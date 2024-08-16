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

       Examples
       -------
       ```
       iex> ExWalApp.manager("/data/wal", :standalone)
       ```
      """
      @spec manager(dirname :: binary(), mode :: :standalone) :: ok_t(ExWal.Manager.t()) | err_t()
      def manager(dirname, mode \\ :standalone), do: delegate(:manager, [dirname, mode])
    end
  end
end
