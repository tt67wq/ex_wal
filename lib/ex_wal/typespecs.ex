defmodule ExWal.Typespecs do
  @moduledoc """
  Common typespecs for ExWal
  """
  @type name :: atom() | {:global, term()} | {:via, module(), term()}
  @type opts :: keyword()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @type dict :: %{binary() => any()}
  @type handler :: pid() | :file.fd()
  @type index :: integer()
  @type permision :: non_neg_integer()
end
