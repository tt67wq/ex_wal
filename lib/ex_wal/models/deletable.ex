defmodule ExWal.Models.Deletable do
  @moduledoc false

  # alias ExWal.Models
  alias ExWal.Models.VirtualLog

  defstruct fs: nil, path: "", log_num: 0

  @type t :: %__MODULE__{
          fs: ExWal.FS.t(),
          path: String.t(),
          log_num: VirtualLog.log_num()
        }
end
