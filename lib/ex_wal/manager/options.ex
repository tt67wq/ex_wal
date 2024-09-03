defmodule ExWal.Manager.Options do
  @moduledoc false
  defstruct primary: [
              fs: nil,
              dir: ""
            ],
            secondary: [
              fs: nil,
              dir: ""
            ],
            max_num_recyclable_logs: 0

  @type t :: %__MODULE__{
          primary: [
            fs: ExWal.FS.t(),
            dir: binary()
          ],
          secondary: [
            fs: ExWal.FS.t(),
            dir: binary()
          ],
          max_num_recyclable_logs: non_neg_integer()
        }
end
