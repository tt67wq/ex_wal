# defprotocol ExWal.Store do
#   def open(impl, opts \\ [])
#   def read_all(impl)
#   def write_all(impl, data)
# end

# defprotocol ExWal.StoreHandler do
#   def append(impl, data)
#   def sync(impl)
#   def close(impl)
# end

# defprotocol ExWal.StoreManager do
#   def new_store(impl, path)
#   def mkdir(impl, path, opts \\ [])
#   def rename(impl, old_path, new_path)
#   def rm(impl, path)
#   def wildcard(impl, path)
#   def dir?(impl, path)
# end
