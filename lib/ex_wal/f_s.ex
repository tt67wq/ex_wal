defprotocol ExWal.FS do
  @doc """
  Create creates the named file for reading and writing. If a file
  already exists at the provided name, it's removed first ensuring the
  resulting file descriptor points to a new inode.
  """
  def create(impl, name)

  @doc """
  create_link creates a hard link to the file
  """
  def link(impl, old_name, new_name)

  @doc """
  remove a file
  """
  def remove(impl, name)

  @doc """
  open_read_write opens the named file for reading and writing. If the file
  does not exist, it is created.
  """
  def open_read_write(impl, name)

  @doc """
  open_dir opens the named directory for syncing
  """
  def open_dir(impl, name)

  @doc """
  remove_all removes all files in a directory
  """
  def remove_all(impl, name)

  @doc """
  rename renames a file
  """
  def rename(impl, old_name, new_name)

  @doc """
  reuse_for_write attempts to reuse the file with oldname by renaming it to newname and opening
  it for writing without truncation. It is acceptable for the implementation to choose not
  to reuse oldname, and simply create the file with newname -- in this case the implementation
  should delete oldname. If the caller calls this function with an oldname that does not exist,
  the implementation may return an error.

  """
  def reuse_for_write(impl, old_name, new_name)

  @doc """
  mkdir_all creates a directory and any necessary parents. The permission bits
  perm have the same semantics as for os.mkdir. If the directory already exists,
  mkdir_all does nothing.
  """
  def mkdir_all(impl, name)

  @doc """
  Lock locks the given file, creating the file if necessary, and
  truncating the file if it already exists. The lock is an exclusive lock
  (a write lock), but locked files should neither be read from nor written
  to. Such files should have zero size and only exist to co-ordinate
  ownership across processes.

  A nil Closer is returned if an error occurred. Otherwise, close that
  Closer to release the lock.

  On Linux and OSX, a lock has the same semantics as fcntl(2)'s advisory
  locks. In particular, closing any other file descriptor for the same
  file will release the lock prematurely.

  Attempting to lock a file that is already locked by the current process
  returns an error and leaves the existing lock untouched.

  Lock is not yet implemented on other operating systems, and calling it
  """
  def lock(impl, name)

  @doc """
  list lists the files in the given directory.
  """
  def list(impl, name)

  @doc """
  stat returns the file info for the given file.
  """
  def stat(impl, name)
end
