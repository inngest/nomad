
client {
  enabled = true

  template {
    # Allow nomad to access arbitrary files on disk, instead of just in the task working directory.
    disable_file_sandbox = true
    function_denylist = []
  }
}
