package main

import (
	"github.com/rclone/rclone/cmd"
	_ "github.com/rclone/rclone/cmd/all" // import all commands
	_ "github.com/rclone/rclone/lib/plugin" // import plugins
	_ "github.com/rclone/rclone/backend/all" // import all backends
	_ "github.com/Niruchie/rclone_backend_mtproto/backend/mtproto" // import mtproto backend
)

func main() {
	cmd.Main()
}
