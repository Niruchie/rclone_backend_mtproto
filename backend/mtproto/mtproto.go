package mtproto

import (
	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration"
	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/options"
	"github.com/rclone/rclone/fs"
)

// Register the MTProto backend.
func init() {
	fs.Register(
		&fs.RegInfo{
			Config:      configuration.Configuration,
			Options:     options.OptionList,
			NewFs:       configuration.Fs,
			Description: "MTProto",
			Name:        "mtproto",
		},
	)
}
