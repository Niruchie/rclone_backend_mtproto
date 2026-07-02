// Package configuration provides the MTProto-based rclone filesystem backend.
package configuration

import (
	"context"
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"
)

// ---------------------------------------------------------------------------
// Type checks — compile-time assertions that MTProtoDirectory implements the
// required interfaces.
// ---------------------------------------------------------------------------

var (
	_ fs.Directory     = (*MTProtoDirectory)(nil)
	_ fs.FullDirectory = (*MTProtoDirectory)(nil)
)

// ---------------------------------------------------------------------------
// MTProtoDirectory
// ---------------------------------------------------------------------------

// MTProtoDirectory represents a directory in the MTProto filesystem, backed
// by a Telegram forum topic.  It implements fs.Directory and all optional
// interfaces from fs.FullDirectory (Metadataer, SetMetadataer, SetModTimer).
type MTProtoDirectory struct {
	fs       fs.Info   // The filesystem this directory belongs to
	remote   string    // Remote path
	modTime  time.Time // Modification time
	size     int64     // Size (-1 for unknown)
	items    int64     // Number of items (-1 for unknown)
	id       string    // Telegram forum topic ID
	parentID string    // Parent directory ID (optional)
}

// NewMTProtoDirectory creates a new MTProtoDirectory with the given
// filesystem, remote path, and modification time.  Size and Items default
// to -1 (unknown).
func NewMTProtoDirectory(fs fs.Info, remote string, modTime time.Time) *MTProtoDirectory {
	return &MTProtoDirectory{
		fs:      fs,
		remote:  remote,
		modTime: modTime,
		size:    -1,
		items:   -1,
	}
}

// NewMTProtoDirectoryFromTopic creates a MTProtoDirectory from a Telegram
// forum topic object and the owning filesystem.  It extracts the remote
// name, modification date, and topic ID.
func NewMTProtoDirectoryFromTopic(fs fs.Info, topicName string, topicDate time.Time, topicID int32, rootPrefix string) *MTProtoDirectory {
	remote := trimPrefix(topicName, rootPrefix)
	return &MTProtoDirectory{
		fs:      fs,
		remote:  remote,
		modTime: topicDate,
		size:    -1,
		items:   -1,
		id:      fmt.Sprintf("%d", topicID),
	}
}

// trimPrefix removes the given prefix from s (if present) and returns the
// result.  Unlike strings.TrimPrefix it also strips a leading slash after
// the prefix so the returned path is clean.
func trimPrefix(s, prefix string) string {
	if len(s) < len(prefix) {
		return s
	}
	if s[:len(prefix)] == prefix {
		s = s[len(prefix):]
	}
	if len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}

// ---------------------------------------------------------------------------
// fs.DirEntry implementation
// ---------------------------------------------------------------------------

// Fs returns read-only access to the filesystem that this directory belongs to.
//
// Read more about the method at [fs.DirEntry.Fs]
//
// [fs.DirEntry.Fs]: https://pkg.go.dev/github.com/rclone/rclone/fs#DirEntry.Fs
func (d *MTProtoDirectory) Fs() fs.Info {
	return d.fs
}

// String returns the remote path of the directory.
//
// Read more about the method at [fs.DirEntry.String]
//
// [fs.DirEntry.String]: https://pkg.go.dev/github.com/rclone/rclone/fs#DirEntry.String
func (d *MTProtoDirectory) String() string {
	return d.remote
}

// Remote returns the remote path.
//
// Read more about the method at [fs.DirEntry.Remote]
//
// [fs.DirEntry.Remote]: https://pkg.go.dev/github.com/rclone/rclone/fs#DirEntry.Remote
func (d *MTProtoDirectory) Remote() string {
	return d.remote
}

// ModTime returns the modification date of the directory.
//
// Read more about the method at [fs.DirEntry.ModTime]
//
// [fs.DirEntry.ModTime]: https://pkg.go.dev/github.com/rclone/rclone/fs#DirEntry.ModTime
func (d *MTProtoDirectory) ModTime(_ context.Context) time.Time {
	return d.modTime
}

// Size returns the size of the directory.  Returns -1 when unknown.
//
// Read more about the method at [fs.DirEntry.Size]
//
// [fs.DirEntry.Size]: https://pkg.go.dev/github.com/rclone/rclone/fs#DirEntry.Size
func (d *MTProtoDirectory) Size() int64 {
	return d.size
}

// ---------------------------------------------------------------------------
// fs.Directory implementation
// ---------------------------------------------------------------------------

// Items returns the count of items in this directory.  Returns -1 when
// unknown.
//
// Read more about the method at [fs.Directory.Items]
//
// [fs.Directory.Items]: https://pkg.go.dev/github.com/rclone/rclone/fs#Directory.Items
func (d *MTProtoDirectory) Items() int64 {
	return d.items
}

// ID returns the internal ID of this directory (Telegram forum topic ID).
// Returns "" when unknown.
//
// Read more about the method at [fs.Directory.ID]
//
// [fs.Directory.ID]: https://pkg.go.dev/github.com/rclone/rclone/fs#Directory.ID
func (d *MTProtoDirectory) ID() string {
	return d.id
}

// ---------------------------------------------------------------------------
// fs.FullDirectory — optional interfaces
// ---------------------------------------------------------------------------

// ParentID returns the parent directory ID.  Returns "" when unknown.
//
// Read more about the method at [fs.FullDirectory.ParentID]
//
// [fs.FullDirectory.ParentID]: https://pkg.go.dev/github.com/rclone/rclone/fs#FullDirectory.ParentID
func (d *MTProtoDirectory) ParentID() string {
	return d.parentID
}

// SetModTime sets the modification date on this directory.
//
// Read more about the method at [fs.SetModTimer.SetModTime]
//
// [fs.SetModTimer.SetModTime]: https://pkg.go.dev/github.com/rclone/rclone/fs#SetModTimer.SetModTime
func (d *MTProtoDirectory) SetModTime(_ context.Context, t time.Time) error {
	d.modTime = t
	return nil
}

// Metadata returns metadata for this directory.
//
// Read more about the method at [fs.Metadataer.Metadata]
//
// [fs.Metadataer.Metadata]: https://pkg.go.dev/github.com/rclone/rclone/fs#Metadataer.Metadata
func (d *MTProtoDirectory) Metadata(_ context.Context) (fs.Metadata, error) {
	return fs.Metadata{}, nil
}

// SetMetadata sets metadata for this directory.
// Currently not supported.
//
// Read more about the method at [fs.SetMetadataer.SetMetadata]
//
// [fs.SetMetadataer.SetMetadata]: https://pkg.go.dev/github.com/rclone/rclone/fs#SetMetadataer.SetMetadata
func (d *MTProtoDirectory) SetMetadata(_ context.Context, _ fs.Metadata) error {
	return fs.ErrorNotImplemented
}

// ---------------------------------------------------------------------------
// Fluent setter methods (builder pattern)
// ---------------------------------------------------------------------------

// SetID sets the Telegram forum topic ID and returns the directory for
// chaining.
func (d *MTProtoDirectory) SetID(id string) *MTProtoDirectory {
	d.id = id
	return d
}

// SetItems sets the item count and returns the directory for chaining.
func (d *MTProtoDirectory) SetItems(items int64) *MTProtoDirectory {
	d.items = items
	return d
}

// SetParentID sets the parent directory ID and returns the directory for
// chaining.
func (d *MTProtoDirectory) SetParentID(parent string) *MTProtoDirectory {
	d.parentID = parent
	return d
}

// SetRemote sets the remote path and returns the directory for chaining.
func (d *MTProtoDirectory) SetRemote(remote string) *MTProtoDirectory {
	d.remote = remote
	return d
}

// SetSize sets the size and returns the directory for chaining.
func (d *MTProtoDirectory) SetSize(size int64) *MTProtoDirectory {
	d.size = size
	return d
}
