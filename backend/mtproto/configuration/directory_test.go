// Test configuration directory methods
package configuration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// trimPrefix
// ---------------------------------------------------------------------------

func TestTrimPrefix(t *testing.T) {
	tests := []struct {
		s      string
		prefix string
		want   string
	}{
		{"hello/world", "hello", "world"},
		{"hello/world", "hello/", "world"},
		{"hello/world", "foo", "hello/world"},
		{"hello/world", "", "hello/world"},
		{"", "prefix", ""},
		{"/foo/bar", "/foo", "bar"},
		{"foo/bar", "foo/", "bar"},
		{"same", "same", ""},
	}
	for _, tt := range tests {
		got := trimPrefix(tt.s, tt.prefix)
		assert.Equal(t, tt.want, got, "trimPrefix(%q, %q)", tt.s, tt.prefix)
	}
}

// ---------------------------------------------------------------------------
// MTProtoDirectory construction
// ---------------------------------------------------------------------------

func TestNewMTProtoDirectory(t *testing.T) {
	now := time.Now()
	d := NewMTProtoDirectory(nil, "some/remote/path", now)
	require.NotNil(t, d)
	assert.Equal(t, "some/remote/path", d.Remote())
	assert.Equal(t, now, d.ModTime(context.Background()))
	assert.Equal(t, int64(-1), d.Size())
	assert.Equal(t, int64(-1), d.Items())
	assert.Equal(t, "", d.ID())
	assert.Equal(t, "", d.ParentID())
}

func TestNewMTProtoDirectoryFromTopic(t *testing.T) {
	now := time.Now()
	d := NewMTProtoDirectoryFromTopic(nil, "prefix/mydir", now, 42, "prefix")
	require.NotNil(t, d)
	assert.Equal(t, "mydir", d.Remote())
	assert.Equal(t, now, d.ModTime(context.Background()))
	assert.Equal(t, int64(-1), d.Size())
	assert.Equal(t, int64(-1), d.Items())
	assert.Equal(t, "42", d.ID())
}

func TestNewMTProtoDirectoryFromTopic_NoPrefix(t *testing.T) {
	now := time.Now()
	d := NewMTProtoDirectoryFromTopic(nil, "justadir", now, 7, "")
	require.NotNil(t, d)
	assert.Equal(t, "justadir", d.Remote())
	assert.Equal(t, "7", d.ID())
}

// ---------------------------------------------------------------------------
// fs.DirEntry implementation
// ---------------------------------------------------------------------------

func TestMTProtoDirectory_Fs(t *testing.T) {
	d := &MTProtoDirectory{fs: nil}
	assert.Nil(t, d.Fs())

	// With a filesystem reference.
	f := &Filesystem{name: "test"}
	d2 := NewMTProtoDirectory(f, "path", time.Now())
	assert.Equal(t, f, d2.Fs())
}

func TestMTProtoDirectory_String(t *testing.T) {
	d := &MTProtoDirectory{remote: "my/dir"}
	assert.Equal(t, "my/dir", d.String())
}

func TestMTProtoDirectory_Remote(t *testing.T) {
	d := &MTProtoDirectory{remote: "a/b/c"}
	assert.Equal(t, "a/b/c", d.Remote())
}

func TestMTProtoDirectory_ModTime(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	d := &MTProtoDirectory{modTime: now}
	assert.Equal(t, now, d.ModTime(context.Background()))
}

func TestMTProtoDirectory_Size(t *testing.T) {
	d := &MTProtoDirectory{size: 12345}
	assert.Equal(t, int64(12345), d.Size())

	d2 := &MTProtoDirectory{size: -1}
	assert.Equal(t, int64(-1), d2.Size())
}

// ---------------------------------------------------------------------------
// fs.Directory implementation
// ---------------------------------------------------------------------------

func TestMTProtoDirectory_Items(t *testing.T) {
	d := &MTProtoDirectory{items: 42}
	assert.Equal(t, int64(42), d.Items())

	d2 := &MTProtoDirectory{items: -1}
	assert.Equal(t, int64(-1), d2.Items())
}

func TestMTProtoDirectory_ID(t *testing.T) {
	d := &MTProtoDirectory{id: "12345"}
	assert.Equal(t, "12345", d.ID())

	d2 := &MTProtoDirectory{}
	assert.Equal(t, "", d2.ID())
}

// ---------------------------------------------------------------------------
// fs.FullDirectory — optional interfaces
// ---------------------------------------------------------------------------

func TestMTProtoDirectory_ParentID(t *testing.T) {
	d := &MTProtoDirectory{parentID: "99"}
	assert.Equal(t, "99", d.ParentID())

	d2 := &MTProtoDirectory{}
	assert.Equal(t, "", d2.ParentID())
}

func TestMTProtoDirectory_SetModTime(t *testing.T) {
	d := &MTProtoDirectory{}
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	err := d.SetModTime(context.Background(), now)
	assert.NoError(t, err)
	assert.Equal(t, now, d.ModTime(context.Background()))
}

func TestMTProtoDirectory_Metadata(t *testing.T) {
	d := &MTProtoDirectory{}
	meta, err := d.Metadata(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, meta)
}

func TestMTProtoDirectory_SetMetadata(t *testing.T) {
	d := &MTProtoDirectory{}
	err := d.SetMetadata(context.Background(), fs.Metadata{})
	assert.ErrorIs(t, err, fs.ErrorNotImplemented)
}

// ---------------------------------------------------------------------------
// Fluent setter methods (builder pattern)
// ---------------------------------------------------------------------------

func TestMTProtoDirectory_SetID(t *testing.T) {
	d := &MTProtoDirectory{}
	ret := d.SetID("789")
	assert.Same(t, d, ret, "should return itself for chaining")
	assert.Equal(t, "789", d.ID())
}

func TestMTProtoDirectory_SetItems(t *testing.T) {
	d := &MTProtoDirectory{}
	ret := d.SetItems(10)
	assert.Same(t, d, ret)
	assert.Equal(t, int64(10), d.Items())
}

func TestMTProtoDirectory_SetParentID(t *testing.T) {
	d := &MTProtoDirectory{}
	ret := d.SetParentID("parent-1")
	assert.Same(t, d, ret)
	assert.Equal(t, "parent-1", d.ParentID())
}

func TestMTProtoDirectory_SetRemote(t *testing.T) {
	d := &MTProtoDirectory{}
	ret := d.SetRemote("new/remote/path")
	assert.Same(t, d, ret)
	assert.Equal(t, "new/remote/path", d.Remote())
}

func TestMTProtoDirectory_SetSize(t *testing.T) {
	d := &MTProtoDirectory{}
	ret := d.SetSize(2048)
	assert.Same(t, d, ret)
	assert.Equal(t, int64(2048), d.Size())
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time checks in tests)
// ---------------------------------------------------------------------------

func TestMTProtoDirectory_ImplementsDirectory(t *testing.T) {
	d := &MTProtoDirectory{}
	assert.Implements(t, (*fs.Directory)(nil), d)
}

func TestMTProtoDirectory_ImplementsFullDirectory(t *testing.T) {
	d := &MTProtoDirectory{}
	assert.Implements(t, (*fs.FullDirectory)(nil), d)
}

// ---------------------------------------------------------------------------
// Chaining example
// ---------------------------------------------------------------------------

func TestMTProtoDirectory_BuilderPattern(t *testing.T) {
	now := time.Now()
	d := NewMTProtoDirectory(nil, "base", now).
		SetID("42").
		SetItems(5).
		SetSize(100).
		SetParentID("1").
		SetRemote("base/child")

	assert.Equal(t, "base/child", d.Remote())
	assert.Equal(t, "42", d.ID())
	assert.Equal(t, int64(5), d.Items())
	assert.Equal(t, int64(100), d.Size())
	assert.Equal(t, "1", d.ParentID())
	assert.Equal(t, now, d.ModTime(context.Background()))

	// Verify String() returns the latest remote.
	assert.Equal(t, "base/child", d.String())
}

func TestMTProtoDirectory_StringFormats(t *testing.T) {
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	d := NewMTProtoDirectory(nil, "path/to/dir", now)
	assert.Equal(t, "path/to/dir", fmt.Sprint(d))
	assert.Equal(t, "path/to/dir", d.String())
}
