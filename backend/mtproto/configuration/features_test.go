// Test configuration features methods
package configuration

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// NewMTProtoFeatures
// ---------------------------------------------------------------------------

func TestNewMTProtoFeatures(t *testing.T) {
	f := &Filesystem{}
	feat := NewMTProtoFeatures(f)
	assert.NotNil(t, feat)

	// Verify it returns a properly constructed features struct.
	assert.True(t, feat.CanHaveEmptyDirectories)
	assert.False(t, feat.CaseInsensitive)
}

func TestNewMTProtoFeatures_Values(t *testing.T) {
	f := &Filesystem{}
	feat := NewMTProtoFeatures(f)

	// Static feature flags.
	assert.False(t, feat.CaseInsensitive)
	assert.False(t, feat.DuplicateFiles)
	assert.False(t, feat.ReadMimeType)
	assert.False(t, feat.WriteMimeType)
	assert.True(t, feat.CanHaveEmptyDirectories)
	assert.True(t, feat.BucketBased)
	assert.True(t, feat.BucketBasedRootOK)
	assert.False(t, feat.SetTier)
	assert.False(t, feat.GetTier)
	assert.False(t, feat.ServerSideAcrossConfigs)
	assert.False(t, feat.IsLocal)
	assert.True(t, feat.SlowModTime)
	assert.True(t, feat.SlowHash)
	assert.True(t, feat.ReadMetadata)
	assert.False(t, feat.WriteMetadata)
	assert.True(t, feat.UserMetadata)
	assert.True(t, feat.ReadDirMetadata)
	assert.False(t, feat.WriteDirMetadata)
	assert.False(t, feat.WriteDirSetModTime)
	assert.False(t, feat.UserDirMetadata)
	assert.False(t, feat.DirModTimeUpdatesOnWrite)
	assert.True(t, feat.FilterAware)
	assert.False(t, feat.PartialUploads)
	assert.False(t, feat.NoMultiThreading)
	assert.False(t, feat.Overlay)
	assert.False(t, feat.ChunkWriterDoesntSeek)
}

func TestNewMTProtoFeatures_Callbacks(t *testing.T) {
	f := &Filesystem{}
	feat := NewMTProtoFeatures(f)

	// Callback fields should be non-nil.
	assert.NotNil(t, feat.About, "About callback should be set")
	assert.NotNil(t, feat.ChangeNotify, "ChangeNotify callback should be set")
	assert.NotNil(t, feat.DirMove, "DirMove callback should be set")
}

func TestNewMTProtoFeatures_AboutCallback(t *testing.T) {
	f := &Filesystem{}
	feat := NewMTProtoFeatures(f)

	usage, err := feat.About(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, usage)
	// About currently returns an empty Usage struct.
	assert.Equal(t, &fs.Usage{}, usage)
}

// ---------------------------------------------------------------------------
// Feature coupling — Features() must return the same instance
// ---------------------------------------------------------------------------

func TestFilesystem_Features_Consistency(t *testing.T) {
	f := &Filesystem{}
	feat1 := f.Features()
	feat2 := f.Features()
	// Each call returns a fresh *fs.Features (no caching currently).
	assert.NotSame(t, feat1, feat2, "each Features() call returns a new instance")
	// Spot-check that boolean fields are consistent between calls.
	assert.Equal(t, feat1.CaseInsensitive, feat2.CaseInsensitive)
	assert.Equal(t, feat1.CanHaveEmptyDirectories, feat2.CanHaveEmptyDirectories)
	assert.Equal(t, feat1.BucketBased, feat2.BucketBased)
}
