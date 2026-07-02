// Test configuration filesystem methods
package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// clean / slash
// ---------------------------------------------------------------------------

func TestClean(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/", "/"},
		{"", "/"},
		{"/foo", "/foo"},
		{"foo/", "/foo"},
		{"/foo/", "/foo"},
		{"//foo///bar//", "/foo/bar"},
		{"a/b/c", "/a/b/c"},
		{"/a/b/c/", "/a/b/c"},
	}
	for _, tt := range tests {
		got := clean(tt.input)
		assert.Equal(t, tt.want, got, "clean(%q)", tt.input)
	}
}

func TestSlash(t *testing.T) {
	tests := []struct {
		op    SlashOpCode
		input string
		want  string
	}{
		{UNTRAIL, "/", "/"},
		{UNTRAIL, "/foo/", "/foo"},
		{UNTRAIL, "foo/", "foo"},
		{UNTRAIL, "foo", "foo"},
		{UNLEAD, "/", "/"},
		{UNLEAD, "/foo", "foo"},
		{UNLEAD, "foo", "foo"},
		{UNLEAD, "/a/b", "a/b"},
		{TRAIL, "foo", "foo/"},
		{TRAIL, "foo/", "foo/"},
		{TRAIL, "/", "/"},
		{LEAD, "foo", "/foo"},
		{LEAD, "/foo", "/foo"},
		{LEAD, "a/b", "/a/b"},
		{LEAD, "/", "/"},
	}
	for _, tt := range tests {
		got := slash(tt.op, tt.input)
		assert.Equal(t, tt.want, got, "slash(%q, %q)", tt.op, tt.input)
	}
}

// ---------------------------------------------------------------------------
// ManagerWithLock
// ---------------------------------------------------------------------------

func TestManagerWithLock(t *testing.T) {
	m := ManagerWithLock{Token: "123456:ABCdef"}
	assert.Equal(t, "123456:ABCdef", m.Token)
}

// ---------------------------------------------------------------------------
// Filesystem — basic info methods (no network needed)
// ---------------------------------------------------------------------------

// testFs is a minimal Filesystem with no MTProto connection.
// Only fields used by the methods under test are populated.
func testFs(t *testing.T) *Filesystem {
	return &Filesystem{
		name: "mtproto-test",
		root: "testroot",
	}
}

func TestFilesystem_Name(t *testing.T) {
	f := testFs(t)
	assert.Equal(t, "mtproto-test", f.Name())
}

func TestFilesystem_Root(t *testing.T) {
	tests := []struct {
		root string
		want string
	}{
		{"testroot", "/testroot"},
		{"/testroot/", "/testroot"},
		{"", "/"},
		{"/", "/"},
		{"foo/bar", "/foo/bar"},
		{"//foo///bar//", "/foo/bar"},
	}
	for _, tt := range tests {
		f := &Filesystem{name: "x", root: tt.root}
		got := f.Root()
		assert.Equal(t, tt.want, got, "Root() with root=%q", tt.root)
	}
}

func TestFilesystem_Hashes(t *testing.T) {
	// Hashes returns an empty set when no hash is registered.
	f := &Filesystem{}
	hs := f.Hashes()
	assert.NotNil(t, hs)
	// With no hash registered the set is empty.
	assert.Equal(t, 0, hs.Count(), "hash set should be empty")
}

func TestFilesystem_String(t *testing.T) {
	f := &Filesystem{name: "test", root: "mnt"}
	s := f.String()
	assert.Contains(t, s, "test")
	assert.Contains(t, s, "mnt")
	assert.Contains(t, s, "MTProto")
}

func TestFilesystem_Precision(t *testing.T) {
	// Precision requires a connected MTProto client.
	// Without one, the method panics because PoolClient is nil.
	// This test verifies the error path is reachable once a client is available.
	// For now we skip — integration tests cover this with a real connection.
	t.Skip("Precision needs a real MTProto client; covered by integration tests")
}

func TestFilesystem_Features(t *testing.T) {
	f := testFs(t)
	feat := f.Features()
	assert.NotNil(t, feat)
	// Spot-check a few known feature values.
	assert.True(t, feat.CanHaveEmptyDirectories)
	assert.True(t, feat.BucketBased)
	assert.False(t, feat.CaseInsensitive)
	assert.False(t, feat.DuplicateFiles)
}

// ---------------------------------------------------------------------------
// locate (path resolution with traversal protection)
// ---------------------------------------------------------------------------

func TestFilesystem_Locate(t *testing.T) {
	tests := []struct {
		root     string
		relative string
		wantRoot string
		wantQ    string
	}{
		{"/", "foo", "/", "/foo"},
		{"/", "/foo", "/", "/foo"},
		{"base", "", "/base", "/base"},
		{"base", "sub", "/base", "/base/sub"},
		{"base", "/sub", "/base", "/base/sub"},
		{"base", "a/b/c", "/base", "/base/a/b/c"},
	}
	for _, tt := range tests {
		f := &Filesystem{root: tt.root}
		gotRoot, gotQ := f.locate(tt.relative)
		assert.Equal(t, tt.wantRoot, gotRoot, "locate(%q) root — root=%q", tt.relative, tt.root)
		assert.Equal(t, tt.wantQ, gotQ, "locate(%q) query — root=%q", tt.relative, tt.root)
	}
}

func TestFilesystem_Locate_PathTraversal(t *testing.T) {
	// When root is "/", traversal is impossible so we expect normal resolution.
	f := &Filesystem{root: "/"}
	root, q := f.locate("../etc")
	assert.Equal(t, "/", root)
	assert.Equal(t, "/etc", q) // path.Join cleans "/../etc" → "/etc"

	// When root is non-"/", a traversal attempt should return root as both values.
	f2 := &Filesystem{root: "safe"}
	root2, q2 := f2.locate("../../etc")
	assert.Equal(t, "/safe", root2, "root should fall back to safe root on traversal")
	assert.Equal(t, "/safe", q2, "query should fall back to safe root on traversal")
}
