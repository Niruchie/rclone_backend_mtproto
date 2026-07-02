package configuration

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/hashing"
	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/logging"
	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/options"
	"github.com/amarnathcjd/gogram/telegram"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
)

// ManagerWithLock pairs a manager token with its metadata.
type ManagerWithLock struct {
	Token string
}

// Filesystem with its properties.
type Filesystem struct {
	managers []ManagerWithLock
	hash     hash.Type
	name     string
	root     string
	MTProtoService
	fs.Fs
}

// Fs creates a new Filesystem instance with the specified parameters and required properties.
func Fs(ctx context.Context, name string, root string, m configmap.Mapper) (fs.Fs, error) {
	var err error = nil
	// ? Create a new Filesystem instance
	f := &Filesystem{
		MTProtoService: *NewMTProtoService(ctx),
	}

	// ? Parse the config into the struct
	err = configstruct.Set(m, &(f.Options))
	if err != nil {
		return nil, err
	}

	// ? Register the hash types for the filesystem.
	size := hashing.NewMTProtoMultipartHasher().Size()
	registeredType := hash.RegisterHash("telegramhashmulti", "TelegramMultipartHash", size, hashing.NewMTProtoMultipartHasher)

	managers := []ManagerWithLock{}
	// ? Register the filesystem managers
	for _, token := range f.Managers {
		managers = append(managers, ManagerWithLock{
			Token: token,
		})
	}

	// ? Set up Filesystem instance
	f.hash = registeredType
	f.managers = managers
	f.root = root
	f.name = name

	// Authorize the client into MTProto API.
	mtproto, err := f.Authorize()
	if err != nil {
		return nil, err
	}

	// Fetch the client instance.
	client, err := mtproto.Client()
	if err != nil {
		return nil, err
	}

	// Cache the channel into memory cache, ignore
	_, err = client.GetChannel(f.SupergroupId)
	if err != nil {
		return nil, err
	}

	// Create the forum topic of root
	return f, f.Mkdir(ctx, "/")
}

// ? ----- Interface fs.Info -----

// Features returns the optional features of this Fs.
//
// Read more about the method at [fs.Info.Features]
//
// [fs.Info.Features]: https://pkg.go.dev/github.com/rclone/rclone/fs#Info.Features
func (f *Filesystem) Features() *fs.Features {
	return NewMTProtoFeatures(f)
}

// Name returns the remote name as passed into NewFs.
//
// Read more about the method at [fs.Info.Name]
//
// [fs.Info.Name]: https://pkg.go.dev/github.com/rclone/rclone/fs#Info.Name
func (f *Filesystem) Name() string {
	return f.name
}

// Root returns the remote root as passed into NewFs.
//
// Read more about the method at [fs.Info.Root]
//
// [fs.Info.Root]: https://pkg.go.dev/github.com/rclone/rclone/fs#Info.Root
func (f *Filesystem) Root() string {
	root := clean(f.root)
	return root
}

// Hashes returns the supported hash types of the filesystem.
//
// Read more about the method at [fs.Info.Hashes]
//
// [fs.Info.Hashes]: https://pkg.go.dev/github.com/rclone/rclone/fs#Info.Hashes
func (f *Filesystem) Hashes() hash.Set {
	return hash.Set(f.hash)
}

// String returns a description of the filesystem.
//
// Read more about the method at [fs.Info.String]
//
// [fs.Info.String]: https://pkg.go.dev/github.com/rclone/rclone/fs#Info.String
func (f *Filesystem) String() string {
	return fmt.Sprintf("MTProto backend mounted at: %s:%s", f.name, f.root)
}

// Precision returns the precision of the ModTimes in this filesystem.
//
// Read more about the method at [fs.Info.Precision]
//
// [fs.Info.Precision]: https://pkg.go.dev/github.com/rclone/rclone/fs#Info.Precision
func (f *Filesystem) Precision() time.Duration {
	mtproto, err := f.Client()
	if err != nil {
		return time.Second
	}

	return mtproto.Ping()
}

// ----- Interface fs.Fs -----

// List the objects and directories in dir into entries.
//
// Read more about the method at [Fs.List]
//
// [Fs.List]: https://pkg.go.dev/github.com/rclone/rclone/fs#Fs.List
func (f *Filesystem) List(ctx context.Context, relative string) (entries fs.DirEntries, err error) {
	// Get locate query for entry.
	root, query := f.locate(relative)
	topics, err := f.GetTopics(ctx, telegram.ForumTopicObj{Title: query})
	if err != nil {
		log := "forum topic directories issue, %s"
		fs.Errorf(logging.LoggerString(f), log, err.Error())
		return entries, fs.ErrorDirNotFound
	}

	for _, topic := range topics {
		trail := slash(TRAIL, root)
		if !strings.HasPrefix(topic.Title, query) || topic.Title == query {
			continue
		}

		name := strings.TrimPrefix(topic.Title, trail)
		date := time.Unix(int64(topic.Date), 0)

		// directory attributes
		// when unknown size, set to -1
		entry := NewMTProtoDirectory(f, name, date)
		entry.SetSize(-1)
		entry.SetID(fmt.Sprintf("%d", topic.ID))

		entries = append(entries, entry)
	}

	// TODO: place holder code here
	// * each object message add a directory entry
	// for _, o := range objects {
	// 	entries = append(entries, o)
	// }

	return entries, nil
}

// Mkdir makes the directory.
// Shouldn't return an error if it already exists.
//
// Read more about the method at [Fs.Mkdir]
//
// [Fs.Mkdir]: https://pkg.go.dev/github.com/rclone/rclone/fs#Fs.Mkdir
func (f *Filesystem) Mkdir(ctx context.Context, relative string) error {
	// Get locate query for entry.
	_, query := f.locate(relative)

	// Create the topic on the channel.
	log := "forum topic directory %s being created"
	fs.Infof(logging.LoggerString(f), log, query)

	_, created, err := f.CreateTopic(ctx, telegram.ForumTopicObj{Title: query})
	switch {
	case err == nil && !created:
		log := "forum topic directory %s already exists"
		fs.Infof(logging.LoggerString(f), log, query)
		return nil
	case err == nil:
		log := "forum topic directory %s created"
		fs.Infof(logging.LoggerString(f), log, query)
		return nil
	default:
		log := "forum topic directory %s error, %s"
		fs.Errorf(logging.LoggerString(f), log, query, err.Error())
		return fs.ErrorDirNotFound
	}
}

// Rmdir removes the directory if empty.
// Return an error if it doesn't exist or isn't empty.
//
// Read more about the method at [Fs.Rmdir]
//
// [Fs.Rmdir]: https://pkg.go.dev/github.com/rclone/rclone/fs#Fs.Rmdir
func (f *Filesystem) Rmdir(ctx context.Context, relative string) error {
	// Get locate query for entry.
	_, query := f.locate(relative)

	// Searching the directory (forum topic) to delete
	// And its child directories (forum topics)
	directories, err := f.GetTopics(ctx, telegram.ForumTopicObj{Title: query})
	if err != nil {
		log := "forum topic directories issue, %s, %s"
		fs.Errorf(logging.LoggerString(f), log, query, err.Error())
		return fs.ErrorNotDeletingDirs
	}

	directory := telegram.ForumTopicObj{ID: 0}
	for _, dir := range directories {
		switch {
		// Must not delete, if not empty
		case dir.Title != query && strings.HasPrefix(dir.Title, query):
			log := "error deleting forum topic directory: %s, not empty"
			fs.Errorf(logging.LoggerString(f), log, query)
			return fs.ErrorDirectoryNotEmpty

		// Channel root topic must not be deleted
		case dir.Title == query && dir.ID == options.ChannelRootTopicId:
			log := "error deleting forum topic directory: %s, is root topic"
			fs.Errorf(logging.LoggerString(f), log, query)
			return fs.ErrorNotDeletingDirs

		// Must not break on this case,
		// Should check for child directories
		case dir.Title == query:
			directory = dir
		}
	}

	if directory.ID == 0 {
		log := "error deleting forum topic directory: %s, not found"
		fs.Errorf(logging.LoggerString(f), log, query)
		return fs.ErrorDirNotFound
	}

	// TODO: placeholder code here
	// * check for child directories
	// if 0 < items {
	// 	log := "error deleting forum topic directory: %s, not empty"
	// 	fs.Errorf(logging.LoggerString(f), log, query)
	// 	return fs.ErrorDirectoryNotEmpty
	// }

	// Delete the forum topic if it's empty.
	err = f.DeleteTopic(ctx, directory)
	if err != nil {
		log := "error deleting forum topic directory: %s, %s"
		fs.Errorf(logging.LoggerString(f), log, query, err.Error())
		return fs.ErrorNotDeletingDirs
	}

	return nil
}

// NewObject finds the Object at remote.
//
// Read more about the method at [Fs.NewObject]
//
// [Fs.NewObject]: https://pkg.go.dev/github.com/rclone/rclone/fs#Fs.NewObject
func (f *Filesystem) NewObject(ctx context.Context, relative string) (fs.Object, error) {
	// Get locate query for entry.
	_, query := f.locate(relative)
	directoryName := path.Dir(query)

	log := "invoked search for %s, on directory %s"
	fs.Infof(logging.LoggerString(f), log, query, directoryName)

	channels, err := f.GetTopics(ctx, telegram.ForumTopicObj{Title: directoryName})
	if err != nil {
		return nil, fs.ErrorDirNotFound
	}

	var directory telegram.ForumTopicObj
	for _, ch := range channels {
		switch {
		case ch.Title == query:
			return nil, fs.ErrorIsDir
		case ch.Title == directoryName:
			directory = ch
		}
	}

	// TODO: placeholder code here
	return f.search(ctx, directory, query)
}

// Put in to the remote path with the modTime given of the given size
//
// Read more about the method at [Fs.Put]
//
// [Fs.Put]: https://pkg.go.dev/github.com/rclone/rclone/fs#Fs.Put
func (f *Filesystem) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	log := "invoked overwrite of %s, hex size %d, modtime %v"
	fs.Infof(logging.LoggerString(f), log, src.Remote(), src.Size(), src.ModTime(ctx))

	// TODO: placeholder code here
	o := TempObject{} // o := NewObjectFromRelative(f, src.Remote())
	return &o, o.Update(ctx, in, src, options...)
}

// ? ----- Path cleaning operations -----

// SlashOpCode represents the operation to perform on slashes.
type SlashOpCode string

const (
	UNTRAIL SlashOpCode = "untrail"
	UNLEAD  SlashOpCode = "unlead"
	TRAIL   SlashOpCode = "trail"
	LEAD    SlashOpCode = "lead"
)

// clean cleans the input path by removing leading and trailing slashes.
//
// Definition:
//
//	clean(input string) string
//
// Parameters:
//
//	input string - The input path to clean.
//
// Returns:
//
//	string - The cleaned path.
func clean(input string) string {
	input = slash(LEAD, input)
	input = path.Clean(input)
	return slash(UNTRAIL, input)
}

// slash modifies the input path by adding or removing leading and trailing slashes.
//
// Parameters:
//
//	op SlashOpCode - The operation to perform (add/remove leading/trailing slash).
//	input string - The input path to modify.
//
// Returns:
//
//	string - The modified path.
func slash(op SlashOpCode, input string) string {
	switch op {
	case UNTRAIL:
		if input == "/" {
			return input
		}
		return strings.TrimSuffix(input, "/")
	case UNLEAD:
		if input == "/" {
			return input
		}
		return strings.TrimPrefix(input, "/")
	case TRAIL:
		if !strings.HasSuffix(input, "/") {
			return input + "/"
		}
		return input
	case LEAD:
		return path.Join("/", input)
	default:
		return input
	}
}

// locate resolves a relative path into the root directory and the absolute query string.
//
// It validates that the resolved path stays within the filesystem root to prevent
// path traversal attacks.
//
// Parameters:
//
//	relative - The relative path to search for the entry.
//
// Returns:
//
//	root  - The root path of the filesystem.
//	query - The absolute query path.
func (f *Filesystem) locate(relative string) (string, string) {
	root := f.Root()
	absolute := path.Join(root, relative)
	query := slash(UNTRAIL, absolute)

	// Security: ensure the resolved path does not escape the root directory.
	// When root is "/" (filesystem root), all absolute paths are valid.
	if root != "/" && !strings.HasPrefix(absolute+"/", root+"/") {
		log := "path traversal detected: root=%v, relative=%v, resolved=%v"
		fs.Errorf(logging.LoggerString(f), log, root, relative, absolute)
		return root, root
	}

	log := "locate query for entry, root=%v, absolute=%v, relative=%v, query=%v"
	fs.Debugf(logging.LoggerString(f), log, root, absolute, relative, query)
	return root, query
}

// search searches for an object message within a forum topic directory.
//
// It is a helper for [Filesystem.NewObject] and iterates through messages
// inside the given forum topic to find one matching the query.
//
// Parameters:
//
//	_ context.Context - The context for the request.
//	directory telegram.ForumTopicObj - The forum topic directory to search within.
//	query string - The query string to search for.
func (f *Filesystem) search(_ context.Context, directory telegram.ForumTopicObj, query string) (object fs.Object, err error) {
	// TODO: placeholder code here
	_, _ = directory, query
	return &TempObject{}, nil
}

// TempObject is a temporary placeholder for rclone objects.
//
// TODO: Replace with the actual object implementation once the
// message-storage layer is complete.
type TempObject struct {
	_ telegram.MessageObj
	fs.Object
}
