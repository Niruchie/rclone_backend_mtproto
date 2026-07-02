package configuration

import (
	"context"
	"strings"
	"sync"
	"time"

	mtproto "github.com/amarnathcjd/gogram/telegram"

	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/logging"
	"github.com/rclone/rclone/fs"
)

// ---------------------------------------------------------------------------
// Features
// ---------------------------------------------------------------------------

// NewMTProtoFeatures creates a new feature set for the backend.
func NewMTProtoFeatures(f *Filesystem) *fs.Features {
	return &fs.Features{
		CaseInsensitive:          false,
		DuplicateFiles:           false,
		ReadMimeType:             false, // TODO
		WriteMimeType:            false, // TODO
		CanHaveEmptyDirectories:  true,
		BucketBased:              true,
		BucketBasedRootOK:        true, // TODO: multichannel
		SetTier:                  false,
		GetTier:                  false,
		ServerSideAcrossConfigs:  false, // TODO
		IsLocal:                  false,
		SlowModTime:              true,
		SlowHash:                 true,
		ReadMetadata:             true,  // TODO
		WriteMetadata:            false, // TODO
		UserMetadata:             true,  // TODO
		ReadDirMetadata:          true,  // TODO
		WriteDirMetadata:         false, // TODO
		WriteDirSetModTime:       false,
		UserDirMetadata:          false, // TODO
		DirModTimeUpdatesOnWrite: false, // TODO
		FilterAware:              true,  // TODO
		PartialUploads:           false, // TODO
		NoMultiThreading:         false,
		Overlay:                  false,
		ChunkWriterDoesntSeek:    false,

		// Implemented methods
		About:        f.About,
		ChangeNotify: f.ChangeNotify,
		DirMove:      f.DirMove,
	}
}

// ---------------------------------------------------------------------------
// About
// ---------------------------------------------------------------------------

// About returns quota information (currently a placeholder).
//
// Read more about the field at [fs.Features.About]
//
// [fs.Features.About]: https://pkg.go.dev/github.com/rclone/rclone/fs#Features.About
func (f *Filesystem) About(_ context.Context) (*fs.Usage, error) {
	return &fs.Usage{}, nil
}

// ---------------------------------------------------------------------------
// DirMove
// ---------------------------------------------------------------------------

// DirMove moves a forum topic directory (server-side rename).
//
// Read more about the field at [fs.Features.DirMove]
//
// [fs.Features.DirMove]: https://pkg.go.dev/github.com/rclone/rclone/fs#Features.DirMove
func (f *Filesystem) DirMove(ctx context.Context, from fs.Fs, src string, dst string) error {
	destination := mtproto.ForumTopicObj{ID: 0, Title: dst}
	source := mtproto.ForumTopicObj{ID: 0, Title: src}

	// Verify destination doesn't exist.
	if list, err := f.GetTopics(ctx, destination); err == nil {
		for _, forumTopic := range list {
			if forumTopic.Title == destination.Title {
				return fs.ErrorDirExists
			}
		}
	} else {
		return fs.ErrorCantDirMove
	}

	// Find source topic.
	if list, err := f.GetTopics(ctx, source); err == nil {
		for _, forumTopic := range list {
			if forumTopic.Title == source.Title {
				source = forumTopic
			}
		}
	} else {
		return fs.ErrorCantDirMove
	}

	if source.ID == 0 {
		return fs.ErrorCantDirMove
	}

	// Update the title (server-side rename).
	source.Title = dst
	_, updated, err := f.UpdateTopic(ctx, source)
	switch {
	case err != nil:
		return fs.ErrorCantDirMove
	case !updated:
		return fs.ErrorCantDirMove
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// ChangeNotify
// ---------------------------------------------------------------------------

// ChangeNotify polls the supergroup for forum topic changes.
//
// Read more about the field at [fs.Features.ChangeNotify]
//
// [fs.Features.ChangeNotify]: https://pkg.go.dev/github.com/rclone/rclone/fs#Features.ChangeNotify
func (f *Filesystem) ChangeNotify(ctx context.Context, handleEntry func(string, fs.EntryType), intervals <-chan time.Duration) {
	handleChangeNotifications := func() {
		defer func() {
			if r := recover(); r != nil {
				fs.Errorf(logging.LoggerString(f), "panic in ChangeNotify: %v", r)
			}
		}()

		client, err := f.Client()
		if err != nil {
			fs.Errorf(logging.LoggerString(f), "error creating polling client, %s", err.Error())
			return
		}

		channel, err := client.GetChannel(f.SupergroupId)
		if err != nil {
			fs.Errorf(logging.LoggerString(f), "error fetching polling supergroup forum, %s", err.Error())
			return
		}

		input := &mtproto.InputChannelObj{
			AccessHash: channel.AccessHash,
			ChannelID:  channel.ID,
		}

		response, err := client.ChannelsGetFullChannel(input)
		if err != nil {
			fs.Errorf(logging.LoggerString(f), "error fetching polling supergroup forum, %s", err.Error())
			return
		}

		channelFull, ok := response.FullChat.(*mtproto.ChannelFull)
		if !ok {
			fs.Errorf(logging.LoggerString(f), "error fetching polling supergroup forum, not a channel")
			return
		}

		ticker := time.NewTicker(time.Minute)
		updates := make(chan time.Time, 1)
		ptsMutex := sync.Mutex{}
		pts := channelFull.Pts
		ticks := ticker.C

		for {
			select {
			case interval, ok := <-intervals:
				switch {
				case !ok:
					fs.Debugf(logging.LoggerString(f), "ticking interval not received")
				case ticker != nil:
					ticker.Stop()
					ticker, ticks = nil, nil
				case interval != 0:
					ticker = time.NewTicker(interval)
					updates <- time.Now()
					ticks = ticker.C
				}

			case <-ticks:
				if !ptsMutex.TryLock() {
					fs.Debugf(logging.LoggerString(f), "skipping tick, previous tick is still being processed")
					continue
				}
				next, err := f.polling(pts, updates, handleEntry)
				if err != nil {
					fs.Errorf(logging.LoggerString(f), "error polling updates, %s", err.Error())
				}
				pts = next
				ptsMutex.Unlock()

			case u := <-updates:
				until := time.Until(u)
				fs.Debugf(logging.LoggerString(f), "waiting for next poll at %v", u)
				ptsMutex.Lock()
				time.AfterFunc(until, func() {
					next, err := f.polling(pts, updates, handleEntry)
					if err != nil {
						fs.Errorf(logging.LoggerString(f), "error polling updates, %s", err.Error())
					}
					pts = next
				})
				ptsMutex.Unlock()

			case <-ctx.Done():
				fs.Debugf(logging.LoggerString(f), "shutting down change notification handler")
				return
			}
		}
	}

	go handleChangeNotifications()
}

// fetchUpdates gets channel differences and processes them.
func (f *Filesystem) fetchUpdates(ptsIn int32, handle func(string, fs.EntryType)) (next time.Time, update bool, pts int32, err error) {
	next, update, pts, err = time.Now(), false, ptsIn, nil

	fs.Debugf(logging.LoggerString(f), "invoking supergroup forum updates")

	client, err := f.Client()
	if err != nil {
		return next, update, pts, err
	}

	channel, err := client.GetChannel(f.SupergroupId)
	if err != nil {
		return next, update, pts, err
	}

	input := &mtproto.InputChannelObj{
		AccessHash: channel.AccessHash,
		ChannelID:  channel.ID,
	}

	var limited int32 = 50
	request := &mtproto.UpdatesGetChannelDifferenceParams{
		Filter:  &mtproto.ChannelMessagesFilterEmpty{},
		Limit:   limited,
		Channel: input,
		Pts:     ptsIn,
		Force:   true,
	}

	diff, err := client.UpdatesGetChannelDifference(request)
	if err != nil {
		return next, update, pts, err
	}

	redirectToHandler := func(messages []mtproto.Message, chats []mtproto.Chat, users []mtproto.User) {
		for _, msg := range messages {
			serviceMsg, ok := msg.(*mtproto.MessageService)
			if !ok {
				continue
			}

			switch serviceMsg.Action.(type) {
			case *mtproto.MessageActionTopicCreate, *mtproto.MessageActionTopicEdit:
				peer, err := client.GetPeerChannel(channel.ID)
				if err != nil {
					continue
				}
				forumTopics, err := client.MessagesGetForumTopicsByID(peer, []int32{serviceMsg.ID})
				if err != nil {
					continue
				}
				for _, ft := range forumTopics.Topics {
					forumTopic, ok := ft.(*mtproto.ForumTopicObj)
					if !ok {
						continue
					}
					if strings.HasPrefix(forumTopic.Title, f.Root()) {
						handle(forumTopic.Title, fs.EntryDirectory)
						fs.Debugf(logging.LoggerString(f), "detected new forum topic directory %q", forumTopic.Title)
					}
				}
			}
		}
	}

	switch diff := diff.(type) {
	case *mtproto.UpdatesChannelDifferenceEmpty:
		next = time.Now().Add(time.Duration(diff.Timeout) * time.Second)
		update = diff.Final && false
		pts = diff.Pts
		fs.Debugf(logging.LoggerString(f), "no difference, current pts=%d, next pts=%d, no timeout=%d, should update=%t", ptsIn, pts, diff.Timeout, update)

	case *mtproto.UpdatesChannelDifferenceObj:
		next = time.Now().Add(time.Duration(diff.Timeout) * time.Second)
		update = diff.Final
		pts = diff.Pts
		fs.Debugf(logging.LoggerString(f), "difference found (short), no re-sync, current pts=%d, next pts=%d, timeout=%d, should update=%t", pts, ptsIn, diff.Timeout, update)
		redirectToHandler(diff.NewMessages, diff.Chats, diff.Users)

	case *mtproto.UpdatesChannelDifferenceTooLong:
		next = time.Now().Add(time.Duration(diff.Timeout) * time.Second)
		update = diff.Final
		pts = ptsIn + limited
		fs.Debugf(logging.LoggerString(f), "difference found (long), need to re-sync, current pts=%d, next pts=%d, timeout=%d, should update=%t", ptsIn, pts, diff.Timeout, update)
		redirectToHandler(diff.Messages, diff.Chats, diff.Users)
	}

	return next, update, pts, err
}

// polling fetches updates and schedules the next poll if needed.
func (f *Filesystem) polling(pts int32, updates chan<- time.Time, handle func(string, fs.EntryType)) (int32, error) {
	if next, schedule, pts, err := f.fetchUpdates(pts, handle); err == nil {
		if schedule {
			select {
			case updates <- next:
				fs.Debugf(logging.LoggerString(f), "scheduling next poll at %v", next)
			default:
			}
		}
		return pts, nil
	} else {
		return pts, err
	}
}
