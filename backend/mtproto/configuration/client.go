package configuration

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"

	mtproto "github.com/amarnathcjd/gogram/telegram"

	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/clientpool"
	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/logging"
	"github.com/Niruchie/rclone_backend_mtproto/backend/mtproto/configuration/options"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
)

// ---------------------------------------------------------------------------
// MTProtoService
// ---------------------------------------------------------------------------

// MTProtoService is the high‑level wrapper around the user‑authenticated
// MTProto session. It embeds options.Options so that rclone's configuration
// system can fill its fields directly, and it delegates all client I/O to
// the global clientpool for thread safety, health checks, and reconnection.
//
// Architecture:
//
//	MTProtoService (configuration layer)
//	  └── *clientpool.PoolClient  (pointer — RWMutex, reconnect, health)
//	        └── *telegram.Client   (gogram raw client)
//	  └── clientpool.ClientPool   (global singleton — bot workers, command bus)
//
// Only one user client exists at any time. Bot workers are managed by the pool.
type MTProtoService struct {
	// PoolClient is stored as a pointer to avoid copying the embedded RWMutex.
	// It is set by Authorize() which looks up or creates the user client.
	*clientpool.PoolClient

	datacenter *mtproto.NearestDc
	cfg        *mtproto.Config
	appCfg     *mtproto.HelpAppConfigObj
	pcr        *pacer.Pacer
	startOnce  sync.Once

	options.Options // filled by rclone config system via configstruct.Set()
}

// NewMTProtoService creates an MTProtoService bound to the global client pool.
//
// The service is not connected until Authorize() is called.
func NewMTProtoService(ctx context.Context) *MTProtoService {
	pcr := pacer.New()
	pcr.SetMaxConnections(10)
	pcr.SetRetries(5)

	return &MTProtoService{
		pcr:    pcr,
		cfg:    &mtproto.Config{},
		appCfg: &mtproto.HelpAppConfigObj{},
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Authorize ensures the user client is connected and authenticated.
//
// If the pool already has a user client, it reuses it. Otherwise it creates
// a new one from the embedded Options and registers it in the pool.
//
// Returns the service itself for chaining.
func (s *MTProtoService) Authorize() (*MTProtoService, error) {
	pool := clientpool.Global()

	// If no user client in the pool yet, create and register one.
	if pool.GetUser() == nil {
		pc, err := clientpool.CreateUserClient(
			s.AppId,
			s.AppHash,
			s.PublicKey,
			s.StringSession,
			s.PhoneNumber,
			s.TestServer,
		)
		if err != nil {
			return nil, fmt.Errorf("create user client: %w", err)
		}
		s.PoolClient = pc
	} else {
		// Adopt the existing pool client (pointer, no copy).
		s.PoolClient = pool.GetUser()

		// If it's already connected, do a lightweight reconnect check.
		if s.PoolClient.IsConnected() {
			_ = s.PoolClient.Reconnect() // best effort
			return s, nil
		}

		if err := s.PoolClient.Connect(); err != nil {
			return nil, fmt.Errorf("reconnect user client: %w", err)
		}
	}

	// Start update listener once.
	s.startOnce.Do(func() {
		go s.handleUpdates()
	})
	_ = s.UpdateConfig()

	return s, nil
}

// Client returns the underlying *mtproto.Client, reconnecting if necessary.
func (s *MTProtoService) Client() (*mtproto.Client, error) {
	return s.PoolClient.Client()
}

// Pacer returns the rate limiter used by the filesystem layer.
func (s *MTProtoService) Pacer() *pacer.Pacer {
	return s.pcr
}

// ---------------------------------------------------------------------------
// Config updates
// ---------------------------------------------------------------------------

// UpdateConfig fetches the nearest DC, app config, and server config.
func (s *MTProtoService) UpdateConfig() error {
	client, err := s.Client()
	if err != nil {
		return err
	}

	dc, err := client.HelpGetNearestDc()
	if err == nil {
		s.datacenter = dc
	} else {
		return err
	}

	if appRaw, _ := client.HelpGetAppConfig(s.appCfg.Hash); appRaw != nil {
		if cfg, ok := appRaw.(*mtproto.HelpAppConfigObj); ok {
			s.appCfg = cfg
		}
	}

	cfg, err := client.HelpGetConfig()
	if err == nil {
		s.cfg = cfg
	} else {
		return err
	}

	return nil
}

// ---------------------------------------------------------------------------
// Update listener
// ---------------------------------------------------------------------------

func (s *MTProtoService) handleUpdates() {
	client, err := s.Client()
	if err != nil {
		fs.Errorf(logging.LoggerString(s), "update listener: %v", err)
		return
	}
	client.On(mtproto.OnRaw, s.onRawUpdate)
}

func (s *MTProtoService) onRawUpdate(u mtproto.Update, _ *mtproto.Client) error {
	switch u.(type) {
	case *mtproto.UpdateConfig:
		if err := s.UpdateConfig(); err != nil {
			fs.Error(logging.LoggerString(u), err.Error())
		}
	default:
		fs.Debugf(logging.LoggerString(u), "raw update: %T", u)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Channel / supergroup management
// ---------------------------------------------------------------------------

// CreateChannel creates a new supergroup with forum topics enabled.
func (s *MTProtoService) CreateChannel(_ context.Context, title string) (mtproto.Channel, bool, error) {
	var channel mtproto.Channel

	client, err := s.Client()
	if err != nil {
		return channel, false, err
	}

	raw, err := client.ChannelsCreateChannel(&mtproto.ChannelsCreateChannelParams{
		About:     title,
		Title:     title,
		ForImport: false,
		Megagroup: true,
		Forum:     true,
	})
	if err != nil {
		return channel, false, err
	}

	updates, ok := raw.(*mtproto.UpdatesObj)
	if !ok || len(updates.Chats) <= 0 {
		return channel, false, logging.ErrOperationWithoutUpdates
	}

	created := false
	if ch, ok := updates.Chats[0].(*mtproto.Channel); ok {
		channel = *ch
		created = true
	}

	return channel, created, nil
}

// GetTopics fetches forum topics matching the given title.
func (s *MTProtoService) GetTopics(_ context.Context, search mtproto.ForumTopicObj) ([]mtproto.ForumTopicObj, error) {
	client, err := s.Client()
	if err != nil {
		return nil, err
	}

	ch, err := client.GetChannel(s.SupergroupId)
	if err != nil {
		return nil, err
	}

	peer, err := client.GetPeerChannel(ch.ID)
	if err != nil {
		return nil, err
	}

	forum, err := client.MessagesGetForumTopics(&mtproto.MessagesGetForumTopicsParams{
		Q:     search.Title,
		Limit: math.MaxInt32,
		Peer:  peer,
	})
	if err != nil {
		return nil, err
	}

	topics := make([]mtproto.ForumTopicObj, 0, len(forum.Topics))
	for i := range forum.Topics {
		if t, ok := forum.Topics[i].(*mtproto.ForumTopicObj); ok {
			topics = append(topics, *t)
		}
	}
	return topics, nil
}

// CreateTopic creates a forum topic, returning the existing one if it exists.
func (s *MTProtoService) CreateTopic(ctx context.Context, topicIn mtproto.ForumTopicObj) (mtproto.ForumTopicObj, bool, error) {
	topics, err := s.GetTopics(ctx, topicIn)
	if err != nil {
		return topicIn, false, err
	}
	for _, t := range topics {
		if t.Title == topicIn.Title {
			return t, false, nil
		}
	}

	client, err := s.Client()
	if err != nil {
		return topicIn, false, err
	}

	ch, err := client.GetChannel(s.SupergroupId)
	if err != nil {
		return topicIn, false, err
	}

	peer, err := client.GetPeerChannel(ch.ID)
	if err != nil {
		return topicIn, false, err
	}

	response, err := client.MessagesCreateForumTopic(&mtproto.MessagesCreateForumTopicParams{
		Title:    topicIn.Title,
		RandomID: rand.Int63(),
		Peer:     peer,
	})
	if err != nil {
		return topicIn, false, err
	}

	updates, ok := response.(*mtproto.UpdatesObj)
	if !ok || len(updates.Updates) <= 0 {
		return topicIn, false, logging.ErrOperationWithoutUpdates
	}

	return topicIn, true, nil
}

// ---------------------------------------------------------------------------
// Pool helpers (convenience for external modules)
// ---------------------------------------------------------------------------

// Pool returns the global client pool singleton.
func Pool() *clientpool.ClientPool { return clientpool.Global() }

// GetBots returns all registered bot workers.
func GetBots() []*clientpool.PoolClient { return clientpool.Global().GetBots() }

// BotCount returns the number of registered bot workers.
func BotCount() int { return len(clientpool.Global().GetBots()) }

// GetBotByIndex returns the nth bot worker, or nil.
func GetBotByIndex(idx int) *clientpool.PoolClient {
	return clientpool.Global().GetByIndex(idx)
}

// SendCommand dispatches a command to a specific bot (or all if targetID is "").
func SendCommand(ctx context.Context, targetID, name string, payload any) []*clientpool.CommandResult {
	cmd := &clientpool.Command{Name: name, Payload: payload, Source: "mtproto-service"}
	if targetID == "" {
		return clientpool.Global().BroadcastToBots(ctx, cmd)
	}
	return clientpool.Global().SendCommandToBot(ctx, targetID, cmd)
}

// SubscribeCommand registers a handler for a command pattern on the global bus.
func SubscribeCommand(pattern string, handler clientpool.CommandHandler) string {
	return clientpool.GlobalCommandBus().Subscribe(pattern, handler)
}

// BootstrapManagers registers all manager bot tokens from the configuration
// as bot workers in the global pool.
func BootstrapManagers(appID int32, appHash, publicKey string, testServer bool, tokens []string) error {
	_, err := clientpool.ParseManagers(appID, appHash, publicKey, testServer, tokens)
	return err
}
