// Package clientpool provides a global, thread-safe pool for managing MTProto
// clients and bot workers. It offers a singleton registry that allows any
// module to discover, create, and communicate with MTProto clients
// dynamically.
//
// Architecture:
//
//	ClientPool (singleton, RWMutex-protected)
//	  ├── PoolClient (User)      — main authenticated user session
//	  ├── PoolClient (Bot #1)    — manager bot worker
//	  ├── PoolClient (Bot #N)    — additional bot workers
//	  └── CommandBus             — publish/subscribe command dispatch
package clientpool

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mtproto "github.com/amarnathcjd/gogram/telegram"
	"github.com/rclone/rclone/fs"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// ClientType indicates whether a pooled client is a user session or a bot.
type ClientType int

const (
	// ClientTypeUser represents a user-authenticated MTProto session (phone).
	ClientTypeUser ClientType = iota
	// ClientTypeBot represents a bot-authenticated MTProto session (token).
	ClientTypeBot
)

func (t ClientType) String() string {
	switch t {
	case ClientTypeUser:
		return "user"
	case ClientTypeBot:
		return "bot"
	default:
		return "unknown"
	}
}

// ClientStatus represents the current connection state of a pooled client.
type ClientStatus int

const (
	// StatusDisconnected means the client is not connected.
	StatusDisconnected ClientStatus = iota
	// StatusConnected means the client is connected and healthy.
	StatusConnected
	// StatusReconnecting means the client is attempting to reconnect.
	StatusReconnecting
	// StatusError means the client encountered a non-recoverable error.
	StatusError
)

func (s ClientStatus) String() string {
	switch s {
	case StatusDisconnected:
		return "disconnected"
	case StatusConnected:
		return "connected"
	case StatusReconnecting:
		return "reconnecting"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

// ClientInfo holds public metadata about a pooled client.
type ClientInfo struct {
	ID        string       // Unique identifier (phone number or bot username)
	Type      ClientType   // User or bot
	Token     string       // Bot token or phone number
	Status    ClientStatus // Current connection status
	Connected bool         // Convenience shortcut
}

// PoolConfig holds configuration for creating a new PoolClient.
type PoolConfig struct {
	AppID         int32
	AppHash       string
	PublicKey     string // Base64-encoded PEM RSA public key
	StringSession string
	TestServer    bool
	DeviceModel   string
	AppVersion    string
	LangCode      string

	// PhoneNumber is required for user clients without a saved session.
	PhoneNumber string

	// BotToken is required for bot clients (empty for user clients).
	BotToken string
}

// PoolOption defines a functional option for pool or client creation.
type PoolOption func(*ClientPool)

// WithHealthCheckInterval sets the health check interval for all clients.
func WithHealthCheckInterval(d time.Duration) PoolOption {
	return func(p *ClientPool) {
		p.healthInterval = d
	}
}

// WithReconnectBackoff sets the base backoff duration for reconnection.
func WithReconnectBackoff(d time.Duration) PoolOption {
	return func(p *ClientPool) {
		p.reconnectBackoff = d
	}
}

// WithMaxReconnectAttempts sets the maximum reconnection attempts.
func WithMaxReconnectAttempts(n int) PoolOption {
	return func(p *ClientPool) {
		p.maxReconnect = n
	}
}

// ---------------------------------------------------------------------------
// PoolClient — thread‑safe wrapper around telegram.Client
// ---------------------------------------------------------------------------

// PoolClient wraps a *telegram.Client with mutex protection, health checks,
// auto‑reconnection, and lifecycle management.
type PoolClient struct {
	mu     sync.RWMutex
	client *mtproto.Client
	config PoolConfig
	info   ClientInfo
	status ClientStatus

	ctx    context.Context
	cancel context.CancelFunc

	// Reconnection
	reconnectCh      chan struct{}
	reconnectBackoff time.Duration
	maxReconnect     int
	reconnectCount   int

	// Health
	lastUsed     time.Time
	healthTicker *time.Ticker
	healthDone   chan struct{}

	// Stats
	connectionCount atomic.Int64
	errorCount      atomic.Int64
	opsCount        atomic.Int64

	// User data — arbitrary metadata attachable by any module
	userData map[string]any
	userMu   sync.RWMutex

	// Pool reference (nil if standalone)
	pool *ClientPool
}

// NewPoolClient creates a new PoolClient from the given config.
//
// The client is NOT automatically connected; call Connect() explicitly.
func NewPoolClient(cfg PoolConfig) *PoolClient {
	ctx, cancel := context.WithCancel(context.Background())

	id := cfg.BotToken
	if id == "" {
		id = cfg.StringSession
	}
	if id == "" {
		id = cfg.AppHash // fallback
	}

	ct := ClientTypeUser
	token := cfg.PhoneNumber
	if cfg.BotToken != "" {
		ct = ClientTypeBot
		token = cfg.BotToken
	}

	return &PoolClient{
		config: cfg,
		info: ClientInfo{
			ID:    id,
			Type:  ct,
			Token: token,
		},
		status:           StatusDisconnected,
		ctx:              ctx,
		cancel:           cancel,
		reconnectCh:      make(chan struct{}, 1),
		reconnectBackoff: 5 * time.Second,
		maxReconnect:     5,
		healthDone:       make(chan struct{}),
		userData:         make(map[string]any),
	}
}

// ---------------------------------------------------------------------------
// PoolClient — connection lifecycle
// ---------------------------------------------------------------------------

// Connect establishes the MTProto connection and authenticates.
//
// For bot clients it uses LoginBot; for user clients it expects an active
// session in StringSession or will prompt via Login.
func (pc *PoolClient) Connect() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.status == StatusConnected {
		return nil
	}

	// Build client config (without connecting yet)
	client, err := pc.buildClient()
	if err != nil {
		pc.setStatusLocked(StatusError)
		return fmt.Errorf("build client: %w", err)
	}

	// Two different connection paths:
	//
	//   Bot  → ConnectBot(botToken)  — single call, connects + logs in
	//   User → Connect() + Login()   — two-step: connect then authenticate
	//
	switch pc.info.Type {
	case ClientTypeBot:
		if err := client.ConnectBot(pc.config.BotToken); err != nil {
			pc.setStatusLocked(StatusError)
			return fmt.Errorf("bot connect: %w", err)
		}

	case ClientTypeUser:
		if pc.config.StringSession == "" {
			// First-time login — connect first, then prompt for code.
			if err := client.Connect(); err != nil {
				pc.setStatusLocked(StatusError)
				return fmt.Errorf("connect: %w", err)
			}

			phone := pc.config.PhoneNumber
			if phone == "" {
				client.Disconnect()
				pc.setStatusLocked(StatusError)
				return fmt.Errorf("user login: phone number is required")
			}
			if _, err := client.Login(phone, &mtproto.LoginOptions{}); err != nil {
				client.Disconnect()
				pc.setStatusLocked(StatusError)
				return fmt.Errorf("user login: %w", err)
			}
		} else {
			// Session already exists — just connect with saved session.
			if err := client.Connect(); err != nil {
				pc.setStatusLocked(StatusError)
				return fmt.Errorf("reconnect: %w", err)
			}
		}
	}

	pc.client = client
	pc.status = StatusConnected
	pc.connectionCount.Add(1)
	pc.lastUsed = time.Now()
	pc.reconnectCount = 0

	// Start health checks
	pc.startHealthChecks()

	return nil
}

// Disconnect gracefully shuts down the connection.
func (pc *PoolClient) Disconnect() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.stopHealthChecks()

	if pc.client != nil {
		pc.client.Disconnect()
		pc.client = nil
	}

	pc.setStatusLocked(StatusDisconnected)
}

// Close cancels the context and disconnects permanently.
func (pc *PoolClient) Close() {
	pc.cancel()
	pc.Disconnect()
}

// Reconnect attempts to re-establish the connection with exponential backoff.
func (pc *PoolClient) Reconnect() error {
	pc.mu.Lock()
	pc.setStatusLocked(StatusReconnecting)
	pc.mu.Unlock()

	pc.Disconnect()

	backoff := pc.reconnectBackoff
	for attempt := 0; attempt < pc.maxReconnect; attempt++ {
		select {
		case <-pc.ctx.Done():
			return pc.ctx.Err()
		default:
		}

		if err := pc.Connect(); err == nil {
			return nil
		}

		// Exponential backoff with jitter
		sleep := backoff + time.Duration(attempt*500)*time.Millisecond
		fs.Infof(nil, "mtproto: reconnect attempt %d/%d, waiting %v", attempt+1, pc.maxReconnect, sleep)

		timer := time.NewTimer(sleep)
		select {
		case <-pc.ctx.Done():
			timer.Stop()
			return pc.ctx.Err()
		case <-timer.C:
		}
	}

	pc.mu.Lock()
	pc.setStatusLocked(StatusError)
	pc.mu.Unlock()

	return fmt.Errorf("mtproto: max reconnect attempts reached (%d)", pc.maxReconnect)
}

// IsConnected returns true if the client is connected and healthy.
func (pc *PoolClient) IsConnected() bool {
	pc.mu.RLock()
	client := pc.client
	status := pc.status
	pc.mu.RUnlock()

	if client == nil || status != StatusConnected {
		return false
	}

	// Verify with a lightweight ping (don't hold lock during I/O)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{}, 1)
	go func() {
		client.Ping()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// Ping checks liveness by measuring round-trip latency.
func (pc *PoolClient) Ping() time.Duration {
	pc.mu.RLock()
	client := pc.client
	pc.mu.RUnlock()

	if client == nil {
		return 0
	}

	return client.Ping()
}

// Client returns the underlying *telegram.Client in a thread-safe manner.
//
// The caller MUST NOT store the returned pointer — it may be invalidated by
// reconnection. Use this for one-shot API calls only.
func (pc *PoolClient) Client() (*mtproto.Client, error) {
	pc.mu.RLock()
	client := pc.client
	status := pc.status
	pc.mu.RUnlock()

	if client == nil || status != StatusConnected {
		// Attempt auto-reconnect
		if err := pc.Reconnect(); err != nil {
			return nil, err
		}
		pc.mu.RLock()
		client = pc.client
		pc.mu.RUnlock()
	}

	pc.mu.Lock()
	pc.lastUsed = time.Now()
	pc.opsCount.Add(1)
	pc.mu.Unlock()

	return client, nil
}

// WithClient executes the given function with a guaranteed valid *telegram.Client,
// handling reconnection automatically.
func (pc *PoolClient) WithClient(fn func(*mtproto.Client) error) error {
	client, err := pc.Client()
	if err != nil {
		return err
	}

	// Wrap with flood wait handling
	return pc.withFloodHandling(client, fn)
}

// withFloodHandling wraps an API call with flood-wait detection and retry.
func (pc *PoolClient) withFloodHandling(client *mtproto.Client, fn func(*mtproto.Client) error) error {
	for {
		err := fn(client)
		if err == nil {
			return nil
		}

		if wait := mtproto.GetFloodWait(err); wait > 0 {
			fs.Infof(nil, "mtproto: flood wait for %d seconds on client %s", wait, pc.info.ID)
			time.Sleep(time.Duration(wait) * time.Second)
			continue
		}

		return err
	}
}

// Status returns the current client status.
func (pc *PoolClient) Status() ClientStatus {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.status
}

// Info returns a copy of the client info.
func (pc *PoolClient) Info() ClientInfo {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	pc.info.Status = pc.status
	pc.info.Connected = pc.status == StatusConnected
	return pc.info
}

// Stats returns operational statistics.
func (pc *PoolClient) Stats() map[string]any {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return map[string]any{
		"id":              pc.info.ID,
		"type":            pc.info.Type.String(),
		"status":          pc.status.String(),
		"connections":     pc.connectionCount.Load(),
		"errors":          pc.errorCount.Load(),
		"operations":      pc.opsCount.Load(),
		"last_used":       pc.lastUsed,
		"reconnect_count": pc.reconnectCount,
		"connected":       pc.client != nil,
	}
}

// Config returns a copy of the pool configuration used to create this client.
func (pc *PoolClient) Config() PoolConfig {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.config
}

// SetStringSession updates the stored session string on an existing client.
// This is used after the initial login to persist the session so that
// subsequent reconnections do not re-prompt for the authentication code.
func (pc *PoolClient) SetStringSession(session string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.config.StringSession = session
}

// SetUserData attaches arbitrary data to this client for cross-module access.
func (pc *PoolClient) SetUserData(key string, value any) {
	pc.userMu.Lock()
	defer pc.userMu.Unlock()
	pc.userData[key] = value
}

// GetUserData retrieves previously attached data.
func (pc *PoolClient) GetUserData(key string) (any, bool) {
	pc.userMu.RLock()
	defer pc.userMu.RUnlock()
	v, ok := pc.userData[key]
	return v, ok
}

// ---------------------------------------------------------------------------
// PoolClient — internal helpers
// ---------------------------------------------------------------------------

func (pc *PoolClient) buildClient() (*mtproto.Client, error) {
	keys, err := pc.decodePublicKey(pc.config.PublicKey)
	if err != nil {
		return nil, err
	}

	// Bot clients use ConnectBot (no session); user clients may have a
	// saved StringSession or connect fresh for phone+code login.
	session := ""
	if pc.info.Type == ClientTypeUser {
		session = pc.config.StringSession
	}

	return mtproto.NewClient(mtproto.ClientConfig{
		AppID:         pc.config.AppID,
		AppHash:       pc.config.AppHash,
		StringSession: session,
		MemorySession: true,
		DisableCache:  true,
		TestMode:      pc.config.TestServer,
		PublicKeys:    keys,
		LogLevel:      mtproto.LogDisable,
		DeviceConfig: mtproto.DeviceConfig{
			DeviceModel:   pc.config.DeviceModel,
			LangCode:      pc.config.LangCode,
			AppVersion:    pc.config.AppVersion,
			SystemVersion: pc.config.AppVersion,
		},
		FloodHandler: pc.onFloodWait,
		ErrorHandler: pc.onError,
	})
}

// DecodePublicKey decodes a base64‑encoded PEM RSA public key string.
// This is exported so that external packages (e.g. the configuration layer)
// can reuse the same decoding logic without duplication.
func DecodePublicKey(pubKey string) ([]*rsa.PublicKey, error) {
	if pubKey == "" {
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return nil, fmt.Errorf("base64 decode public key: %w", err)
	}

	block, _ := pem.Decode(decoded)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in public key")
	}

	key, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse RSA public key: %w", err)
	}

	return []*rsa.PublicKey{key}, nil
}

func (pc *PoolClient) decodePublicKey(pubKey string) ([]*rsa.PublicKey, error) {
	return DecodePublicKey(pubKey)
}

func (pc *PoolClient) onFloodWait(err error) bool {
	if wait := mtproto.GetFloodWait(err); wait > 0 {
		fs.Infof(nil, "mtproto client %s: flood wait %ds", pc.info.ID, wait)
		time.Sleep(time.Duration(wait) * time.Second)
		return true
	}
	return false
}

func (pc *PoolClient) onError(err error) bool {
	pc.errorCount.Add(1)
	fs.Errorf(nil, "mtproto client %s error: %v", pc.info.ID, err)
	return false
}

func (pc *PoolClient) setStatusLocked(s ClientStatus) {
	pc.status = s
	pc.info.Status = s
	pc.info.Connected = s == StatusConnected
}

func (pc *PoolClient) startHealthChecks() {
	pc.stopHealthChecks()

	pc.healthTicker = time.NewTicker(30 * time.Second)
	pc.healthDone = make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fs.Errorf(nil, "mtproto: health check panic for %s: %v", pc.info.ID, r)
			}
		}()

		for {
			select {
			case <-pc.healthDone:
				return
			case <-pc.healthTicker.C:
				pc.runHealthCheck()
			}
		}
	}()
}

func (pc *PoolClient) stopHealthChecks() {
	if pc.healthTicker != nil {
		pc.healthTicker.Stop()
		pc.healthTicker = nil
	}

	select {
	case <-pc.healthDone:
	default:
		close(pc.healthDone)
	}
}

func (pc *PoolClient) runHealthCheck() {
	pc.mu.RLock()
	client := pc.client
	status := pc.status
	pc.mu.RUnlock()

	if client == nil || status != StatusConnected {
		return
	}

	// Ping with timeout via context
	pingCtx, cancel := context.WithTimeout(pc.ctx, 10*time.Second)
	defer cancel()

	done := make(chan struct{}, 1)
	go func() {
		client.Ping()
		close(done)
	}()

	select {
	case <-done:
		// healthy
	case <-pingCtx.Done():
		fs.Errorf(nil, "mtproto: health check timeout for %s, reconnecting", pc.info.ID)
		go pc.Reconnect()
	}
}

// ---------------------------------------------------------------------------
// ClientPool — singleton registry
// ---------------------------------------------------------------------------

// ClientPool is a global, thread-safe registry of MTProto clients.
//
// Use Global() to access the singleton instance, or create a standalone pool
// with NewClientPool() for isolated use cases.
type ClientPool struct {
	mu      sync.RWMutex
	clients map[string]*PoolClient
	ordered []string // insertion order for iteration

	healthInterval   time.Duration
	reconnectBackoff time.Duration
	maxReconnect     int

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	cmdBus *CommandBus // lazy‑initialized command bus
}

var (
	globalPool     *ClientPool
	globalPoolOnce sync.Once
)

// Global returns the singleton client pool, creating it if needed.
func Global() *ClientPool {
	globalPoolOnce.Do(func() {
		globalPool = NewClientPool()
	})
	return globalPool
}

// ResetGlobal resets the singleton pool (primarily for testing).
func ResetGlobal() {
	globalPoolOnce = sync.Once{}
	globalPool = nil
}

// NewClientPool creates a new standalone client pool.
func NewClientPool(opts ...PoolOption) *ClientPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &ClientPool{
		clients:          make(map[string]*PoolClient),
		ordered:          make([]string, 0),
		healthInterval:   30 * time.Second,
		reconnectBackoff: 5 * time.Second,
		maxReconnect:     5,
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
	}

	pool.cmdBus = NewCommandBus(pool)

	for _, opt := range opts {
		opt(pool)
	}

	return pool
}

// ---------------------------------------------------------------------------
// ClientPool — registration
// ---------------------------------------------------------------------------

// Register adds a client to the pool. If an entry with the same ID already
// exists, it is replaced and the old client is closed.
//
// Use RegisterUser / RegisterBot for type‑safe registration that enforces
// the single‑user constraint.
func (p *ClientPool) Register(pc *PoolClient) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Enforce single-user constraint: reject a second user client.
	if pc.info.Type == ClientTypeUser {
		for _, id := range p.ordered {
			if existing := p.clients[id]; existing != nil && existing.info.Type == ClientTypeUser {
				return fmt.Errorf("mtproto: a user client is already registered (%s)", id)
			}
		}
	}

	id := pc.info.ID
	if existing, ok := p.clients[id]; ok {
		existing.Close()
	}

	pc.pool = p
	p.clients[id] = pc
	p.ordered = append(p.ordered, id)

	fs.Infof(nil, "mtproto: registered client %s (%s)", id, pc.info.Type)

	return nil
}

// RegisterUser registers the single user client. If a user client is already
// registered, an error is returned. This ensures exactly one user session
// exists in the pool.
func (p *ClientPool) RegisterUser(pc *PoolClient) error {
	if pc.info.Type != ClientTypeUser {
		return fmt.Errorf("mtproto: RegisterUser requires a ClientTypeUser client")
	}
	return p.Register(pc)
}

// RegisterBot registers a bot worker client. Multiple bots are allowed.
func (p *ClientPool) RegisterBot(pc *PoolClient) error {
	if pc.info.Type != ClientTypeBot {
		return fmt.Errorf("mtproto: RegisterBot requires a ClientTypeBot client")
	}
	return p.Register(pc)
}

// Unregister removes a client from the pool and disconnects it.
func (p *ClientPool) Unregister(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pc, ok := p.clients[id]; ok {
		pc.Close()
		delete(p.clients, id)

		// Remove from ordered slice
		for i, v := range p.ordered {
			if v == id {
				p.ordered = append(p.ordered[:i], p.ordered[i+1:]...)
				break
			}
		}
	}
}

// Get retrieves a client by ID. Returns nil if not found.
func (p *ClientPool) Get(id string) *PoolClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.clients[id]
}

// GetUser returns the single user client, or nil if none is registered.
func (p *ClientPool) GetUser() *PoolClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, pc := range p.clients {
		if pc.info.Type == ClientTypeUser {
			return pc
		}
	}
	return nil
}

// GetBots returns all registered bot worker clients.
func (p *ClientPool) GetBots() []*PoolClient {
	return p.GetAllByType(ClientTypeBot)
}

// GetByIndex retrieves a client by insertion index.
func (p *ClientPool) GetByIndex(idx int) *PoolClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if idx < 0 || idx >= len(p.ordered) {
		return nil
	}

	id := p.ordered[idx]
	return p.clients[id]
}

// GetAll returns a snapshot of all registered clients.
func (p *ClientPool) GetAll() []*PoolClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*PoolClient, 0, len(p.ordered))
	for _, id := range p.ordered {
		if c, ok := p.clients[id]; ok {
			result = append(result, c)
		}
	}
	return result
}

// GetAllByType returns all clients of the specified type.
func (p *ClientPool) GetAllByType(t ClientType) []*PoolClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*PoolClient, 0)
	for _, pc := range p.clients {
		if pc.info.Type == t {
			result = append(result, pc)
		}
	}
	return result
}

// Size returns the number of registered clients.
func (p *ClientPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

// Exists checks if a client with the given ID is registered.
func (p *ClientPool) Exists(id string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.clients[id]
	return ok
}

// IDs returns a copy of all registered client IDs.
func (p *ClientPool) IDs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]string, len(p.ordered))
	copy(ids, p.ordered)
	return ids
}

// ForEach iterates over all clients, calling fn for each.
// If fn returns an error, iteration stops and the error is returned.
func (p *ClientPool) ForEach(fn func(*PoolClient) error) error {
	p.mu.RLock()
	clients := make([]*PoolClient, 0, len(p.ordered))
	for _, id := range p.ordered {
		if c, ok := p.clients[id]; ok {
			clients = append(clients, c)
		}
	}
	p.mu.RUnlock()

	for _, pc := range clients {
		if err := fn(pc); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// ClientPool — lifecycle
// ---------------------------------------------------------------------------

// Shutdown gracefully disconnects all clients and releases resources.
func (p *ClientPool) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, pc := range p.clients {
		fs.Infof(nil, "mtproto: shutting down client %s", id)
		pc.Close()
		delete(p.clients, id)
	}

	p.ordered = make([]string, 0)
	p.cancel()
	close(p.done)
}

// Done returns a channel that is closed when the pool is shut down.
func (p *ClientPool) Done() <-chan struct{} {
	return p.done
}

// ---------------------------------------------------------------------------
// Pool utilities
// ---------------------------------------------------------------------------

// ParseManagers creates PoolClient instances from bot tokens stored in the
// rclone configuration (fs.SpaceSepList). Each bot is registered in the
// global pool and connected.
//
// This is the integration point between the rclone configuration system
// and the client pool.
func ParseManagers(appID int32, appHash string, publicKey string, testServer bool, tokens []string) ([]string, error) {
	pool := Global()
	var ids []string

	for _, token := range tokens {
		id := fmt.Sprintf("bot:%s", token)
		if pool.Exists(id) {
			ids = append(ids, id)
			ids = append(ids, id)
			continue
		}

		cfg := PoolConfig{
			AppID:     appID,
			AppHash:   appHash,
			PublicKey: publicKey,
			BotToken:  token,
		}

		pc := NewPoolClient(cfg)
		if err := pc.Connect(); err != nil {
			return ids, fmt.Errorf("connect manager bot: %w", err)
		}

		if err := pool.RegisterBot(pc); err != nil {
			pc.Close()
			return ids, fmt.Errorf("register manager bot: %w", err)
		}

		ids = append(ids, id)
	}

	return ids, nil
}

// CreateUserClient creates and registers a user-authenticated client in the
// global pool, using the provided session string or phone number.
//
// Only one user client may exist at a time — subsequent calls return the
// existing one (idempotent).
func CreateUserClient(appID int32, appHash, publicKey, session, phoneNumber string, testServer bool) (*PoolClient, error) {
	pool := Global()

	// Check if a user client already exists
	if existing := pool.GetUser(); existing != nil {
		return existing, nil // idempotent: return existing
	}

	cfg := PoolConfig{
		AppID:         appID,
		AppHash:       appHash,
		PublicKey:     publicKey,
		StringSession: session,
		PhoneNumber:   phoneNumber,
		TestServer:    testServer,
		DeviceModel:   fmt.Sprintf("rclone %s %s", fs.VersionSuffix, fs.VersionTag),
		LangCode:      "en",
	}

	pc := NewPoolClient(cfg)
	if err := pc.Connect(); err != nil {
		return nil, err
	}

	if err := Global().RegisterUser(pc); err != nil {
		pc.Close()
		return nil, err
	}

	return pc, nil
}
