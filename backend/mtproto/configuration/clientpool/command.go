package clientpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rclone/rclone/fs"
)

// ---------------------------------------------------------------------------
// Command types
// ---------------------------------------------------------------------------

// Command represents a unit of work that can be dispatched to a pooled client.
//
// Commands are the primary mechanism for cross-module communication: any
// module can publish a command, and any handler subscribed to that command
// name will receive it.
type Command struct {
	// Name identifies the command (e.g. "upload", "delete", "list").
	Name string

	// Payload carries command-specific data.
	Payload any

	// Source is an optional identifier for the sender module.
	Source string

	// TargetID, if set, routes this command to a specific client.
	// Use "" for broadcast to all bots.
	TargetID string
}

// CommandResult is returned after a command is handled.
type CommandResult struct {
	HandlerID string
	ClientID  string
	Data      any
	Err       error
}

// CommandHandler processes a command and returns a result.
type CommandHandler func(ctx context.Context, cmd *Command) (*CommandResult, error)

// CommandMiddleware wraps a handler with cross-cutting logic (logging,
// metrics, access control, etc.).
type CommandMiddleware func(next CommandHandler) CommandHandler

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

// Subscription represents a registered interest in a command pattern.
type Subscription struct {
	ID         string
	Pattern    string // command name or "*" for all
	Handler    CommandHandler
	ClientType ClientType // ClientTypeBot to receive only bot commands, etc.
}

// ---------------------------------------------------------------------------
// CommandBus
// ---------------------------------------------------------------------------

// CommandBus is a thread-safe publish/subscribe bus for dispatching commands
// to client workers. It lives at the pool level so that any module with
// access to the pool can send or receive commands.
type CommandBus struct {
	mu          sync.RWMutex
	subs        map[string][]*Subscription // pattern -> subscriptions
	handlers    map[string]CommandHandler  // named handlers (for direct dispatch)
	nextID      atomic.Int64
	middlewares []CommandMiddleware

	// Pool reference for client discovery
	pool *ClientPool
}

// NewCommandBus creates a command bus bound to the given pool.
func NewCommandBus(pool *ClientPool) *CommandBus {
	return &CommandBus{
		subs:     make(map[string][]*Subscription),
		handlers: make(map[string]CommandHandler),
		pool:     pool,
	}
}

// GlobalCommandBus returns the command bus associated with the global pool.
func GlobalCommandBus() *CommandBus {
	return Global().CommandBus()
}

// CommandBus returns the pool's command bus.
func (p *ClientPool) CommandBus() *CommandBus {
	return p.cmdBus
}

// ---------------------------------------------------------------------------
// CommandBus — subscription
// ---------------------------------------------------------------------------

// Subscribe registers a handler for commands matching the given pattern.
//
//   - pattern: command name (e.g. "upload") or "*" for all commands
//   - handler: the function to invoke
//
// Returns a unique subscription ID that can be used with Unsubscribe.
func (cb *CommandBus) Subscribe(pattern string, handler CommandHandler) string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	id := fmt.Sprintf("sub:%d", cb.nextID.Add(1))
	sub := &Subscription{
		ID:      id,
		Pattern: pattern,
		Handler: cb.applyMiddlewares(handler),
	}

	cb.subs[pattern] = append(cb.subs[pattern], sub)

	fs.Debugf(nil, "mtproto cmd: subscribed %q to pattern %q", id, pattern)
	return id
}

// SubscribeBot is a convenience method that only delivers commands to bot
// worker clients.
func (cb *CommandBus) SubscribeBot(pattern string, handler CommandHandler) string {
	base := cb.Subscribe(pattern, func(ctx context.Context, cmd *Command) (*CommandResult, error) {
		// Only handle if the target is a bot
		if cmd.TargetID != "" {
			pc := cb.pool.Get(cmd.TargetID)
			if pc == nil || pc.info.Type != ClientTypeBot {
				return nil, nil
			}
		}
		return handler(ctx, cmd)
	})
	return base
}

// Unsubscribe removes a subscription by ID.
func (cb *CommandBus) Unsubscribe(id string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	for pattern, subs := range cb.subs {
		for i, sub := range subs {
			if sub.ID == id {
				cb.subs[pattern] = append(subs[:i], subs[i+1:]...)
				if len(cb.subs[pattern]) == 0 {
					delete(cb.subs, pattern)
				}
				return
			}
		}
	}
}

// RegisterHandler registers a named handler that can be invoked directly
// via DispatchToHandler.
func (cb *CommandBus) RegisterHandler(name string, handler CommandHandler) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.handlers[name] = cb.applyMiddlewares(handler)
}

// Use adds a middleware that wraps every handler.
func (cb *CommandBus) Use(mw CommandMiddleware) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.middlewares = append(cb.middlewares, mw)
}

// ---------------------------------------------------------------------------
// CommandBus — publishing
// ---------------------------------------------------------------------------

// Publish sends a command to all matching subscribers.
//
// For each matching subscription, the handler is invoked concurrently.
// Results are collected into a slice.
func (cb *CommandBus) Publish(ctx context.Context, cmd *Command) []*CommandResult {
	patterns := []string{cmd.Name, "*"}
	var results []*CommandResult
	var wg sync.WaitGroup
	var mu sync.Mutex

	cb.mu.RLock()

	for _, pattern := range patterns {
		subs := cb.subs[pattern]
		for _, sub := range subs {
			wg.Add(1)
			go func(s *Subscription) {
				defer wg.Done()

				res, err := s.Handler(ctx, cmd)
				mu.Lock()
				if res != nil {
					res.HandlerID = s.ID
					results = append(results, res)
				} else if err != nil {
					results = append(results, &CommandResult{
						HandlerID: s.ID,
						Err:       err,
					})
				}
				mu.Unlock()
			}(sub)
		}
	}

	cb.mu.RUnlock()
	wg.Wait()

	return results
}

// PublishTo sends a command to a specific client by ID. Returns results
// from all matching subscriptions.
func (cb *CommandBus) PublishTo(ctx context.Context, clientID string, cmd *Command) []*CommandResult {
	cmd.TargetID = clientID
	return cb.Publish(ctx, cmd)
}

// DispatchToHandler invokes a specific named handler directly.
func (cb *CommandBus) DispatchToHandler(ctx context.Context, name string, cmd *Command) (*CommandResult, error) {
	cb.mu.RLock()
	handler, ok := cb.handlers[name]
	cb.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("mtproto cmd: no handler registered with name %q", name)
	}

	return handler(ctx, cmd)
}

// ---------------------------------------------------------------------------
// CommandBus — internal
// ---------------------------------------------------------------------------

func (cb *CommandBus) applyMiddlewares(handler CommandHandler) CommandHandler {
	// Apply in reverse order so the first middleware added wraps the outermost
	for i := len(cb.middlewares) - 1; i >= 0; i-- {
		handler = cb.middlewares[i](handler)
	}
	return handler
}

// ---------------------------------------------------------------------------
// Default middleware
// ---------------------------------------------------------------------------

// LoggingMiddleware logs every command dispatched.
func LoggingMiddleware() CommandMiddleware {
	return func(next CommandHandler) CommandHandler {
		return func(ctx context.Context, cmd *Command) (*CommandResult, error) {
			fs.Debugf(nil, "mtproto cmd: %s from %s (target=%s)", cmd.Name, cmd.Source, cmd.TargetID)
			res, err := next(ctx, cmd)
			if err != nil {
				fs.Errorf(nil, "mtproto cmd: %s failed: %v", cmd.Name, err)
			}
			return res, err
		}
	}
}

// RecoveryMiddleware catches panics in command handlers.
func RecoveryMiddleware() CommandMiddleware {
	return func(next CommandHandler) CommandHandler {
		return func(ctx context.Context, cmd *Command) (res *CommandResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("mtproto cmd: panic in handler for %q: %v", cmd.Name, r)
					fs.Errorf(nil, "%v", err)
				}
			}()
			return next(ctx, cmd)
		}
	}
}

// ---------------------------------------------------------------------------
// Convenience: pool-level command helpers
// ---------------------------------------------------------------------------

// SendCommandToBot dispatches a command to a specific bot worker by ID.
// It is a shortcut for pool.CommandBus().PublishTo(ctx, botID, cmd).
func (p *ClientPool) SendCommandToBot(ctx context.Context, botID string, cmd *Command) []*CommandResult {
	return p.CommandBus().PublishTo(ctx, botID, cmd)
}

// BroadcastToBots dispatches a command to every registered bot worker.
func (p *ClientPool) BroadcastToBots(ctx context.Context, cmd *Command) []*CommandResult {
	cmd.TargetID = "" // broadcast
	return p.CommandBus().Publish(ctx, cmd)
}

// SendToUser dispatches a command to the single user client.
func (p *ClientPool) SendToUser(ctx context.Context, cmd *Command) []*CommandResult {
	user := p.GetUser()
	if user == nil {
		fs.Errorf(nil, "mtproto: no user client to send command %q", cmd.Name)
		return nil
	}
	return p.CommandBus().PublishTo(ctx, user.info.ID, cmd)
}
