package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

var (
	ErrSlowSubscriber = errors.New("subscriber evicted due to slow consumption")
	// ErrInsufficientState ends a subscription whose catch-up replay found a hole in the
	// history; the client must reconnect with its last cursor to get a cursor_reset.
	ErrInsufficientState = errors.New("subscriber evicted: event history has a gap")
	ErrInvalidInput      = errors.New("invalid publish request")
	ErrEventTooLarge     = errors.New("event payload exceeds the maximum event size")
	// ErrActiveRunQuotaExceeded is returned when an event would grow the active run beyond
	// the storage's MaxActiveRunBytes. Terminal events are never rejected by it.
	ErrActiveRunQuotaExceeded = hotwindow.ErrActiveRunQuotaExceeded
	ErrBrokerClosed           = errors.New("broker closed")
)

const (
	defaultSubscriberBufferSize = 128
	defaultIdleSessionTTL       = 30 * time.Minute
	defaultSweepInterval        = 1 * time.Minute

	DefaultMaxEventBytes        = 256 << 10
	DefaultActiveRunIdleTimeout = 2 * time.Minute
	DefaultIdempotencyWindow    = 5 * time.Minute
)

type Config struct {
	SubscriberBufferSize int           // default 128 if <= 0
	IdleSessionTTL       time.Duration // default 30m if <= 0
	SweepInterval        time.Duration // default 1m if <= 0
	// InstanceID identifies this broker process (random 12-char hex if empty). Session
	// cursors use the per-session stream epoch from storage, not this value.
	InstanceID string
	// MaxEventBytes bounds the payload of one publish (default 256 KiB).
	MaxEventBytes int
	// ActiveRunIdleTimeout ends an active run with run_failed {"reason":"idle_timeout"} when
	// no event was published in it for this long (default 2m).
	ActiveRunIdleTimeout time.Duration
	// IdempotencyWindow is how long an idempotency_key deduplicates retries (default 5m).
	IdempotencyWindow time.Duration
}

func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

func normalizeConfig(cfg Config) Config {
	cfg.SubscriberBufferSize = orDefault(cfg.SubscriberBufferSize, defaultSubscriberBufferSize)
	cfg.IdleSessionTTL = orDefault(cfg.IdleSessionTTL, defaultIdleSessionTTL)
	cfg.SweepInterval = orDefault(cfg.SweepInterval, defaultSweepInterval)
	cfg.MaxEventBytes = orDefault(cfg.MaxEventBytes, DefaultMaxEventBytes)
	cfg.ActiveRunIdleTimeout = orDefault(cfg.ActiveRunIdleTimeout, DefaultActiveRunIdleTimeout)
	cfg.IdempotencyWindow = orDefault(cfg.IdempotencyWindow, DefaultIdempotencyWindow)
	if cfg.InstanceID == "" {
		cfg.InstanceID = randomHex(6)
	}
	return cfg
}

type Broker struct {
	store   hotwindow.Storage
	cfg     Config
	mu      sync.RWMutex
	closed  bool
	doneCh  chan struct{}
	actors  map[string]*sessionActor
	sweepWg sync.WaitGroup
	subWg   sync.WaitGroup
}

func New(store hotwindow.Storage, cfg Config) *Broker {
	if store == nil {
		store = hotwindow.NewMemoryStorage()
	}
	b := &Broker{
		store:  store,
		cfg:    normalizeConfig(cfg),
		doneCh: make(chan struct{}),
		actors: make(map[string]*sessionActor),
	}
	b.sweepWg.Add(1)
	go b.sweepLoop()
	return b
}

func randomHex(numBytes int) string {
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%012x", time.Now().UnixNano()&0xffffffffffff)
	}
	return hex.EncodeToString(buf)
}

// InstanceID identifies this broker process. It is not a session cursor epoch.
func (b *Broker) InstanceID() string {
	return b.cfg.InstanceID
}

// MaxEventBytes is the largest accepted event payload.
func (b *Broker) MaxEventBytes() int {
	return b.cfg.MaxEventBytes
}

func (b *Broker) isClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

// activeActorCount returns the number of in-memory session actors (used by tests).
func (b *Broker) activeActorCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.actors)
}

func metaFromInfo(info hotwindow.StreamInfo) model.SessionMeta {
	return model.SessionMeta{
		SessionID:         info.SessionID,
		Epoch:             info.Epoch,
		State:             info.State,
		EarliestSeq:       info.EarliestSeq,
		LatestSeq:         info.LatestSeq,
		ActiveRunStartSeq: info.ActiveRunStartSeq,
		CreatedAt:         info.CreatedAt,
		UpdatedAt:         info.UpdatedAt,
	}
}

func (b *Broker) checkOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.isClosed() {
		return ErrBrokerClosed
	}
	return nil
}

// CreateSession creates the session stream (with a fresh epoch) if it does not exist yet.
func (b *Broker) CreateSession(ctx context.Context, sessionID string) (model.SessionMeta, error) {
	if err := b.checkOpen(ctx); err != nil {
		return model.SessionMeta{}, err
	}
	if sessionID == "" {
		sessionID = "sess_" + randomHex(6)
	}
	info, err := b.store.EnsureStream(ctx, sessionID)
	if err != nil {
		return model.SessionMeta{}, err
	}
	return metaFromInfo(info), nil
}

// Inspect reads a session from storage, which is authoritative because every append writes the
// event and the session state together.
func (b *Broker) Inspect(ctx context.Context, sessionID string) (model.SessionSnapshot, bool, error) {
	if err := b.checkOpen(ctx); err != nil {
		return model.SessionSnapshot{}, false, err
	}
	if sessionID == "" {
		return model.SessionSnapshot{}, false, nil
	}
	info, ok, err := b.store.Stream(ctx, sessionID)
	if err != nil || !ok {
		return model.SessionSnapshot{}, false, err
	}
	return model.SessionSnapshot{Meta: metaFromInfo(info), SubscriberCount: b.subscriberCount(sessionID)}, true, nil
}

func (b *Broker) ListSessions(ctx context.Context) ([]model.SessionSnapshot, error) {
	if err := b.checkOpen(ctx); err != nil {
		return nil, err
	}
	infos, err := b.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.SessionSnapshot, 0, len(infos))
	for _, info := range infos {
		out = append(out, model.SessionSnapshot{Meta: metaFromInfo(info), SubscriberCount: b.subscriberCount(info.SessionID)})
	}
	return out, nil
}

// subscriberCount looks up a live actor under b.mu.RLock and locks it only after releasing b.mu.
func (b *Broker) subscriberCount(sessionID string) int {
	b.mu.RLock()
	actor := b.actors[sessionID]
	b.mu.RUnlock()
	if actor == nil {
		return 0
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	return len(actor.subscribers)
}

func (b *Broker) Close() error {
	b.mu.Lock()
	var oldActors map[string]*sessionActor
	if !b.closed {
		b.closed = true
		close(b.doneCh)
		oldActors = b.actors
		b.actors = make(map[string]*sessionActor)
	}
	b.mu.Unlock()

	for _, actor := range oldActors {
		actor.closeSubscribersWithErr(ErrBrokerClosed)
	}
	b.sweepWg.Wait()
	b.subWg.Wait()
	return nil
}
