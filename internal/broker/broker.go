package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

var (
	ErrSlowSubscriber = errors.New("subscriber evicted due to slow consumption")
	// ErrInsufficientState ends a subscription whose catch-up replay found a hole in the
	// committed history; the client must reconnect with its last cursor to get a cursor_reset.
	ErrInsufficientState = errors.New("subscriber evicted: committed history has a gap")
	ErrInvalidInput      = errors.New("invalid publish request")
	ErrSequenceGap       = errors.New("out-of-order stream sequence")
	ErrTooManyStreams    = errors.New("too many concurrent in-flight streams for this session")
	ErrStreamFinalized   = errors.New("stream already finalized")
	ErrBrokerClosed      = errors.New("broker closed")

	ErrEventTooLarge         = errors.New("event exceeds the maximum event size")
	ErrInFlightQuotaExceeded = errors.New("in-flight stream buffers would exceed the session quota")
)

const (
	defaultSubscriberBufferSize = 128
	defaultIdleSessionTTL       = 30 * time.Minute
	defaultSweepInterval        = 1 * time.Minute

	DefaultMaxConcurrentStreams = 8
	DefaultMaxEventBytes        = 256 << 10
	DefaultMaxInFlightBytes     = 2 << 20
	DefaultIdempotencyWindow    = 5 * time.Minute
)

type Config struct {
	SubscriberBufferSize int           // default 128 if <= 0
	IdleSessionTTL       time.Duration // default 30m if <= 0
	SweepInterval        time.Duration // default 1m if <= 0
	// InstanceID identifies this broker process (random 12-char hex if empty). Session
	// cursors use the per-session stream epoch from Storage, not this value.
	InstanceID string
	// MaxConcurrentStreams bounds in-flight streams per session (default 8).
	MaxConcurrentStreams int
	// MaxEventBytes bounds delta + payload + meta of a single publish (default 256 KiB).
	MaxEventBytes int
	// MaxInFlightBytes bounds the merged text + meta of all in-flight streams of a session (default 2 MiB).
	MaxInFlightBytes int
	// IdempotencyWindow is how long a finalized stream keeps deduplicating retries (default 5m).
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
	cfg.MaxConcurrentStreams = orDefault(cfg.MaxConcurrentStreams, DefaultMaxConcurrentStreams)
	cfg.MaxEventBytes = orDefault(cfg.MaxEventBytes, DefaultMaxEventBytes)
	cfg.MaxInFlightBytes = orDefault(cfg.MaxInFlightBytes, DefaultMaxInFlightBytes)
	cfg.IdempotencyWindow = orDefault(cfg.IdempotencyWindow, DefaultIdempotencyWindow)
	if cfg.InstanceID == "" {
		cfg.InstanceID = randomHex(6)
	}
	return cfg
}

type Broker struct {
	store   storage.Storage
	cfg     Config
	mu      sync.RWMutex
	closed  bool
	doneCh  chan struct{}
	actors  map[string]*sessionActor
	sweepWg sync.WaitGroup
	subWg   sync.WaitGroup
}

func New(store storage.Storage, cfg Config) *Broker {
	if store == nil {
		store = storage.NewMemoryStorage()
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

func (b *Broker) isClosed() bool {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	return closed
}

// activeActorCount returns the number of in-memory session actors (used by tests).
func (b *Broker) activeActorCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.actors)
}

func (b *Broker) CreateSession(ctx context.Context, sessionID string) (model.SessionMeta, error) {
	if err := ctx.Err(); err != nil {
		return model.SessionMeta{}, err
	}
	if b.isClosed() {
		return model.SessionMeta{}, ErrBrokerClosed
	}
	if sessionID == "" {
		sessionID = "sess_" + randomHex(6)
	}

	actor, _, err := b.acquireActor(ctx, sessionID, true, false)
	if err != nil {
		return model.SessionMeta{}, err
	}
	defer actor.unpin()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if !actor.metaPersisted {
		if err := b.store.SaveMeta(ctx, actor.meta); err != nil {
			return model.SessionMeta{}, err
		}
		actor.metaPersisted = true
	}
	meta := actor.meta
	meta.EarliestOffset = b.earliestOffset(ctx, sessionID, meta.LastOffset)
	return meta, nil
}

// earliestOffset reads the first retained offset from storage, falling back to an empty window.
func (b *Broker) earliestOffset(ctx context.Context, sessionID string, lastOffset uint64) uint64 {
	info, ok, err := b.store.Stream(ctx, sessionID)
	if err != nil || !ok {
		return lastOffset + 1
	}
	return info.EarliestOffset
}

func (b *Broker) Inspect(ctx context.Context, sessionID string) (model.SessionSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return model.SessionSnapshot{}, false, err
	}
	if b.isClosed() {
		return model.SessionSnapshot{}, false, ErrBrokerClosed
	}
	if sessionID == "" {
		return model.SessionSnapshot{}, false, nil
	}

	actor, exists, err := b.acquireActor(ctx, sessionID, false, false)
	if err != nil {
		return model.SessionSnapshot{}, false, err
	}
	if !exists {
		return model.SessionSnapshot{}, false, nil
	}
	defer actor.unpin()

	actor.mu.Lock()
	if !actor.metaPersisted {
		if err := b.store.SaveMeta(context.WithoutCancel(ctx), actor.meta); err == nil {
			actor.metaPersisted = true
		}
	}
	snap := actor.snapshotLocked()
	actor.mu.Unlock()

	snap.Meta.EarliestOffset = b.earliestOffset(ctx, sessionID, snap.Meta.LastOffset)
	return snap, true, nil
}

type namedActor struct {
	id    string
	actor *sessionActor
}

func (b *Broker) snapshotActorPointers() ([]namedActor, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrBrokerClosed
	}
	active := make([]namedActor, 0, len(b.actors))
	for id, actor := range b.actors {
		active = append(active, namedActor{id: id, actor: actor})
	}
	return active, nil
}

// normalizeStoredMeta fixes metadata of sessions without a live actor: in-flight streams only
// live inside actors, so a stored "running" state is stale.
func normalizeStoredMeta(meta model.SessionMeta) model.SessionMeta {
	if meta.State == model.StateRunning {
		meta.State = model.StateIdle
	}
	return meta
}

func sortSessionSnapshots(out []model.SessionSnapshot) {
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Meta.CreatedAt.Equal(out[j].Meta.CreatedAt) {
			return out[i].Meta.CreatedAt.Before(out[j].Meta.CreatedAt)
		}
		return out[i].Meta.SessionID < out[j].Meta.SessionID
	})
}

func (b *Broker) ListSessions(ctx context.Context) ([]model.SessionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.isClosed() {
		return nil, ErrBrokerClosed
	}

	metas, err := b.store.ListSessions(ctx)
	if err != nil {
		return nil, err
	}

	byID := make(map[string]model.SessionSnapshot, len(metas))
	for _, meta := range metas {
		byID[meta.SessionID] = model.SessionSnapshot{
			Meta:      normalizeStoredMeta(meta),
			InFlights: []model.InFlightView{},
		}
	}
	if err := b.overlayLiveActors(byID); err != nil {
		return nil, err
	}

	out := make([]model.SessionSnapshot, 0, len(byID))
	for _, snap := range byID {
		out = append(out, snap)
	}
	sortSessionSnapshots(out)
	return out, nil
}

// overlayLiveActors replaces stored snapshots with live actor state, keeping the stored window
// start because only storage knows which records were evicted.
func (b *Broker) overlayLiveActors(byID map[string]model.SessionSnapshot) error {
	active, err := b.snapshotActorPointers()
	if err != nil {
		return err
	}
	for _, item := range active {
		item.actor.mu.Lock()
		snap := item.actor.snapshotLocked()
		item.actor.mu.Unlock()
		snap.Meta.EarliestOffset = snap.Meta.LastOffset + 1
		if stored, ok := byID[item.id]; ok {
			snap.Meta.EarliestOffset = stored.Meta.EarliestOffset
		}
		byID[item.id] = snap
	}
	return nil
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
