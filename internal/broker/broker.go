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
	ErrInvalidInput   = errors.New("invalid publish request")
	ErrSequenceGap    = errors.New("out-of-order stream sequence")
	ErrStreamConflict = errors.New("another stream is currently in-flight for this session")
	ErrBrokerClosed   = errors.New("broker closed")
)

const (
	defaultSubscriberBufferSize = 128
	defaultIdleSessionTTL       = 30 * time.Minute
	defaultSweepInterval        = 1 * time.Minute
)

type Config struct {
	SubscriberBufferSize int           // default 128 if <= 0
	IdleSessionTTL       time.Duration // default 30m if <= 0
	SweepInterval        time.Duration // default 1m if <= 0
	Epoch                string        // random 12-char hex generated on New if empty
}

func normalizeConfig(cfg Config) Config {
	if cfg.SubscriberBufferSize <= 0 {
		cfg.SubscriberBufferSize = defaultSubscriberBufferSize
	}
	if cfg.IdleSessionTTL <= 0 {
		cfg.IdleSessionTTL = defaultIdleSessionTTL
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaultSweepInterval
	}
	if cfg.Epoch == "" {
		cfg.Epoch = randomHex(6)
	}
	return cfg
}

type Broker struct {
	store   storage.Storage
	cfg     Config
	epoch   string
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
	cfg = normalizeConfig(cfg)

	b := &Broker{
		store:  store,
		cfg:    cfg,
		epoch:  cfg.Epoch,
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

func (b *Broker) Epoch() string {
	return b.epoch
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
	return actor.meta, nil
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

func (b *Broker) normalizeStoredMeta(meta model.SessionMeta) model.SessionMeta {
	if meta.Epoch != b.epoch && meta.State == model.StateRunning {
		meta.State = model.StateIdle
	}
	meta.Epoch = b.epoch
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
			Meta: b.normalizeStoredMeta(meta),
		}
	}

	active, err := b.snapshotActorPointers()
	if err != nil {
		return nil, err
	}
	for _, item := range active {
		item.actor.mu.Lock()
		byID[item.id] = item.actor.snapshotLocked()
		item.actor.mu.Unlock()
	}

	out := make([]model.SessionSnapshot, 0, len(byID))
	for _, snap := range byID {
		out = append(out, snap)
	}
	sortSessionSnapshots(out)
	return out, nil
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
