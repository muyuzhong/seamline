package broker

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

type inFlightState struct {
	streamID  string
	typ       string
	meta      json.RawMessage
	mergedBuf strings.Builder
	lastSeq   uint64
	startedAt time.Time
	updatedAt time.Time
}

// bytes is this stream's contribution to the session's MaxInFlightBytes quota.
func (f *inFlightState) bytes() int {
	return f.mergedBuf.Len() + len(f.meta)
}

func (f *inFlightState) view() model.InFlightView {
	return model.InFlightView{
		StreamID:  f.streamID,
		Type:      f.typ,
		Meta:      f.meta,
		Merged:    f.mergedBuf.String(),
		LastSeq:   f.lastSeq,
		StartedAt: f.startedAt,
		UpdatedAt: f.updatedAt,
	}
}

// finalizedStream remembers a stream compacted into a Record so retries stay idempotent.
type finalizedStream struct {
	lastSeq uint64
	offset  uint64
}

type subState struct {
	liveCh     chan model.OutEvent
	cancelCh   chan struct{}
	cancelOnce sync.Once
	evictedCh  chan struct{}
	errMu      sync.Mutex
	err        error
}

func (s *subState) cancel() {
	s.cancelOnce.Do(func() {
		close(s.cancelCh)
	})
}

func (s *subState) setErr(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

func (s *subState) getErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

type sessionActor struct {
	mu             sync.Mutex
	meta           model.SessionMeta // meta.Epoch is the session stream epoch owned by Storage
	metaPersisted  bool
	metaFlushTried bool
	lastOffset     uint64
	inFlights      map[string]*inFlightState
	inFlightBytes  int
	finalized      *windowIndex[finalizedStream]
	idempotent     *windowIndex[uint64]
	subscribers    map[*subState]struct{}
	lastActiveNano atomic.Int64
	pins           atomic.Int32
}

func (a *sessionActor) touch() {
	a.lastActiveNano.Store(time.Now().UnixNano())
}

func (a *sessionActor) unpin() {
	a.touch()
	a.pins.Add(-1)
}

// inFlightViewsLocked returns the active streams ordered by start time (then stream ID).
func (a *sessionActor) inFlightViewsLocked() []model.InFlightView {
	views := make([]model.InFlightView, 0, len(a.inFlights))
	for _, f := range a.inFlights {
		views = append(views, f.view())
	}
	sort.Slice(views, func(i, j int) bool {
		if !views[i].StartedAt.Equal(views[j].StartedAt) {
			return views[i].StartedAt.Before(views[j].StartedAt)
		}
		return views[i].StreamID < views[j].StreamID
	})
	return views
}

func (a *sessionActor) snapshotLocked() model.SessionSnapshot {
	return model.SessionSnapshot{
		Meta:            a.meta,
		InFlights:       a.inFlightViewsLocked(),
		SubscriberCount: len(a.subscribers),
	}
}

func (a *sessionActor) saveMetaBestEffortLocked(ctx context.Context, store storage.Storage) {
	a.metaFlushTried = false
	if err := store.SaveMeta(context.WithoutCancel(ctx), a.meta); err == nil {
		a.metaPersisted = true
	} else {
		a.metaPersisted = false
	}
}

func (a *sessionActor) fanOutLocked(ev model.OutEvent) {
	for sub := range a.subscribers {
		select {
		case sub.liveCh <- ev:
		default:
			delete(a.subscribers, sub)
			sub.setErr(ErrSlowSubscriber)
			close(sub.liveCh)
			close(sub.evictedCh)
		}
	}
}

func (a *sessionActor) closeSubscribersWithErr(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for sub := range a.subscribers {
		delete(a.subscribers, sub)
		sub.setErr(err)
		close(sub.liveCh)
	}
}

func (b *Broker) getPinnedActor(sessionID string) (*sessionActor, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrBrokerClosed
	}
	actor, ok := b.actors[sessionID]
	if !ok {
		return nil, nil
	}
	actor.pins.Add(1)
	actor.touch()
	return actor, nil
}

// openStream makes sure the session stream exists in storage (creating it with a fresh epoch
// when allowed) and reports whether it exists.
func (b *Broker) openStream(ctx context.Context, sessionID string, create bool) (bool, error) {
	if create {
		_, err := b.store.EnsureStream(ctx, sessionID)
		return err == nil, err
	}
	_, ok, err := b.store.Stream(ctx, sessionID)
	return ok, err
}

func (b *Broker) newCandidateActor(meta model.SessionMeta, saved bool) *sessionActor {
	// In-flight streams only live inside actors, so a freshly hydrated actor is never running.
	if meta.State == model.StateRunning {
		meta.State = model.StateIdle
	}
	actor := &sessionActor{
		meta:          meta,
		metaPersisted: saved,
		lastOffset:    meta.LastOffset,
		inFlights:     make(map[string]*inFlightState),
		finalized:     newWindowIndex[finalizedStream](b.cfg.IdempotencyWindow),
		idempotent:    newWindowIndex[uint64](b.cfg.IdempotencyWindow),
		subscribers:   make(map[*subState]struct{}),
	}
	actor.pins.Store(1)
	actor.touch()
	return actor
}

func (b *Broker) registerOrPinActor(sessionID string, candidate *sessionActor) (*sessionActor, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, false, ErrBrokerClosed
	}
	if existing, ok := b.actors[sessionID]; ok {
		existing.pins.Add(1)
		existing.touch()
		return existing, false, nil
	}
	b.actors[sessionID] = candidate
	return candidate, true, nil
}

func (a *sessionActor) ensureInitialMetaPersisted(ctx context.Context, store storage.Storage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.metaPersisted {
		return
	}
	if err := store.SaveMeta(context.WithoutCancel(ctx), a.meta); err == nil {
		a.metaPersisted = true
	}
}

// acquireActor finds or hydrates a sessionActor and increments its atomic pin
// count so the background sweeper cannot evict it while an operation runs.
func (b *Broker) acquireActor(ctx context.Context, sessionID string, createIfMissing bool, persistInitialMeta bool) (*sessionActor, bool, error) {
	actor, err := b.getPinnedActor(sessionID)
	if err != nil {
		return nil, false, err
	}
	if actor != nil {
		return actor, true, nil
	}

	// Storage I/O happens outside b.mu so it never blocks other sessions.
	exists, err := b.openStream(ctx, sessionID, createIfMissing)
	if err != nil || !exists {
		return nil, false, err
	}
	meta, saved, err := b.store.LoadMeta(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}

	candidate := b.newCandidateActor(meta, saved)
	actor, isNewCandidate, err := b.registerOrPinActor(sessionID, candidate)
	if err != nil {
		return nil, false, err
	}
	if isNewCandidate && persistInitialMeta {
		candidate.ensureInitialMetaPersisted(ctx, b.store)
	}
	return actor, true, nil
}
