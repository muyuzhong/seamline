package broker

import (
	"context"
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
	mergedBuf strings.Builder
	lastSeq   uint64
	startedAt time.Time
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
	mu                    sync.Mutex
	meta                  model.SessionMeta
	metaPersisted         bool
	metaFlushTried        bool
	lastOffset            uint64
	inFlight              *inFlightState
	lastCompactedStreamID string
	lastCompactedSeq      uint64
	lastCompactedOffset   uint64
	subscribers           map[*subState]struct{}
	lastActiveNano        atomic.Int64
	pins                  atomic.Int32
}

func (a *sessionActor) touch() {
	a.lastActiveNano.Store(time.Now().UnixNano())
}

func (a *sessionActor) unpin() {
	a.touch()
	a.pins.Add(-1)
}

func (a *sessionActor) inFlightViewLocked() *model.InFlightView {
	if a.inFlight == nil {
		return nil
	}
	return &model.InFlightView{
		StreamID:  a.inFlight.streamID,
		Type:      a.inFlight.typ,
		Merged:    a.inFlight.mergedBuf.String(),
		LastSeq:   a.inFlight.lastSeq,
		StartedAt: a.inFlight.startedAt,
	}
}

func (a *sessionActor) snapshotLocked() model.SessionSnapshot {
	return model.SessionSnapshot{
		Meta:            a.meta,
		InFlight:        a.inFlightViewLocked(),
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
