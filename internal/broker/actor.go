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

func (b *Broker) sweepLoop() {
	defer b.sweepWg.Done()
	ticker := time.NewTicker(b.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.doneCh:
			return
		case <-ticker.C:
			b.evictIdleActors(time.Now())
		}
	}
}

func (b *Broker) evictIdleActors(now time.Time) {
	var toFlush []*sessionActor

	b.mu.Lock()
	if !b.closed {
		for id, actor := range b.actors {
			if dirty := b.tryEvictOrStageFlushLocked(id, actor, now); dirty != nil {
				toFlush = append(toFlush, dirty)
			}
		}
	}
	b.mu.Unlock()

	for _, actor := range toFlush {
		b.flushDirtyActorMeta(actor)
	}
}

func (b *Broker) tryEvictOrStageFlushLocked(id string, actor *sessionActor, now time.Time) *sessionActor {
	if actor.pins.Load() > 0 || !actor.mu.TryLock() {
		return nil
	}
	defer actor.mu.Unlock()

	lastActive := time.Unix(0, actor.lastActiveNano.Load())
	isIdle := actor.pins.Load() == 0 && len(actor.subscribers) == 0 && actor.inFlight == nil
	if !isIdle || now.Sub(lastActive) <= b.cfg.IdleSessionTTL {
		return nil
	}

	if !actor.metaPersisted && !actor.metaFlushTried {
		actor.pins.Add(1)
		return actor
	}

	delete(b.actors, id)
	return nil
}

func (b *Broker) flushDirtyActorMeta(actor *sessionActor) {
	defer actor.pins.Add(-1)
	actor.mu.Lock()
	defer actor.mu.Unlock()

	if actor.metaPersisted {
		return
	}
	actor.metaFlushTried = true
	if err := b.store.SaveMeta(context.Background(), actor.meta); err == nil {
		actor.metaPersisted = true
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

func (b *Broker) loadReconciledMeta(ctx context.Context, sessionID string) (model.SessionMeta, bool, bool, error) {
	meta, exists, err := b.store.LoadMeta(ctx, sessionID)
	if err != nil {
		return model.SessionMeta{}, false, false, err
	}

	tailRecs, err := b.store.ReadFrom(ctx, sessionID, meta.LastOffset)
	if err != nil {
		return model.SessionMeta{}, false, false, err
	}
	if len(tailRecs) == 0 {
		return meta, exists, false, nil
	}

	lastRec := tailRecs[len(tailRecs)-1]
	if !exists {
		meta = model.SessionMeta{
			SessionID: sessionID,
			Epoch:     b.epoch,
			CreatedAt: tailRecs[0].CreatedAt,
		}
	}
	meta.LastOffset = lastRec.Offset
	meta.UpdatedAt = lastRec.CreatedAt
	meta.State = model.StateFromRecordType(lastRec.Type)
	return meta, true, true, nil
}

func (b *Broker) newCandidateActor(sessionID string, meta model.SessionMeta, exists bool, needsHeal bool) *sessionActor {
	if exists {
		meta.Epoch = b.epoch
		if meta.State == model.StateRunning {
			meta.State = model.StateIdle
		}
	} else {
		now := time.Now().UTC()
		meta = model.SessionMeta{
			SessionID:  sessionID,
			Epoch:      b.epoch,
			State:      model.StateIdle,
			LastOffset: 0,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
	}

	actor := &sessionActor{
		meta:          meta,
		metaPersisted: exists && !needsHeal,
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

	// Load from storage outside b.mu so storage I/O never blocks other sessions.
	meta, exists, needsHeal, err := b.loadReconciledMeta(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}
	if !exists && !createIfMissing {
		return nil, false, nil
	}

	candidate := b.newCandidateActor(sessionID, meta, exists, needsHeal)
	actor, isNewCandidate, err := b.registerOrPinActor(sessionID, candidate)
	if err != nil {
		return nil, false, err
	}
	if !isNewCandidate {
		return actor, true, nil
	}

	if persistInitialMeta || needsHeal {
		candidate.ensureInitialMetaPersisted(ctx, b.store)
	}
	return candidate, true, nil
}
