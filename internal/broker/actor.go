package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

type subState struct {
	liveCh     chan model.Delivery
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

// sessionActor serializes publishes and subscription registration of one session. Its fields
// mirror the storage stream and only change after a successful Append.
type sessionActor struct {
	mu                sync.Mutex
	sessionID         string
	epoch             string
	latestSeq         uint64
	activeRunStartSeq uint64
	subscribers       map[*subState]struct{}
	lastActiveNano    atomic.Int64
	pins              atomic.Int32
}

func (a *sessionActor) touch() {
	a.lastActiveNano.Store(time.Now().UnixNano())
}

func (a *sessionActor) unpin() {
	a.touch()
	a.pins.Add(-1)
}

func eventDelivery(ev model.Event) model.Delivery {
	return model.Delivery{Kind: model.DeliveryEvent, Event: ev}
}

func (a *sessionActor) fanOutLocked(d model.Delivery) {
	for sub := range a.subscribers {
		select {
		case sub.liveCh <- d:
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

func (b *Broker) registerOrPinActor(sessionID string, candidate *sessionActor) (*sessionActor, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBrokerClosed
	}
	if existing, ok := b.actors[sessionID]; ok {
		existing.pins.Add(1)
		existing.touch()
		return existing, nil
	}
	b.actors[sessionID] = candidate
	return candidate, nil
}

// acquireActor finds or hydrates a sessionActor and increments its atomic pin count so the
// background sweeper cannot evict it while an operation runs.
func (b *Broker) acquireActor(ctx context.Context, sessionID string) (*sessionActor, error) {
	actor, err := b.getPinnedActor(sessionID)
	if err != nil || actor != nil {
		return actor, err
	}

	// Storage I/O happens outside b.mu so it never blocks other sessions.
	info, err := b.store.EnsureStream(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	candidate := &sessionActor{
		sessionID:         sessionID,
		epoch:             info.Epoch,
		latestSeq:         info.LatestSeq,
		activeRunStartSeq: info.ActiveRunStartSeq,
		subscribers:       make(map[*subState]struct{}),
	}
	candidate.pins.Store(1)
	candidate.touch()
	return b.registerOrPinActor(sessionID, candidate)
}
