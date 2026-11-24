package broker

import (
	"context"

	"github.com/muyuzhong/seamline/internal/model"
)

type Subscription struct {
	// Epoch is the session stream epoch every delivered seq belongs to.
	Epoch  string
	Events <-chan model.Delivery
	Cancel func()
	Err    func() error
}

// subSnapshot is captured in the same actor.mu critical section that registers liveCh.
type subSnapshot struct {
	epoch string
	seq   uint64
}

func (b *Broker) reserveSubSlot() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	b.subWg.Add(1)
	return nil
}

func (b *Broker) registerSubscriber(ctx context.Context, actor *sessionActor, sub *subState) (subSnapshot, error) {
	actor.mu.Lock()
	defer actor.mu.Unlock()

	if err := b.checkActorPublishReady(ctx); err != nil {
		return subSnapshot{}, err
	}
	actor.subscribers[sub] = struct{}{}
	actor.touch()
	return subSnapshot{epoch: actor.epoch, seq: actor.latestSeq}, nil
}

// Subscribe resumes the session after (afterEpoch, afterSeq). An empty epoch with seq 0 is a fresh
// subscription that replays the retained window before live events.
func (b *Broker) Subscribe(ctx context.Context, sessionID string, afterSeq uint64, afterEpoch string) (*Subscription, error) {
	if err := b.checkOpen(ctx); err != nil {
		return nil, err
	}
	if sessionID == "" {
		return nil, ErrInvalidInput
	}

	actor, err := b.acquireActor(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	defer actor.unpin()

	if err := b.reserveSubSlot(); err != nil {
		return nil, err
	}
	sub := &subState{
		liveCh:    make(chan model.Delivery, b.cfg.SubscriberBufferSize),
		cancelCh:  make(chan struct{}),
		evictedCh: make(chan struct{}),
	}
	snap, err := b.registerSubscriber(ctx, actor, sub)
	if err != nil {
		b.subWg.Done()
		return nil, err
	}

	// A cursor from another epoch of this session is replayed from the start of the retained
	// window. An explicit cursor_reset notification replaces this in a follow-up change.
	if afterEpoch != snap.epoch {
		afterSeq = 0
	}

	outCh := make(chan model.Delivery)
	go b.runSubscriberPump(ctx, sessionID, actor, sub, outCh, afterSeq, snap)
	return &Subscription{Epoch: snap.epoch, Events: outCh, Cancel: sub.cancel, Err: sub.getErr}, nil
}

func detachSubscriber(actor *sessionActor, sub *subState) {
	actor.mu.Lock()
	if _, ok := actor.subscribers[sub]; ok {
		delete(actor.subscribers, sub)
		close(sub.liveCh)
	}
	actor.touch()
	actor.mu.Unlock()
}

func (b *Broker) runSubscriberPump(
	ctx context.Context,
	sessionID string,
	actor *sessionActor,
	sub *subState,
	outCh chan<- model.Delivery,
	afterSeq uint64,
	snap subSnapshot,
) {
	defer b.subWg.Done()
	defer close(outCh)
	defer detachSubscriber(actor, sub)

	if !b.replayCatchUp(ctx, sessionID, sub, outCh, afterSeq, snap.seq) {
		return
	}
	b.drainLiveStream(ctx, sub, outCh, snap.seq)
}

// replayCatchUp sends the retained events in (afterSeq, snapSeq] from one ReadAfter.
func (b *Broker) replayCatchUp(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.Delivery,
	afterSeq uint64,
	snapSeq uint64,
) bool {
	events, _, err := b.store.ReadAfter(ctx, sessionID, afterSeq)
	if err != nil {
		sub.setErr(err)
		return false
	}
	for _, ev := range events {
		if ev.Seq > snapSeq {
			break
		}
		if !b.sendOut(ctx, sub, outCh, eventDelivery(ev)) {
			return false
		}
	}
	return true
}

func (b *Broker) checkPumpStop(ctx context.Context, sub *subState) bool {
	select {
	case <-ctx.Done():
		sub.setErr(ctx.Err())
		return true
	case <-sub.cancelCh:
		return true
	case <-sub.evictedCh:
		return true
	case <-b.doneCh:
		sub.setErr(ErrBrokerClosed)
		return true
	default:
		return false
	}
}

func (b *Broker) waitNextLiveEvent(ctx context.Context, sub *subState) (model.Delivery, bool) {
	select {
	case <-ctx.Done():
		sub.setErr(ctx.Err())
		return model.Delivery{}, false
	case <-sub.cancelCh:
		return model.Delivery{}, false
	case <-sub.evictedCh:
		return model.Delivery{}, false
	case <-b.doneCh:
		sub.setErr(ErrBrokerClosed)
		return model.Delivery{}, false
	case d, ok := <-sub.liveCh:
		return d, ok
	}
}

// drainLiveStream forwards live events. Registration and the snapshot share one critical
// section, so every live event is newer than snapSeq; the filter is defense in depth.
func (b *Broker) drainLiveStream(ctx context.Context, sub *subState, outCh chan<- model.Delivery, snapSeq uint64) {
	for {
		if b.checkPumpStop(ctx, sub) {
			return
		}
		d, ok := b.waitNextLiveEvent(ctx, sub)
		if !ok {
			return
		}
		if d.Event.Seq <= snapSeq {
			continue
		}
		if !b.sendOut(ctx, sub, outCh, d) {
			return
		}
	}
}

func (b *Broker) sendOut(ctx context.Context, sub *subState, outCh chan<- model.Delivery, d model.Delivery) bool {
	if b.checkPumpStop(ctx, sub) {
		return false
	}
	d.Event.Payload = normalizeRaw(d.Event.Payload)
	select {
	case outCh <- d:
		return true
	case <-ctx.Done():
		sub.setErr(ctx.Err())
		return false
	case <-sub.cancelCh:
		return false
	case <-sub.evictedCh:
		return false
	case <-b.doneCh:
		sub.setErr(ErrBrokerClosed)
		return false
	}
}
