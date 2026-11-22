package broker

import (
	"context"

	"github.com/muyuzhong/seamline/internal/model"
)

type Subscription struct {
	Epoch  string
	Events <-chan model.OutEvent
	Cancel func()
	Err    func() error
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

func (b *Broker) registerSubscriber(ctx context.Context, actor *sessionActor, sub *subState) (uint64, *model.InFlightView, error) {
	actor.mu.Lock()
	defer actor.mu.Unlock()

	if err := b.checkActorPublishReady(ctx); err != nil {
		return 0, nil, err
	}
	actor.subscribers[sub] = struct{}{}
	snapOffset := actor.lastOffset
	inFlightSnap := actor.inFlightViewLocked()
	actor.touch()
	return snapOffset, inFlightSnap, nil
}

func (b *Broker) Subscribe(ctx context.Context, sessionID string, afterOffset uint64, afterEpoch string) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.isClosed() {
		return nil, ErrBrokerClosed
	}
	if sessionID == "" {
		return nil, ErrInvalidInput
	}
	if afterEpoch != "" && afterEpoch != b.epoch {
		afterOffset = 0
	}

	actor, _, err := b.acquireActor(ctx, sessionID, true, true)
	if err != nil {
		return nil, err
	}
	defer actor.unpin()

	if err := b.reserveSubSlot(); err != nil {
		return nil, err
	}

	sub := &subState{
		liveCh:    make(chan model.OutEvent, b.cfg.SubscriberBufferSize),
		cancelCh:  make(chan struct{}),
		evictedCh: make(chan struct{}),
	}

	snapOffset, inFlightSnap, err := b.registerSubscriber(ctx, actor, sub)
	if err != nil {
		b.subWg.Done()
		return nil, err
	}

	outCh := make(chan model.OutEvent)
	go b.runSubscriberPump(ctx, sessionID, actor, sub, outCh, afterOffset, snapOffset, inFlightSnap)

	return &Subscription{
		Epoch:  b.epoch,
		Events: outCh,
		Cancel: sub.cancel,
		Err:    sub.getErr,
	}, nil
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
	outCh chan<- model.OutEvent,
	afterOffset uint64,
	snapOffset uint64,
	inFlightSnap *model.InFlightView,
) {
	defer b.subWg.Done()
	defer close(outCh)
	defer detachSubscriber(actor, sub)

	if !b.replayCatchUp(ctx, sessionID, sub, outCh, afterOffset, snapOffset, inFlightSnap) {
		return
	}
	b.drainLiveStream(ctx, sub, outCh, snapOffset, inFlightSnap)
}

func (b *Broker) replayCatchUp(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.OutEvent,
	afterOffset uint64,
	snapOffset uint64,
	inFlightSnap *model.InFlightView,
) bool {
	if snapOffset > afterOffset {
		if !b.replayStoredRecords(ctx, sessionID, sub, outCh, afterOffset, snapOffset) {
			return false
		}
	}
	if inFlightSnap == nil || inFlightSnap.LastSeq == 0 {
		return true
	}
	return b.sendOutEvent(ctx, sub, outCh, model.OutEvent{
		Kind:      model.EventInFlightSnapshot,
		Epoch:     b.epoch,
		StreamID:  inFlightSnap.StreamID,
		Seq:       inFlightSnap.LastSeq,
		Type:      inFlightSnap.Type,
		Delta:     inFlightSnap.Merged,
		CreatedAt: inFlightSnap.StartedAt,
	})
}

func (b *Broker) replayStoredRecords(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.OutEvent,
	afterOffset uint64,
	snapOffset uint64,
) bool {
	recs, err := b.store.ReadFrom(ctx, sessionID, afterOffset)
	if err != nil {
		sub.setErr(err)
		return false
	}
	for _, rec := range recs {
		if rec.Offset > snapOffset {
			continue
		}
		ev := model.OutEvent{
			Kind:      model.EventCommitted,
			Offset:    rec.Offset,
			Epoch:     rec.Epoch,
			StreamID:  rec.StreamID,
			Type:      rec.Type,
			Payload:   rec.Payload,
			CreatedAt: rec.CreatedAt,
		}
		if !b.sendOutEvent(ctx, sub, outCh, ev) {
			return false
		}
	}
	return true
}

func isCoveredBySnapshot(ev model.OutEvent, snapOffset uint64, inFlightSnap *model.InFlightView) (bool, *model.InFlightView) {
	if ev.Kind == model.EventCommitted {
		if ev.Offset <= snapOffset {
			return true, inFlightSnap
		}
		if inFlightSnap != nil && ev.StreamID == inFlightSnap.StreamID {
			return false, nil
		}
		return false, inFlightSnap
	}
	if ev.Kind == model.EventDelta && inFlightSnap != nil && ev.StreamID == inFlightSnap.StreamID && ev.Seq <= inFlightSnap.LastSeq {
		return true, inFlightSnap
	}
	return false, inFlightSnap
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

func (b *Broker) waitNextLiveEvent(ctx context.Context, sub *subState) (model.OutEvent, bool) {
	select {
	case <-ctx.Done():
		sub.setErr(ctx.Err())
		return model.OutEvent{}, false
	case <-sub.cancelCh:
		return model.OutEvent{}, false
	case <-sub.evictedCh:
		return model.OutEvent{}, false
	case <-b.doneCh:
		sub.setErr(ErrBrokerClosed)
		return model.OutEvent{}, false
	case ev, ok := <-sub.liveCh:
		return ev, ok
	}
}

func (b *Broker) drainLiveStream(
	ctx context.Context,
	sub *subState,
	outCh chan<- model.OutEvent,
	snapOffset uint64,
	inFlightSnap *model.InFlightView,
) {
	for {
		if b.checkPumpStop(ctx, sub) {
			return
		}
		ev, ok := b.waitNextLiveEvent(ctx, sub)
		if !ok {
			return
		}
		var covered bool
		covered, inFlightSnap = isCoveredBySnapshot(ev, snapOffset, inFlightSnap)
		if covered {
			continue
		}
		if !b.sendOutEvent(ctx, sub, outCh, ev) {
			return
		}
	}
}

func (b *Broker) sendOutEvent(ctx context.Context, sub *subState, outCh chan<- model.OutEvent, ev model.OutEvent) bool {
	if b.checkPumpStop(ctx, sub) {
		return false
	}
	if ev.Payload != nil {
		ev.Payload = cloneRawMessage(ev.Payload)
	}
	select {
	case outCh <- ev:
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
