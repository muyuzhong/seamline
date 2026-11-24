package broker

import (
	"context"

	"github.com/muyuzhong/seamline/internal/model"
)

type Subscription struct {
	// Epoch is the session stream epoch that every committed offset in this subscription belongs to.
	Epoch  string
	Events <-chan model.OutEvent
	Cancel func()
	Err    func() error
}

// subSnapshot is captured in the same actor.mu critical section that registers liveCh.
type subSnapshot struct {
	epoch     string
	offset    uint64
	inFlights []model.InFlightView
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
	snap := subSnapshot{
		epoch:     actor.meta.Epoch,
		offset:    actor.lastOffset,
		inFlights: actor.inFlightViewsLocked(),
	}
	actor.touch()
	return snap, nil
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

	snap, err := b.registerSubscriber(ctx, actor, sub)
	if err != nil {
		b.subWg.Done()
		return nil, err
	}

	// A cursor from another epoch of this session is replayed from the start of the retained
	// window. An explicit cursor_reset notification replaces this in a follow-up change.
	if afterEpoch != "" && afterEpoch != snap.epoch {
		afterOffset = 0
	}

	outCh := make(chan model.OutEvent)
	go b.runSubscriberPump(ctx, sessionID, actor, sub, outCh, afterOffset, snap)

	return &Subscription{
		Epoch:  snap.epoch,
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
	snap subSnapshot,
) {
	defer b.subWg.Done()
	defer close(outCh)
	defer detachSubscriber(actor, sub)

	if !b.replayCatchUp(ctx, sessionID, sub, outCh, afterOffset, snap) {
		return
	}
	b.drainLiveStream(ctx, sub, outCh, snap)
}

// replayCatchUp sends the committed records in (afterOffset, snap.offset] and then one
// inflight_snapshot per stream that was active at subscription time.
func (b *Broker) replayCatchUp(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.OutEvent,
	afterOffset uint64,
	snap subSnapshot,
) bool {
	if snap.offset > afterOffset && !b.replayStoredRecords(ctx, sessionID, sub, outCh, afterOffset, snap.offset) {
		return false
	}
	return b.replayInFlightSnapshots(ctx, sub, outCh, snap)
}

func (b *Broker) replayStoredRecords(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.OutEvent,
	afterOffset uint64,
	snapOffset uint64,
) bool {
	recs, _, err := b.store.ReadFrom(ctx, sessionID, afterOffset)
	if err != nil {
		sub.setErr(err)
		return false
	}
	for _, rec := range recs {
		if rec.Offset > snapOffset {
			break
		}
		if !b.sendOutEvent(ctx, sub, outCh, committedEvent(rec)) {
			return false
		}
	}
	return true
}

func (b *Broker) replayInFlightSnapshots(ctx context.Context, sub *subState, outCh chan<- model.OutEvent, snap subSnapshot) bool {
	for _, f := range snap.inFlights {
		if f.LastSeq == 0 {
			continue
		}
		ev := model.OutEvent{
			Kind:      model.EventInFlightSnapshot,
			Epoch:     snap.epoch,
			StreamID:  f.StreamID,
			Seq:       f.LastSeq,
			Type:      f.Type,
			Delta:     f.Merged,
			Meta:      f.Meta,
			CreatedAt: f.StartedAt,
		}
		if !b.sendOutEvent(ctx, sub, outCh, ev) {
			return false
		}
	}
	return true
}

// isCoveredBySnapshot drops live events already delivered by the catch-up replay: records at or
// below the snapshot offset and deltas of a snapshotted stream at or below its snapshot seq.
// A stream's entry is cleared once its compacted record arrives.
func isCoveredBySnapshot(ev model.OutEvent, snapOffset uint64, snapSeqs map[string]uint64) bool {
	switch ev.Kind {
	case model.EventCommitted:
		if ev.Offset <= snapOffset {
			return true
		}
		delete(snapSeqs, ev.StreamID)
		return false
	case model.EventDelta:
		last, ok := snapSeqs[ev.StreamID]
		return ok && ev.Seq <= last
	default:
		return false
	}
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

// snapshotSeqs maps each snapshotted stream to the seq its inflight_snapshot covered.
func snapshotSeqs(views []model.InFlightView) map[string]uint64 {
	seqs := make(map[string]uint64, len(views))
	for _, f := range views {
		seqs[f.StreamID] = f.LastSeq
	}
	return seqs
}

func (b *Broker) drainLiveStream(ctx context.Context, sub *subState, outCh chan<- model.OutEvent, snap subSnapshot) {
	snapSeqs := snapshotSeqs(snap.inFlights)
	for {
		if b.checkPumpStop(ctx, sub) {
			return
		}
		ev, ok := b.waitNextLiveEvent(ctx, sub)
		if !ok {
			return
		}
		if isCoveredBySnapshot(ev, snap.offset, snapSeqs) {
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
	ev.Payload = cloneRawMessage(ev.Payload)
	ev.Meta = cloneRawMessage(ev.Meta)
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
