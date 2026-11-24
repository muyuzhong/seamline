package broker

import (
	"context"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

// replayPlan decides how a subscription starts before history is read.
type replayPlan struct {
	readAfter uint64
	// reason is a cursor_reset reason decided from the cursor alone ("" when none yet).
	reason string
	// checkWindow is set when the cursor is resumable unless its next offset was evicted.
	checkWindow bool
}

func planReplay(cur cursor, snap subSnapshot) replayPlan {
	switch {
	case cur.epoch == "" && cur.offset == 0:
		return replayPlan{} // fresh subscription: whole hot window, no reset
	case cur.epoch != snap.epoch:
		// Includes a bare offset without epoch: it cannot be validated against this incarnation.
		return replayPlan{reason: model.ResetEpochMismatch}
	case cur.offset > snap.offset:
		return replayPlan{reason: model.ResetCursorAhead}
	default:
		return replayPlan{readAfter: cur.offset, checkWindow: true}
	}
}

func resetEvent(cur cursor, snap subSnapshot, info storage.StreamInfo, reason string) model.OutEvent {
	return model.OutEvent{
		Kind:  model.EventCursorReset,
		Epoch: snap.epoch,
		Reset: &model.CursorReset{
			Reason:          reason,
			RequestedEpoch:  cur.epoch,
			RequestedOffset: cur.offset,
			CurrentEpoch:    snap.epoch,
			EarliestOffset:  min(info.EarliestOffset, snap.offset+1),
			LatestOffset:    snap.offset,
		},
		CreatedAt: time.Now().UTC(),
	}
}

// replayCatchUp sends, in order: an optional cursor_reset, the retained committed records up to
// the snapshot offset, and one inflight_snapshot per stream active at subscription time.
func (b *Broker) replayCatchUp(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.OutEvent,
	cur cursor,
	snap subSnapshot,
) bool {
	plan := planReplay(cur, snap)
	recs, info, err := b.store.ReadFrom(ctx, sessionID, plan.readAfter)
	if err != nil {
		sub.setErr(err)
		return false
	}
	if info.Epoch != snap.epoch {
		sub.setErr(ErrInsufficientState)
		return false
	}
	if plan.checkWindow && plan.readAfter+1 < info.EarliestOffset {
		plan.reason = model.ResetCursorExpired
	}
	if plan.reason != "" && !b.sendOutEvent(ctx, sub, outCh, resetEvent(cur, snap, info, plan.reason)) {
		return false
	}
	if !b.replayRecords(ctx, sub, outCh, recs, plan, snap.offset) {
		return false
	}
	return b.replayInFlightSnapshots(ctx, sub, outCh, snap)
}

// contiguity checks that replayed offsets have no holes. While strict is false (fresh subscription
// or after a reset) the first record may start anywhere in the window; after that, and from the
// start for a resumable cursor, every offset must be prev+1.
type contiguity struct {
	prev   uint64
	strict bool
}

func (c *contiguity) accept(offset uint64) bool {
	if c.strict && offset != c.prev+1 {
		return false
	}
	c.prev, c.strict = offset, true
	return true
}

// reached reports whether the replay got up to target without losing its tail.
func (c *contiguity) reached(target uint64) bool {
	return !c.strict || c.prev >= target
}

// replayRecords emits records in (readAfter, snapOffset]. A hole means history was lost under
// the subscriber, which then ends with ErrInsufficientState instead of skipping silently.
func (b *Broker) replayRecords(
	ctx context.Context,
	sub *subState,
	outCh chan<- model.OutEvent,
	recs []model.Record,
	plan replayPlan,
	snapOffset uint64,
) bool {
	order := contiguity{prev: plan.readAfter, strict: plan.checkWindow && plan.reason == ""}
	for _, rec := range recs {
		if rec.Offset > snapOffset {
			break
		}
		if !order.accept(rec.Offset) {
			sub.setErr(ErrInsufficientState)
			return false
		}
		if !b.sendOutEvent(ctx, sub, outCh, committedEvent(rec)) {
			return false
		}
	}
	if !order.reached(snapOffset) {
		sub.setErr(ErrInsufficientState)
		return false
	}
	return true
}
