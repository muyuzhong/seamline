package broker

import (
	"context"

	"github.com/muyuzhong/seamline/internal/model"
)

// replayPlan decides how a subscription starts before history is read.
type replayPlan struct {
	readAfter uint64
	// reason is a cursor reset reason decided from the cursor alone ("" when none yet).
	reason string
	// checkWindow is set when the cursor is resumable unless its next seq was evicted.
	checkWindow bool
}

func planReplay(cur cursor, snap subSnapshot) replayPlan {
	switch {
	case cur.epoch == "" && cur.seq == 0:
		return replayPlan{} // fresh subscription: whole hot window, no reset
	case cur.epoch != snap.epoch:
		// Includes a bare seq without epoch: it cannot be validated against this incarnation.
		return replayPlan{reason: model.ResetEpochMismatch}
	case cur.seq > snap.seq:
		return replayPlan{reason: model.ResetCursorAhead}
	default:
		return replayPlan{readAfter: cur.seq, checkWindow: true}
	}
}

func resetDelivery(cur cursor, snap subSnapshot, info model.SessionMeta, reason string) model.Delivery {
	return model.Delivery{
		Kind: model.DeliveryCursorReset,
		Reset: &model.CursorReset{
			Reason:            reason,
			RequestedEpoch:    cur.epoch,
			RequestedSeq:      cur.seq,
			CurrentEpoch:      snap.epoch,
			EarliestSeq:       min(info.EarliestSeq, snap.seq+1),
			LatestSeq:         snap.seq,
			ActiveRunStartSeq: snap.activeRunStartSeq,
		},
	}
}

// replayCatchUp sends an optional cursor reset (first event only) and then the retained events
// up to the snapshot seq, from one atomic ReadAfter.
func (b *Broker) replayCatchUp(
	ctx context.Context,
	sessionID string,
	sub *subState,
	outCh chan<- model.Delivery,
	cur cursor,
	snap subSnapshot,
) bool {
	plan := planReplay(cur, snap)
	events, info, err := b.store.ReadAfter(ctx, sessionID, plan.readAfter)
	if err != nil {
		sub.setErr(err)
		return false
	}
	if info.Epoch != snap.epoch {
		sub.setErr(ErrInsufficientState)
		return false
	}
	// Events after the snapshot arrive through liveCh, so only a hole at or below snap.seq expires
	// the cursor; publishes since the snapshot may already have evicted later seqs from storage.
	if plan.checkWindow && plan.readAfter+1 < min(info.EarliestSeq, snap.seq+1) {
		plan.reason = model.ResetCursorExpired
	}
	if plan.reason != "" && !b.sendOut(ctx, sub, outCh, resetDelivery(cur, snap, info, plan.reason)) {
		return false
	}
	return b.replayEvents(ctx, sub, outCh, events, plan, snap.seq)
}

// contiguity checks that replayed seqs have no holes. While strict is false (fresh subscription or
// after a reset) the first event may start anywhere in the window; after that, and from the start
// for a resumable cursor, every seq must be prev+1.
type contiguity struct {
	prev   uint64
	strict bool
}

func (c *contiguity) accept(seq uint64) bool {
	if c.strict && seq != c.prev+1 {
		return false
	}
	c.prev, c.strict = seq, true
	return true
}

// reached reports whether the replay got up to target without losing its tail.
func (c *contiguity) reached(target uint64) bool {
	return !c.strict || c.prev >= target
}

// replayEvents emits events in (readAfter, snapSeq]. A hole means history was lost under the
// subscriber, which then ends with ErrInsufficientState instead of skipping silently.
func (b *Broker) replayEvents(
	ctx context.Context,
	sub *subState,
	outCh chan<- model.Delivery,
	events []model.Event,
	plan replayPlan,
	snapSeq uint64,
) bool {
	order := contiguity{prev: plan.readAfter, strict: plan.checkWindow && plan.reason == ""}
	for _, ev := range events {
		if ev.Seq > snapSeq {
			break
		}
		if !order.accept(ev.Seq) {
			sub.setErr(ErrInsufficientState)
			return false
		}
		if !b.sendOut(ctx, sub, outCh, eventDelivery(ev)) {
			return false
		}
	}
	if !order.reached(snapSeq) {
		sub.setErr(ErrInsufficientState)
		return false
	}
	return true
}
