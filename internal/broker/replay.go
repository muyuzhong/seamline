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
	if plan.reason == "" && plan.readAfter == snap.seq {
		// Nothing to replay (caught-up cursor, or a fresh subscription to an empty stream): every
		// later event arrives through liveCh, so the cursor cannot have expired.
		return true
	}
	events, info, err := b.store.ReadAfter(ctx, sessionID, plan.readAfter)
	if err != nil {
		sub.setErr(err)
		return false
	}
	if info.Epoch != snap.epoch {
		sub.setErr(ErrInsufficientState)
		return false
	}
	if plan.checkWindow && plan.readAfter+1 < info.EarliestSeq {
		plan.reason = model.ResetCursorExpired
	}
	if plan.reason != "" && !b.sendOut(ctx, sub, outCh, resetDelivery(cur, snap, info, plan.reason)) {
		return false
	}
	return b.replayEvents(ctx, sub, outCh, events, plan, snap.seq)
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
	// wantSeq is the next seq the replay must deliver; 0 lets the first event start anywhere in
	// the window (fresh subscription or after a reset).
	var wantSeq uint64
	if plan.checkWindow && plan.reason == "" {
		wantSeq = plan.readAfter + 1
	}
	for _, ev := range events {
		if ev.Seq > snapSeq {
			break
		}
		if wantSeq != 0 && ev.Seq != wantSeq {
			sub.setErr(ErrInsufficientState)
			return false
		}
		if !b.sendOut(ctx, sub, outCh, eventDelivery(ev)) {
			return false
		}
		wantSeq = ev.Seq + 1
	}
	if wantSeq != 0 && wantSeq <= snapSeq {
		sub.setErr(ErrInsufficientState)
		return false
	}
	return true
}
