package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

// idleTimeoutPayload is the payload of the run_failed event that ends a silent run.
var idleTimeoutPayload = json.RawMessage(`{"reason":"idle_timeout"}`)

// nextRunState is the run state machine. Only an explicit run_started opens a run (and only when
// none is active); a terminal event ends the active run. Everything else leaves the run as is.
func (a *sessionActor) nextRunState(eventType string, seq uint64) (uint64, model.SessionState) {
	running := a.activeRunStartSeq != 0
	if eventType == model.TypeRunStarted && !running {
		return seq, model.StateRunning
	}
	if terminal, ok := model.TerminalState(eventType); ok && running {
		return 0, terminal
	}
	return a.activeRunStartSeq, a.state
}

// nextCommit builds the next event of the session and the run bookkeeping it implies. It does
// not change the actor; applyCommitLocked does once the append succeeded.
func (a *sessionActor) nextCommit(eventType string, payload json.RawMessage, now time.Time) hotwindow.Commit {
	ev := model.Event{
		Seq:       a.latestSeq + 1,
		Epoch:     a.epoch,
		Type:      eventType,
		Payload:   normalizeRaw(payload),
		CreatedAt: now,
	}
	pin, state := a.nextRunState(eventType, ev.Seq)
	return hotwindow.Commit{Event: ev, State: state, ActiveRunStartSeq: pin}
}

func (a *sessionActor) applyCommitLocked(c hotwindow.Commit, now time.Time) {
	a.latestSeq = c.Event.Seq
	a.activeRunStartSeq = c.ActiveRunStartSeq
	a.state = c.State
	if c.ActiveRunStartSeq != 0 {
		a.runActiveAt = now
	}
}

// sealIdleRunLocked ends an active run whose producer has been silent for ActiveRunIdleTimeout
// by appending run_failed {"reason":"idle_timeout"} through the normal commit path.
func (b *Broker) sealIdleRunLocked(actor *sessionActor, now time.Time) error {
	if actor.activeRunStartSeq == 0 || now.Sub(actor.runActiveAt) < b.cfg.ActiveRunIdleTimeout {
		return nil
	}
	c := actor.nextCommit(model.TypeRunFailed, idleTimeoutPayload, now.UTC())
	if err := b.store.Append(context.Background(), actor.sessionID, c); err != nil {
		return err
	}
	actor.applyCommitLocked(c, now)
	actor.fanOutLocked(eventDelivery(c.Event))
	return nil
}
