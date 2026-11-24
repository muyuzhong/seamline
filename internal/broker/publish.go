package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

func hasJSON(raw json.RawMessage) bool {
	return len(bytes.TrimSpace(raw)) > 0
}

// payloadOrNil returns raw, or nil when it is empty or whitespace. Storage keeps its own copy.
func payloadOrNil(raw json.RawMessage) json.RawMessage {
	if !hasJSON(raw) {
		return nil
	}
	return raw
}

func (b *Broker) validatePublishRequest(sessionID string, req model.PublishRequest) error {
	if sessionID == "" || req.Type == "" || (hasJSON(req.Payload) && !json.Valid(req.Payload)) {
		return ErrInvalidInput
	}
	return nil
}

func (b *Broker) checkActorPublishReady(ctx context.Context) error {
	select {
	case <-b.doneCh:
		return ErrBrokerClosed
	default:
	}
	return ctx.Err()
}

// Publish assigns the next seq of the session to an opaque event, commits it to storage together
// with the run bookkeeping, and fans it out.
func (b *Broker) Publish(ctx context.Context, sessionID string, req model.PublishRequest) (model.PublishResult, error) {
	if err := b.checkOpen(ctx); err != nil {
		return model.PublishResult{}, err
	}
	if err := b.validatePublishRequest(sessionID, req); err != nil {
		return model.PublishResult{}, err
	}

	actor, err := b.acquireActor(ctx, sessionID)
	if err != nil {
		return model.PublishResult{}, err
	}
	defer actor.unpin()

	actor.mu.Lock()
	defer actor.mu.Unlock()
	if err := b.checkActorPublishReady(ctx); err != nil {
		return model.PublishResult{}, err
	}

	actor.touch()
	ev, err := b.appendLocked(ctx, actor, req.Type, payloadOrNil(req.Payload), time.Now().UTC())
	if err != nil {
		return model.PublishResult{}, err
	}
	return model.PublishResult{Epoch: ev.Epoch, Seq: ev.Seq}, nil
}

// appendLocked commits the next event of the actor's session; storage assigns its seq and moves
// the run state. Only once Append succeeded does the actor mirror the new seq and run pin and fan
// the event out, so a failed append changes nothing.
func (b *Broker) appendLocked(ctx context.Context, actor *sessionActor, eventType string, payload json.RawMessage, now time.Time) (model.Event, error) {
	ev, info, err := b.store.Append(ctx, actor.sessionID, eventType, payload, now)
	if err != nil {
		return model.Event{}, err
	}
	actor.latestSeq, actor.activeRunStartSeq = ev.Seq, info.ActiveRunStartSeq
	actor.fanOutLocked(eventDelivery(ev))
	return ev, nil
}
