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
	if len(req.Payload) > b.cfg.MaxEventBytes {
		return ErrEventTooLarge
	}
	return nil
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
	if err := b.checkOpen(ctx); err != nil {
		return model.PublishResult{}, err
	}

	now := time.Now().UTC()
	actor.touch()
	if res, ok := b.idempotentReplayLocked(actor, req.IdempotencyKey, now); ok {
		return res, nil
	}
	ev, err := b.appendLocked(ctx, actor, req.Type, payloadOrNil(req.Payload), now)
	if err != nil {
		return model.PublishResult{}, err
	}
	if req.IdempotencyKey != "" {
		actor.idempotent.put(req.IdempotencyKey, ev.Seq, now)
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
	if info.ActiveRunStartSeq != 0 {
		actor.runActiveAt = now
	}
	actor.fanOutLocked(eventDelivery(ev))
	return ev, nil
}

// idempotentReplayLocked returns the original result for a retried publish.
func (b *Broker) idempotentReplayLocked(actor *sessionActor, key string, now time.Time) (model.PublishResult, bool) {
	if key == "" {
		return model.PublishResult{}, false
	}
	seq, ok := actor.idempotent.get(key, now)
	if !ok {
		return model.PublishResult{}, false
	}
	return model.PublishResult{Epoch: actor.epoch, Seq: seq, Deduplicated: true}, true
}
