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

// normalizeRaw returns a private copy of raw, or nil when it is empty or whitespace.
func normalizeRaw(raw json.RawMessage) json.RawMessage {
	if !hasJSON(raw) {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
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

	now := time.Now().UTC()
	actor.touch()

	c := actor.nextCommit(req.Type, req.Payload, now)
	if err := b.store.Append(ctx, sessionID, c); err != nil {
		return model.PublishResult{}, err
	}

	// Point of no return: the event and its session state are committed.
	actor.applyCommitLocked(c, now)
	actor.fanOutLocked(eventDelivery(c.Event))
	return model.PublishResult{Epoch: c.Event.Epoch, Seq: c.Event.Seq}, nil
}
