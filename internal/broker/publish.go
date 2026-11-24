package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

type contentPayload struct {
	Content string `json:"content"`
}

func marshalContentPayload(content string) (json.RawMessage, error) {
	raw, err := json.Marshal(contentPayload{Content: content})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	cp := make(json.RawMessage, len(raw))
	copy(cp, raw)
	return cp
}

func resolveRecordPayload(hasPayload bool, raw json.RawMessage, text string) (json.RawMessage, error) {
	if hasPayload {
		return cloneRawMessage(raw), nil
	}
	return marshalContentPayload(text)
}

func validatePublishRequest(sessionID string, req model.PublishRequest) (bool, error) {
	if sessionID == "" {
		return false, ErrInvalidInput
	}
	hasPayload := len(bytes.TrimSpace(req.Payload)) > 0
	if req.StreamID == "" {
		if !hasPayload && req.Delta == "" {
			return false, ErrInvalidInput
		}
		if hasPayload && !json.Valid(req.Payload) {
			return false, ErrInvalidInput
		}
		return hasPayload, nil
	}
	if req.Fin && hasPayload && !json.Valid(req.Payload) {
		return false, ErrInvalidInput
	}
	return hasPayload, nil
}

func (b *Broker) checkActorPublishReady(ctx context.Context) error {
	select {
	case <-b.doneCh:
		return ErrBrokerClosed
	default:
	}
	return ctx.Err()
}

func (b *Broker) Publish(ctx context.Context, sessionID string, req model.PublishRequest) (model.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return model.PublishResult{}, err
	}
	if b.isClosed() {
		return model.PublishResult{}, ErrBrokerClosed
	}

	hasPayload, err := validatePublishRequest(sessionID, req)
	if err != nil {
		return model.PublishResult{}, err
	}

	actor, _, err := b.acquireActor(ctx, sessionID, true, false)
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

	if req.StreamID == "" {
		return b.publishAtomicLocked(ctx, sessionID, actor, req, hasPayload, now)
	}
	return b.publishStreamLocked(ctx, sessionID, actor, req, hasPayload, now)
}

func nextAtomicState(reqType string, hasInFlight bool, current model.SessionState) model.SessionState {
	if terminal := model.StateFromRecordType(reqType); terminal != model.StateIdle {
		return terminal
	}
	if hasInFlight {
		return model.StateRunning
	}
	if current == "" {
		return model.StateIdle
	}
	return current
}

func (b *Broker) publishAtomicLocked(
	ctx context.Context,
	sessionID string,
	actor *sessionActor,
	req model.PublishRequest,
	hasPayload bool,
	now time.Time,
) (model.PublishResult, error) {
	payload, err := resolveRecordPayload(hasPayload, req.Payload, req.Delta)
	if err != nil {
		return model.PublishResult{}, err
	}

	nextOffset := actor.lastOffset + 1
	rec := model.Record{
		Offset:    nextOffset,
		Epoch:     actor.meta.Epoch,
		Type:      req.Type,
		Payload:   payload,
		CreatedAt: now,
	}
	if err := b.store.Append(ctx, sessionID, rec); err != nil {
		return model.PublishResult{}, err
	}

	// Point of no return: record is committed in storage. Advance memory state and fan out first.
	actor.lastOffset = nextOffset
	actor.meta.LastOffset = nextOffset
	actor.meta.UpdatedAt = now
	actor.meta.State = nextAtomicState(req.Type, actor.inFlight != nil, actor.meta.State)

	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventCommitted,
		Offset:    rec.Offset,
		Epoch:     rec.Epoch,
		Type:      rec.Type,
		Payload:   rec.Payload,
		CreatedAt: rec.CreatedAt,
	})
	actor.saveMetaBestEffortLocked(ctx, b.store)

	return model.PublishResult{
		CommittedOffset: rec.Offset,
		Epoch:           actor.meta.Epoch,
	}, nil
}
