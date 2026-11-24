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

func hasJSON(raw json.RawMessage) bool {
	return len(bytes.TrimSpace(raw)) > 0
}

// normalizeRaw returns a private copy of raw, or nil when it is empty or whitespace.
func normalizeRaw(raw json.RawMessage) json.RawMessage {
	if !hasJSON(raw) {
		return nil
	}
	return cloneRawMessage(raw)
}

func resolveRecordPayload(hasPayload bool, raw json.RawMessage, text string) (json.RawMessage, error) {
	if hasPayload {
		return cloneRawMessage(raw), nil
	}
	return marshalContentPayload(text)
}

func committedEvent(rec model.Record) model.OutEvent {
	return model.OutEvent{
		Kind:      model.EventCommitted,
		Offset:    rec.Offset,
		Epoch:     rec.Epoch,
		StreamID:  rec.StreamID,
		Type:      rec.Type,
		Payload:   rec.Payload,
		Meta:      rec.Meta,
		CreatedAt: rec.CreatedAt,
	}
}

// validOptionalJSON accepts an absent (empty or whitespace) value or valid JSON.
func validOptionalJSON(raw json.RawMessage) bool {
	return !hasJSON(raw) || json.Valid(raw)
}

// validPublishShape rejects an atomic event that carries neither payload nor delta.
func validPublishShape(req model.PublishRequest, hasPayload bool) bool {
	return req.StreamID != "" || hasPayload || req.Delta != ""
}

func (b *Broker) validatePublishRequest(sessionID string, req model.PublishRequest) (bool, error) {
	if sessionID == "" || !validOptionalJSON(req.Payload) || !validOptionalJSON(req.Meta) {
		return false, ErrInvalidInput
	}
	if len(req.Delta)+len(req.Payload)+len(req.Meta) > b.cfg.MaxEventBytes {
		return false, ErrEventTooLarge
	}
	hasPayload := hasJSON(req.Payload)
	if !validPublishShape(req, hasPayload) {
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

	hasPayload, err := b.validatePublishRequest(sessionID, req)
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
		return b.publishAtomicLocked(ctx, actor, req, hasPayload, now)
	}
	return b.publishStreamLocked(ctx, actor, req, hasPayload, now)
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
	actor *sessionActor,
	req model.PublishRequest,
	hasPayload bool,
	now time.Time,
) (model.PublishResult, error) {
	payload, err := resolveRecordPayload(hasPayload, req.Payload, req.Delta)
	if err != nil {
		return model.PublishResult{}, err
	}

	rec := model.Record{
		Offset:    actor.lastOffset + 1,
		Epoch:     actor.meta.Epoch,
		Type:      req.Type,
		Payload:   payload,
		Meta:      normalizeRaw(req.Meta),
		CreatedAt: now,
	}
	if err := b.store.Append(ctx, actor.meta.SessionID, rec); err != nil {
		return model.PublishResult{}, err
	}

	// Point of no return: the record is committed. Advance memory state and fan out first.
	actor.lastOffset = rec.Offset
	actor.meta.LastOffset = rec.Offset
	actor.meta.UpdatedAt = now
	actor.meta.State = nextAtomicState(req.Type, len(actor.inFlights) > 0, actor.meta.State)

	actor.fanOutLocked(committedEvent(rec))
	actor.saveMetaBestEffortLocked(ctx, b.store)

	return model.PublishResult{
		CommittedOffset: rec.Offset,
		Epoch:           rec.Epoch,
	}, nil
}
