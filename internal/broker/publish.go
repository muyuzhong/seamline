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
		Epoch:     b.epoch,
		Type:      req.Type,
		Payload:   payload,
		CreatedAt: now,
	}
	if err := b.store.Append(ctx, sessionID, rec); err != nil {
		return model.PublishResult{}, err
	}

	// Point of no return: record is committed in storage. Advance memory state and fan out first.
	actor.lastOffset = nextOffset
	actor.meta.Epoch = b.epoch
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
		Epoch:           b.epoch,
	}, nil
}

type streamCandidate struct {
	seq        uint64
	baseType   string
	evType     string
	baseMerged string
}

func (b *Broker) isCompactedDuplicateLocked(actor *sessionActor, req model.PublishRequest) (model.PublishResult, bool) {
	if actor.inFlight != nil || actor.lastCompactedStreamID == "" || req.StreamID != actor.lastCompactedStreamID {
		return model.PublishResult{}, false
	}
	if req.Seq == 0 || req.Seq > actor.lastCompactedSeq {
		return model.PublishResult{}, false
	}
	return model.PublishResult{
		CommittedOffset: actor.lastCompactedOffset,
		Epoch:           b.epoch,
		InFlightSeq:     actor.lastCompactedSeq,
		Deduplicated:    true,
	}, true
}

func (b *Broker) resolveStreamSeqLocked(inFlight *inFlightState, reqSeq uint64) (uint64, *model.PublishResult, error) {
	if inFlight == nil {
		if reqSeq > 1 {
			return 0, nil, ErrSequenceGap
		}
		return 1, nil, nil
	}
	if reqSeq == 0 {
		return inFlight.lastSeq + 1, nil, nil
	}
	if reqSeq <= inFlight.lastSeq {
		return 0, &model.PublishResult{
			Epoch:        b.epoch,
			InFlightSeq:  inFlight.lastSeq,
			Deduplicated: true,
		}, nil
	}
	if reqSeq > inFlight.lastSeq+1 {
		return 0, nil, ErrSequenceGap
	}
	return reqSeq, nil, nil
}

func resolveCandidateTypes(inFlight *inFlightState, reqType string) (string, string) {
	baseType := reqType
	if inFlight != nil && inFlight.typ != "" {
		baseType = inFlight.typ
	}
	evType := reqType
	if evType == "" {
		evType = baseType
	}
	return baseType, evType
}

func (b *Broker) publishStreamLocked(
	ctx context.Context,
	sessionID string,
	actor *sessionActor,
	req model.PublishRequest,
	hasPayload bool,
	now time.Time,
) (model.PublishResult, error) {
	if dupRes, ok := b.isCompactedDuplicateLocked(actor, req); ok {
		return dupRes, nil
	}
	if actor.inFlight != nil && actor.inFlight.streamID != req.StreamID {
		return model.PublishResult{}, ErrStreamConflict
	}

	seq, dupInFlight, err := b.resolveStreamSeqLocked(actor.inFlight, req.Seq)
	if err != nil {
		return model.PublishResult{}, err
	}
	if dupInFlight != nil {
		return *dupInFlight, nil
	}

	baseType, evType := resolveCandidateTypes(actor.inFlight, req.Type)
	cand := streamCandidate{
		seq:      seq,
		baseType: baseType,
		evType:   evType,
	}
	if req.Fin && actor.inFlight != nil {
		cand.baseMerged = actor.inFlight.mergedBuf.String()
	}

	if !req.Fin {
		return b.appendStreamDeltaLocked(ctx, actor, req, cand, now), nil
	}
	return b.commitFinStreamLocked(ctx, sessionID, actor, req, cand, hasPayload, now)
}

func (b *Broker) appendStreamDeltaLocked(
	ctx context.Context,
	actor *sessionActor,
	req model.PublishRequest,
	cand streamCandidate,
	now time.Time,
) model.PublishResult {
	if actor.inFlight == nil {
		actor.inFlight = &inFlightState{
			streamID:  req.StreamID,
			typ:       cand.baseType,
			startedAt: now,
		}
	} else {
		actor.inFlight.typ = cand.baseType
	}
	_, _ = actor.inFlight.mergedBuf.WriteString(req.Delta)
	actor.inFlight.lastSeq = cand.seq

	needMetaSave := !actor.metaPersisted || actor.meta.State != model.StateRunning
	actor.meta.State = model.StateRunning
	actor.meta.Epoch = b.epoch
	actor.meta.UpdatedAt = now

	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventDelta,
		Epoch:     b.epoch,
		StreamID:  req.StreamID,
		Seq:       cand.seq,
		Type:      cand.evType,
		Delta:     req.Delta,
		Fin:       false,
		CreatedAt: now,
	})
	if needMetaSave {
		actor.saveMetaBestEffortLocked(ctx, b.store)
	}

	return model.PublishResult{
		Epoch:       b.epoch,
		InFlightSeq: cand.seq,
	}
}

func resolveFinRecordType(baseType string, finType string) string {
	if model.StateFromRecordType(finType) != model.StateIdle {
		return finType
	}
	if baseType != "" {
		return baseType
	}
	return finType
}

func (b *Broker) commitFinStreamLocked(
	ctx context.Context,
	sessionID string,
	actor *sessionActor,
	req model.PublishRequest,
	cand streamCandidate,
	hasPayload bool,
	now time.Time,
) (model.PublishResult, error) {
	mergedText := cand.baseMerged + req.Delta
	recordPayload, err := resolveRecordPayload(hasPayload, req.Payload, mergedText)
	if err != nil {
		return model.PublishResult{}, err
	}

	recordType := resolveFinRecordType(cand.baseType, req.Type)

	nextOffset := actor.lastOffset + 1
	rec := model.Record{
		Offset:    nextOffset,
		Epoch:     b.epoch,
		StreamID:  req.StreamID,
		Type:      recordType,
		Payload:   recordPayload,
		CreatedAt: now,
	}
	if err := b.store.Append(ctx, sessionID, rec); err != nil {
		return model.PublishResult{}, err
	}

	// Point of no return: compacted record is committed in storage.
	actor.lastCompactedStreamID = req.StreamID
	actor.lastCompactedSeq = cand.seq
	actor.lastCompactedOffset = rec.Offset
	actor.inFlight = nil
	actor.lastOffset = nextOffset
	actor.meta.Epoch = b.epoch
	actor.meta.State = model.StateFromRecordType(recordType)
	actor.meta.LastOffset = nextOffset
	actor.meta.UpdatedAt = now

	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventDelta,
		Epoch:     b.epoch,
		StreamID:  req.StreamID,
		Seq:       cand.seq,
		Type:      cand.evType,
		Delta:     req.Delta,
		Fin:       true,
		CreatedAt: now,
	})
	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventCommitted,
		Offset:    rec.Offset,
		Epoch:     rec.Epoch,
		StreamID:  rec.StreamID,
		Type:      rec.Type,
		Payload:   rec.Payload,
		CreatedAt: rec.CreatedAt,
	})
	actor.saveMetaBestEffortLocked(ctx, b.store)

	return model.PublishResult{
		CommittedOffset: rec.Offset,
		Epoch:           b.epoch,
		InFlightSeq:     cand.seq,
	}, nil
}
