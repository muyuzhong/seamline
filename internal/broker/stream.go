package broker

import (
	"context"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

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
		Epoch:           actor.meta.Epoch,
		InFlightSeq:     actor.lastCompactedSeq,
		Deduplicated:    true,
	}, true
}

func resolveStreamSeq(epoch string, inFlight *inFlightState, reqSeq uint64) (uint64, *model.PublishResult, error) {
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
			Epoch:        epoch,
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

	seq, dupInFlight, err := resolveStreamSeq(actor.meta.Epoch, actor.inFlight, req.Seq)
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
	actor.meta.UpdatedAt = now

	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventDelta,
		Epoch:     actor.meta.Epoch,
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
		Epoch:       actor.meta.Epoch,
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
		Epoch:     actor.meta.Epoch,
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
	actor.meta.State = model.StateFromRecordType(recordType)
	actor.meta.LastOffset = nextOffset
	actor.meta.UpdatedAt = now

	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventDelta,
		Epoch:     actor.meta.Epoch,
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
		Epoch:           actor.meta.Epoch,
		InFlightSeq:     cand.seq,
	}, nil
}
