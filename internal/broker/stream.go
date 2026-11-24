package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

type streamCandidate struct {
	seq        uint64
	baseType   string
	evType     string
	baseMerged string
	meta       json.RawMessage
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

func newStreamCandidate(inFlight *inFlightState, req model.PublishRequest, seq uint64) streamCandidate {
	baseType, evType := resolveCandidateTypes(inFlight, req.Type)
	cand := streamCandidate{seq: seq, baseType: baseType, evType: evType}
	if inFlight != nil {
		cand.baseMerged = inFlight.mergedBuf.String()
		cand.meta = inFlight.meta
	} else {
		cand.meta = normalizeRaw(req.Meta)
	}
	return cand
}

func deltaEvent(epoch, streamID string, cand streamCandidate, delta string, fin bool, now time.Time) model.OutEvent {
	return model.OutEvent{
		Kind:      model.EventDelta,
		Epoch:     epoch,
		StreamID:  streamID,
		Seq:       cand.seq,
		Type:      cand.evType,
		Delta:     delta,
		Fin:       fin,
		CreatedAt: now,
	}
}

// finalizedReplayLocked handles chunks for a stream that was already compacted (by fin=true or
// by idle sealing): exact retries are deduplicated, anything else is rejected.
func (b *Broker) finalizedReplayLocked(actor *sessionActor, req model.PublishRequest, now time.Time) (model.PublishResult, bool, error) {
	f, ok := actor.finalized.get(req.StreamID, now)
	if !ok {
		return model.PublishResult{}, false, nil
	}
	if req.Seq != 0 && req.Seq <= f.lastSeq {
		return model.PublishResult{
			CommittedOffset: f.offset,
			Epoch:           actor.meta.Epoch,
			InFlightSeq:     f.lastSeq,
			Deduplicated:    true,
		}, true, nil
	}
	return model.PublishResult{}, true, ErrStreamFinalized
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

// checkStreamCapacityLocked enforces MaxConcurrentStreams and MaxInFlightBytes before any
// state changes. A single-chunk stream (fin on its first chunk) never occupies a slot, and a
// fin chunk with an explicit payload does not grow the merged buffer.
func (b *Broker) checkStreamCapacityLocked(actor *sessionActor, inFlight *inFlightState, req model.PublishRequest, hasPayload bool) error {
	if inFlight == nil && !req.Fin && len(actor.inFlights) >= b.cfg.MaxConcurrentStreams {
		return ErrTooManyStreams
	}
	if req.Fin && hasPayload {
		return nil
	}
	growth := len(req.Delta)
	if inFlight == nil {
		growth += len(normalizeRaw(req.Meta))
	}
	if actor.inFlightBytes+growth > b.cfg.MaxInFlightBytes {
		return ErrInFlightQuotaExceeded
	}
	return nil
}

func (b *Broker) publishStreamLocked(
	ctx context.Context,
	actor *sessionActor,
	req model.PublishRequest,
	hasPayload bool,
	now time.Time,
) (model.PublishResult, error) {
	inFlight := actor.inFlights[req.StreamID]
	if inFlight == nil {
		if res, done, err := b.finalizedReplayLocked(actor, req, now); done {
			return res, err
		}
	}

	seq, dup, err := resolveStreamSeq(actor.meta.Epoch, inFlight, req.Seq)
	if err != nil {
		return model.PublishResult{}, err
	}
	if dup != nil {
		return *dup, nil
	}
	if err := b.checkStreamCapacityLocked(actor, inFlight, req, hasPayload); err != nil {
		return model.PublishResult{}, err
	}

	cand := newStreamCandidate(inFlight, req, seq)
	if !req.Fin {
		return b.appendStreamDeltaLocked(ctx, actor, inFlight, req, cand, now), nil
	}
	return b.commitFinStreamLocked(ctx, actor, req, cand, hasPayload, now)
}

func (b *Broker) appendStreamDeltaLocked(
	ctx context.Context,
	actor *sessionActor,
	inFlight *inFlightState,
	req model.PublishRequest,
	cand streamCandidate,
	now time.Time,
) model.PublishResult {
	first := inFlight == nil
	if first {
		inFlight = &inFlightState{
			streamID:  req.StreamID,
			meta:      cand.meta,
			startedAt: now,
		}
		actor.inFlights[req.StreamID] = inFlight
		actor.inFlightBytes += len(cand.meta)
	}
	inFlight.typ = cand.baseType
	_, _ = inFlight.mergedBuf.WriteString(req.Delta)
	actor.inFlightBytes += len(req.Delta)
	inFlight.lastSeq = cand.seq
	inFlight.updatedAt = now

	needMetaSave := !actor.metaPersisted || actor.meta.State != model.StateRunning
	actor.meta.State = model.StateRunning
	actor.meta.UpdatedAt = now

	ev := deltaEvent(actor.meta.Epoch, req.StreamID, cand, req.Delta, false, now)
	if first {
		ev.Meta = cand.meta
	}
	actor.fanOutLocked(ev)
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

// settledState is the session state right after a stream is compacted into rec.
func settledState(recordType string, hasInFlight bool) model.SessionState {
	if terminal := model.StateFromRecordType(recordType); terminal != model.StateIdle {
		return terminal
	}
	if hasInFlight {
		return model.StateRunning
	}
	return model.StateIdle
}

// finishStreamLocked applies the in-memory effects of a committed stream Record. It must only
// run after store.Append succeeded (point of no return).
func (b *Broker) finishStreamLocked(actor *sessionActor, streamID string, lastSeq uint64, rec model.Record, now time.Time) {
	if f := actor.inFlights[streamID]; f != nil {
		actor.inFlightBytes -= f.bytes()
		delete(actor.inFlights, streamID)
	}
	actor.finalized.put(streamID, finalizedStream{lastSeq: lastSeq, offset: rec.Offset}, now)
	actor.lastOffset = rec.Offset
	actor.meta.LastOffset = rec.Offset
	actor.meta.UpdatedAt = rec.CreatedAt
	actor.meta.State = settledState(rec.Type, len(actor.inFlights) > 0)
}

func (b *Broker) commitFinStreamLocked(
	ctx context.Context,
	actor *sessionActor,
	req model.PublishRequest,
	cand streamCandidate,
	hasPayload bool,
	now time.Time,
) (model.PublishResult, error) {
	recordPayload, err := resolveRecordPayload(hasPayload, req.Payload, cand.baseMerged+req.Delta)
	if err != nil {
		return model.PublishResult{}, err
	}

	rec := model.Record{
		Offset:    actor.lastOffset + 1,
		Epoch:     actor.meta.Epoch,
		StreamID:  req.StreamID,
		Type:      resolveFinRecordType(cand.baseType, req.Type),
		Payload:   recordPayload,
		Meta:      cand.meta,
		CreatedAt: now,
	}
	_, wasInFlight := actor.inFlights[req.StreamID]
	if err := b.store.Append(ctx, actor.meta.SessionID, rec); err != nil {
		return model.PublishResult{}, err
	}

	// Point of no return: the compacted record is committed.
	b.finishStreamLocked(actor, req.StreamID, cand.seq, rec, now)

	finDelta := deltaEvent(rec.Epoch, req.StreamID, cand, req.Delta, true, now)
	if !wasInFlight {
		finDelta.Meta = cand.meta
	}
	actor.fanOutLocked(finDelta)
	actor.fanOutLocked(committedEvent(rec))
	actor.saveMetaBestEffortLocked(ctx, b.store)

	return model.PublishResult{
		CommittedOffset: rec.Offset,
		Epoch:           rec.Epoch,
		InFlightSeq:     cand.seq,
	}, nil
}

// sealStreamLocked compacts an in-flight stream whose producer went silent into a Record marked
// Sealed=idle_timeout, so the partial text is kept and its slot and quota are released.
func (b *Broker) sealStreamLocked(actor *sessionActor, f *inFlightState, now time.Time) error {
	payload, err := marshalContentPayload(f.mergedBuf.String())
	if err != nil {
		return err
	}
	rec := model.Record{
		Offset:    actor.lastOffset + 1,
		Epoch:     actor.meta.Epoch,
		StreamID:  f.streamID,
		Type:      f.typ,
		Payload:   payload,
		Meta:      f.meta,
		Sealed:    model.SealedIdleTimeout,
		CreatedAt: now.UTC(),
	}
	ctx := context.Background()
	if err := b.store.Append(ctx, actor.meta.SessionID, rec); err != nil {
		return err
	}

	// Point of no return.
	b.finishStreamLocked(actor, f.streamID, f.lastSeq, rec, now)
	actor.fanOutLocked(committedEvent(rec))
	actor.saveMetaBestEffortLocked(ctx, b.store)
	return nil
}
