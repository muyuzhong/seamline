package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

var (
	ErrSlowSubscriber = errors.New("subscriber evicted due to slow consumption")
	ErrInvalidInput   = errors.New("invalid publish request")
	ErrSequenceGap    = errors.New("out-of-order stream sequence")
	ErrStreamConflict = errors.New("another stream is currently in-flight for this session")
	ErrBrokerClosed   = errors.New("broker closed")
)

const (
	defaultSubscriberBufferSize = 128
	defaultIdleSessionTTL       = 30 * time.Minute
	defaultSweepInterval        = 1 * time.Minute
)

type Config struct {
	SubscriberBufferSize int           // default 128 if <= 0
	IdleSessionTTL       time.Duration // default 30m if <= 0
	SweepInterval        time.Duration // default 1m if <= 0
	Epoch                string        // random 12-char hex generated on New if empty
}

type Subscription struct {
	Epoch  string
	Events <-chan model.OutEvent
	Cancel func()
	Err    func() error
}

type inFlightState struct {
	streamID  string
	typ       string
	mergedBuf strings.Builder
	lastSeq   uint64
	startedAt time.Time
}

type subState struct {
	liveCh     chan model.OutEvent
	cancelCh   chan struct{}
	cancelOnce sync.Once
	evictedCh  chan struct{}
	errMu      sync.Mutex
	err        error
}

func (s *subState) cancel() {
	s.cancelOnce.Do(func() {
		close(s.cancelCh)
	})
}

func (s *subState) setErr(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

func (s *subState) getErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

type sessionActor struct {
	mu                    sync.Mutex
	meta                  model.SessionMeta
	metaPersisted         bool
	metaFlushTried        bool
	lastOffset            uint64
	inFlight              *inFlightState
	lastCompactedStreamID string
	lastCompactedSeq      uint64
	lastCompactedOffset   uint64
	subscribers           map[*subState]struct{}
	lastActiveNano        atomic.Int64
	pins                  atomic.Int32
}

func (a *sessionActor) touch() {
	a.lastActiveNano.Store(time.Now().UnixNano())
}

func (a *sessionActor) unpin() {
	a.touch()
	a.pins.Add(-1)
}

func (a *sessionActor) inFlightViewLocked() *model.InFlightView {
	if a.inFlight == nil {
		return nil
	}
	return &model.InFlightView{
		StreamID:  a.inFlight.streamID,
		Type:      a.inFlight.typ,
		Merged:    a.inFlight.mergedBuf.String(),
		LastSeq:   a.inFlight.lastSeq,
		StartedAt: a.inFlight.startedAt,
	}
}

func (a *sessionActor) snapshotLocked() model.SessionSnapshot {
	return model.SessionSnapshot{
		Meta:            a.meta,
		InFlight:        a.inFlightViewLocked(),
		SubscriberCount: len(a.subscribers),
	}
}

func (a *sessionActor) saveMetaBestEffortLocked(ctx context.Context, store storage.Storage) {
	a.metaFlushTried = false
	if err := store.SaveMeta(context.WithoutCancel(ctx), a.meta); err == nil {
		a.metaPersisted = true
	} else {
		a.metaPersisted = false
	}
}

func (a *sessionActor) fanOutLocked(ev model.OutEvent) {
	for sub := range a.subscribers {
		select {
		case sub.liveCh <- ev:
		default:
			delete(a.subscribers, sub)
			sub.setErr(ErrSlowSubscriber)
			close(sub.liveCh)
			close(sub.evictedCh)
		}
	}
}

type Broker struct {
	store   storage.Storage
	cfg     Config
	epoch   string
	mu      sync.RWMutex
	closed  bool
	doneCh  chan struct{}
	actors  map[string]*sessionActor
	sweepWg sync.WaitGroup
	subWg   sync.WaitGroup
}

func New(store storage.Storage, cfg Config) *Broker {
	if store == nil {
		store = storage.NewMemoryStorage()
	}
	if cfg.SubscriberBufferSize <= 0 {
		cfg.SubscriberBufferSize = defaultSubscriberBufferSize
	}
	if cfg.IdleSessionTTL <= 0 {
		cfg.IdleSessionTTL = defaultIdleSessionTTL
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaultSweepInterval
	}
	if cfg.Epoch == "" {
		cfg.Epoch = randomHex(6)
	}

	b := &Broker{
		store:  store,
		cfg:    cfg,
		epoch:  cfg.Epoch,
		doneCh: make(chan struct{}),
		actors: make(map[string]*sessionActor),
	}

	b.sweepWg.Add(1)
	go b.sweepLoop()
	return b
}

func randomHex(numBytes int) string {
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%012x", time.Now().UnixNano()&0xffffffffffff)
	}
	return hex.EncodeToString(buf)
}

func (b *Broker) Epoch() string {
	return b.epoch
}

func (b *Broker) isClosed() bool {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	return closed
}

// activeActorCount returns the number of in-memory session actors (used by tests).
func (b *Broker) activeActorCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.actors)
}

func (b *Broker) sweepLoop() {
	defer b.sweepWg.Done()
	ticker := time.NewTicker(b.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.doneCh:
			return
		case <-ticker.C:
			b.evictIdleActors(time.Now())
		}
	}
}

func (b *Broker) evictIdleActors(now time.Time) {
	var toFlush []*sessionActor

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}

	for id, actor := range b.actors {
		if actor.pins.Load() > 0 {
			continue
		}
		if !actor.mu.TryLock() {
			continue
		}
		lastActive := time.Unix(0, actor.lastActiveNano.Load())
		if actor.pins.Load() == 0 && len(actor.subscribers) == 0 && actor.inFlight == nil && now.Sub(lastActive) > b.cfg.IdleSessionTTL {
			if !actor.metaPersisted && !actor.metaFlushTried {
				actor.pins.Add(1)
				toFlush = append(toFlush, actor)
			} else {
				delete(b.actors, id)
			}
		}
		actor.mu.Unlock()
	}
	b.mu.Unlock()

	for _, actor := range toFlush {
		actor.mu.Lock()
		if !actor.metaPersisted {
			actor.metaFlushTried = true
			if err := b.store.SaveMeta(context.Background(), actor.meta); err == nil {
				actor.metaPersisted = true
			}
		}
		actor.mu.Unlock()
		actor.pins.Add(-1)
	}
}

func reconcileStateFromTail(recType string, current model.SessionState) model.SessionState {
	switch recType {
	case "run_finished":
		return model.StateCompleted
	case "run_failed":
		return model.StateFailed
	default:
		if current == "" || current == model.StateRunning {
			return model.StateIdle
		}
		return current
	}
}

// acquireActor finds or hydrates a sessionActor and increments its atomic pin
// count so the background sweeper cannot evict it while an operation runs.
func (b *Broker) acquireActor(ctx context.Context, sessionID string, createIfMissing bool, persistInitialMeta bool) (*sessionActor, bool, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, false, ErrBrokerClosed
	}
	if actor, ok := b.actors[sessionID]; ok {
		actor.pins.Add(1)
		actor.touch()
		b.mu.RUnlock()
		return actor, true, nil
	}
	b.mu.RUnlock()

	// Load from storage outside b.mu so storage I/O never blocks other sessions.
	meta, exists, err := b.store.LoadMeta(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}

	var startOffset uint64
	if exists {
		startOffset = meta.LastOffset
	}

	// Catch up startOffset and state if storage has newer committed records than saved meta.
	tailRecs, err := b.store.ReadFrom(ctx, sessionID, startOffset)
	if err != nil {
		return nil, false, err
	}
	needsMetaHeal := false
	if len(tailRecs) > 0 {
		lastRec := tailRecs[len(tailRecs)-1]
		startOffset = lastRec.Offset
		needsMetaHeal = true
		if !exists {
			exists = true
			meta = model.SessionMeta{
				SessionID:  sessionID,
				Epoch:      b.epoch,
				State:      reconcileStateFromTail(lastRec.Type, model.StateIdle),
				LastOffset: startOffset,
				CreatedAt:  tailRecs[0].CreatedAt,
				UpdatedAt:  lastRec.CreatedAt,
			}
		} else {
			meta.LastOffset = startOffset
			meta.UpdatedAt = lastRec.CreatedAt
			meta.State = reconcileStateFromTail(lastRec.Type, model.StateIdle)
		}
	}

	if !exists && !createIfMissing {
		return nil, false, nil
	}

	now := time.Now().UTC()
	persisted := false
	if exists {
		persisted = !needsMetaHeal
		meta.Epoch = b.epoch
		if meta.State == model.StateRunning {
			// Cold reload has no in-memory stream; normalize state to idle.
			meta.State = model.StateIdle
		}
	} else {
		meta = model.SessionMeta{
			SessionID:  sessionID,
			Epoch:      b.epoch,
			State:      model.StateIdle,
			LastOffset: 0,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
	}

	candidate := &sessionActor{
		meta:          meta,
		metaPersisted: persisted,
		lastOffset:    meta.LastOffset,
		subscribers:   make(map[*subState]struct{}),
	}
	candidate.pins.Store(1)
	candidate.touch()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, false, ErrBrokerClosed
	}
	if existing, ok := b.actors[sessionID]; ok {
		existing.pins.Add(1)
		existing.touch()
		b.mu.Unlock()
		return existing, true, nil
	}
	b.actors[sessionID] = candidate
	b.mu.Unlock()

	if (persistInitialMeta && !persisted) || needsMetaHeal {
		candidate.mu.Lock()
		if !candidate.metaPersisted {
			if err := b.store.SaveMeta(context.WithoutCancel(ctx), candidate.meta); err == nil {
				candidate.metaPersisted = true
			}
		}
		candidate.mu.Unlock()
	}

	return candidate, true, nil
}

func (b *Broker) CreateSession(ctx context.Context, sessionID string) (model.SessionMeta, error) {
	if err := ctx.Err(); err != nil {
		return model.SessionMeta{}, err
	}
	if b.isClosed() {
		return model.SessionMeta{}, ErrBrokerClosed
	}
	if sessionID == "" {
		sessionID = "sess_" + randomHex(6)
	}

	actor, _, err := b.acquireActor(ctx, sessionID, true, false)
	if err != nil {
		return model.SessionMeta{}, err
	}
	defer actor.unpin()

	actor.mu.Lock()
	if !actor.metaPersisted {
		if err := b.store.SaveMeta(ctx, actor.meta); err != nil {
			actor.mu.Unlock()
			return model.SessionMeta{}, err
		}
		actor.metaPersisted = true
	}
	meta := actor.meta
	actor.mu.Unlock()
	return meta, nil
}

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

func (b *Broker) Publish(ctx context.Context, sessionID string, req model.PublishRequest) (model.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return model.PublishResult{}, err
	}
	if b.isClosed() {
		return model.PublishResult{}, ErrBrokerClosed
	}
	if sessionID == "" {
		return model.PublishResult{}, ErrInvalidInput
	}

	// Pre-validate payload inputs before acquiring actor.
	hasPayload := len(bytes.TrimSpace(req.Payload)) > 0
	if req.StreamID == "" {
		if hasPayload {
			if !json.Valid(req.Payload) {
				return model.PublishResult{}, ErrInvalidInput
			}
		} else if req.Delta == "" {
			return model.PublishResult{}, ErrInvalidInput
		}
	} else if req.Fin && hasPayload {
		if !json.Valid(req.Payload) {
			return model.PublishResult{}, ErrInvalidInput
		}
	}

	actor, _, err := b.acquireActor(ctx, sessionID, true, false)
	if err != nil {
		return model.PublishResult{}, err
	}
	defer actor.unpin()

	actor.mu.Lock()
	defer actor.mu.Unlock()

	select {
	case <-b.doneCh:
		return model.PublishResult{}, ErrBrokerClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return model.PublishResult{}, err
	}

	now := time.Now().UTC()
	actor.touch()

	// Path 1: Atomic committed event (no StreamID).
	if req.StreamID == "" {
		var payload json.RawMessage
		if hasPayload {
			payload = cloneRawMessage(req.Payload)
		} else {
			payload, err = marshalContentPayload(req.Delta)
			if err != nil {
				return model.PublishResult{}, err
			}
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
		switch req.Type {
		case "run_finished":
			actor.meta.State = model.StateCompleted
		case "run_failed":
			actor.meta.State = model.StateFailed
		default:
			if actor.inFlight != nil {
				actor.meta.State = model.StateRunning
			} else if actor.meta.State == "" {
				actor.meta.State = model.StateIdle
			}
		}

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

	// Path 2: Streaming delta (req.StreamID != "").
	// Validate sequence & conflict and compute candidate values in local variables
	// BEFORE mutating actor or actor.inFlight state.
	var (
		seq        uint64
		baseType   string
		baseMerged string
	)

	if actor.inFlight == nil {
		// Check if this is a late duplicate retry of the just-compacted stream.
		if actor.lastCompactedStreamID != "" && req.StreamID == actor.lastCompactedStreamID {
			if req.Seq > 0 && req.Seq <= actor.lastCompactedSeq {
				return model.PublishResult{
					CommittedOffset: actor.lastCompactedOffset,
					Epoch:           b.epoch,
					InFlightSeq:     actor.lastCompactedSeq,
					Deduplicated:    true,
				}, nil
			}
		}
		if req.Seq > 1 {
			return model.PublishResult{}, ErrSequenceGap
		}
		seq = 1
		baseType = req.Type
	} else {
		if actor.inFlight.streamID != req.StreamID {
			return model.PublishResult{}, ErrStreamConflict
		}
		if req.Seq > 0 {
			if req.Seq <= actor.inFlight.lastSeq {
				return model.PublishResult{
					Epoch:        b.epoch,
					InFlightSeq:  actor.inFlight.lastSeq,
					Deduplicated: true,
				}, nil
			}
			if req.Seq > actor.inFlight.lastSeq+1 {
				return model.PublishResult{}, ErrSequenceGap
			}
			seq = req.Seq
		} else {
			seq = actor.inFlight.lastSeq + 1
		}
		baseType = actor.inFlight.typ
		if baseType == "" && req.Type != "" {
			baseType = req.Type
		}
		if req.Fin {
			baseMerged = actor.inFlight.mergedBuf.String()
		}
	}

	evType := req.Type
	if evType == "" {
		evType = baseType
	}

	if !req.Fin {
		if actor.inFlight == nil {
			actor.inFlight = &inFlightState{
				streamID:  req.StreamID,
				typ:       baseType,
				startedAt: now,
			}
		} else {
			actor.inFlight.typ = baseType
		}
		_, _ = actor.inFlight.mergedBuf.WriteString(req.Delta)
		actor.inFlight.lastSeq = seq

		needMetaSave := !actor.metaPersisted || actor.meta.State != model.StateRunning
		actor.meta.State = model.StateRunning
		actor.meta.Epoch = b.epoch
		actor.meta.UpdatedAt = now

		actor.fanOutLocked(model.OutEvent{
			Kind:      model.EventDelta,
			Epoch:     b.epoch,
			StreamID:  req.StreamID,
			Seq:       seq,
			Type:      evType,
			Delta:     req.Delta,
			Fin:       false,
			CreatedAt: now,
		})

		if needMetaSave {
			actor.saveMetaBestEffortLocked(ctx, b.store)
		}

		return model.PublishResult{
			Epoch:       b.epoch,
			InFlightSeq: seq,
		}, nil
	}

	// Fin == true: build compacted Record in local variables and persist BEFORE mutating actor state.
	mergedText := baseMerged + req.Delta
	recordType := baseType
	if recordType == "" {
		recordType = req.Type
	}

	var recordPayload json.RawMessage
	if hasPayload {
		recordPayload = cloneRawMessage(req.Payload)
	} else {
		recordPayload, err = marshalContentPayload(mergedText)
		if err != nil {
			return model.PublishResult{}, err
		}
	}

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
	nextState := model.StateIdle
	if req.Type == "run_finished" || recordType == "run_finished" {
		nextState = model.StateCompleted
	} else if req.Type == "run_failed" || recordType == "run_failed" {
		nextState = model.StateFailed
	}

	actor.lastCompactedStreamID = req.StreamID
	actor.lastCompactedSeq = seq
	actor.lastCompactedOffset = rec.Offset
	actor.inFlight = nil
	actor.lastOffset = nextOffset
	actor.meta.Epoch = b.epoch
	actor.meta.State = nextState
	actor.meta.LastOffset = nextOffset
	actor.meta.UpdatedAt = now

	actor.fanOutLocked(model.OutEvent{
		Kind:      model.EventDelta,
		Epoch:     b.epoch,
		StreamID:  req.StreamID,
		Seq:       seq,
		Type:      evType,
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
		InFlightSeq:     seq,
	}, nil
}

func (b *Broker) Subscribe(ctx context.Context, sessionID string, afterOffset uint64, afterEpoch string) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.isClosed() {
		return nil, ErrBrokerClosed
	}
	if sessionID == "" {
		return nil, ErrInvalidInput
	}

	if afterEpoch != "" && afterEpoch != b.epoch {
		afterOffset = 0
	}

	actor, _, err := b.acquireActor(ctx, sessionID, true, true)
	if err != nil {
		return nil, err
	}
	defer actor.unpin()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrBrokerClosed
	}
	b.subWg.Add(1)
	b.mu.Unlock()

	sub := &subState{
		liveCh:    make(chan model.OutEvent, b.cfg.SubscriberBufferSize),
		cancelCh:  make(chan struct{}),
		evictedCh: make(chan struct{}),
	}

	actor.mu.Lock()
	select {
	case <-b.doneCh:
		actor.mu.Unlock()
		b.subWg.Done()
		return nil, ErrBrokerClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		actor.mu.Unlock()
		b.subWg.Done()
		return nil, err
	}
	actor.subscribers[sub] = struct{}{}
	snapOffset := actor.lastOffset
	inFlightSnap := actor.inFlightViewLocked()
	actor.touch()
	actor.mu.Unlock()

	outCh := make(chan model.OutEvent)
	go b.runSubscriberPump(ctx, sessionID, actor, sub, outCh, afterOffset, snapOffset, inFlightSnap)

	return &Subscription{
		Epoch:  b.epoch,
		Events: outCh,
		Cancel: sub.cancel,
		Err:    sub.getErr,
	}, nil
}

func (b *Broker) runSubscriberPump(
	ctx context.Context,
	sessionID string,
	actor *sessionActor,
	sub *subState,
	outCh chan<- model.OutEvent,
	afterOffset uint64,
	snapOffset uint64,
	inFlightSnap *model.InFlightView,
) {
	defer b.subWg.Done()
	defer close(outCh)
	defer func() {
		actor.mu.Lock()
		if _, ok := actor.subscribers[sub]; ok {
			delete(actor.subscribers, sub)
			close(sub.liveCh)
		}
		actor.touch()
		actor.mu.Unlock()
	}()

	// Step 1: Catch up committed records up to snapOffset.
	if snapOffset > afterOffset {
		recs, err := b.store.ReadFrom(ctx, sessionID, afterOffset)
		if err != nil {
			sub.setErr(err)
			return
		}
		for _, rec := range recs {
			if rec.Offset > snapOffset {
				continue
			}
			ev := model.OutEvent{
				Kind:      model.EventCommitted,
				Offset:    rec.Offset,
				Epoch:     rec.Epoch,
				StreamID:  rec.StreamID,
				Type:      rec.Type,
				Payload:   rec.Payload,
				CreatedAt: rec.CreatedAt,
			}
			if !b.sendOutEvent(ctx, sub, outCh, ev) {
				return
			}
		}
	}

	// Step 2: Emit coalesced in-flight snapshot if an active stream was running at subscribe time.
	if inFlightSnap != nil && inFlightSnap.LastSeq > 0 {
		ev := model.OutEvent{
			Kind:      model.EventInFlightSnapshot,
			Epoch:     b.epoch,
			StreamID:  inFlightSnap.StreamID,
			Seq:       inFlightSnap.LastSeq,
			Type:      inFlightSnap.Type,
			Delta:     inFlightSnap.Merged,
			CreatedAt: inFlightSnap.StartedAt,
		}
		if !b.sendOutEvent(ctx, sub, outCh, ev) {
			return
		}
	}

	// Step 3: Drain liveCh, deduplicating anything already covered by snapOffset / inFlightSnap.
	for {
		select {
		case <-ctx.Done():
			sub.setErr(ctx.Err())
			return
		case <-sub.cancelCh:
			return
		case <-sub.evictedCh:
			return
		case <-b.doneCh:
			sub.setErr(ErrBrokerClosed)
			return
		default:
		}

		select {
		case <-ctx.Done():
			sub.setErr(ctx.Err())
			return
		case <-sub.cancelCh:
			return
		case <-sub.evictedCh:
			return
		case <-b.doneCh:
			sub.setErr(ErrBrokerClosed)
			return
		case ev, ok := <-sub.liveCh:
			if !ok {
				return
			}
			if ev.Kind == model.EventCommitted {
				if ev.Offset <= snapOffset {
					continue
				}
				if inFlightSnap != nil && ev.StreamID == inFlightSnap.StreamID {
					inFlightSnap = nil
				}
			}
			if ev.Kind == model.EventDelta && inFlightSnap != nil && ev.StreamID == inFlightSnap.StreamID && ev.Seq <= inFlightSnap.LastSeq {
				continue
			}
			if !b.sendOutEvent(ctx, sub, outCh, ev) {
				return
			}
		}
	}
}

func (b *Broker) sendOutEvent(ctx context.Context, sub *subState, outCh chan<- model.OutEvent, ev model.OutEvent) bool {
	if ev.Payload != nil {
		ev.Payload = cloneRawMessage(ev.Payload)
	}
	select {
	case <-ctx.Done():
		sub.setErr(ctx.Err())
		return false
	case <-sub.cancelCh:
		return false
	case <-sub.evictedCh:
		return false
	case <-b.doneCh:
		sub.setErr(ErrBrokerClosed)
		return false
	default:
	}

	select {
	case outCh <- ev:
		return true
	case <-ctx.Done():
		sub.setErr(ctx.Err())
		return false
	case <-sub.cancelCh:
		return false
	case <-sub.evictedCh:
		return false
	case <-b.doneCh:
		sub.setErr(ErrBrokerClosed)
		return false
	}
}

func (b *Broker) Inspect(ctx context.Context, sessionID string) (model.SessionSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return model.SessionSnapshot{}, false, err
	}
	if b.isClosed() {
		return model.SessionSnapshot{}, false, ErrBrokerClosed
	}
	if sessionID == "" {
		return model.SessionSnapshot{}, false, nil
	}

	actor, exists, err := b.acquireActor(ctx, sessionID, false, false)
	if err != nil {
		return model.SessionSnapshot{}, false, err
	}
	if !exists {
		return model.SessionSnapshot{}, false, nil
	}
	defer actor.unpin()

	actor.mu.Lock()
	if !actor.metaPersisted {
		if err := b.store.SaveMeta(context.WithoutCancel(ctx), actor.meta); err == nil {
			actor.metaPersisted = true
		}
	}
	snap := actor.snapshotLocked()
	actor.mu.Unlock()
	return snap, true, nil
}

type namedActor struct {
	id    string
	actor *sessionActor
}

func (b *Broker) ListSessions(ctx context.Context) ([]model.SessionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.isClosed() {
		return nil, ErrBrokerClosed
	}

	metas, err := b.store.ListSessions(ctx)
	if err != nil {
		return nil, err
	}

	byID := make(map[string]model.SessionSnapshot, len(metas))
	for _, meta := range metas {
		if meta.Epoch != b.epoch && meta.State == model.StateRunning {
			meta.State = model.StateIdle
		}
		meta.Epoch = b.epoch
		byID[meta.SessionID] = model.SessionSnapshot{
			Meta:            meta,
			InFlight:        nil,
			SubscriberCount: 0,
		}
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}
	active := make([]namedActor, 0, len(b.actors))
	for id, actor := range b.actors {
		active = append(active, namedActor{id: id, actor: actor})
	}
	b.mu.RUnlock()

	for _, item := range active {
		item.actor.mu.Lock()
		byID[item.id] = item.actor.snapshotLocked()
		item.actor.mu.Unlock()
	}

	out := make([]model.SessionSnapshot, 0, len(byID))
	for _, snap := range byID {
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Meta.CreatedAt.Equal(out[j].Meta.CreatedAt) {
			return out[i].Meta.CreatedAt.Before(out[j].Meta.CreatedAt)
		}
		return out[i].Meta.SessionID < out[j].Meta.SessionID
	})
	return out, nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	var oldActors map[string]*sessionActor
	if !b.closed {
		b.closed = true
		close(b.doneCh)
		oldActors = b.actors
		b.actors = make(map[string]*sessionActor)
	}
	b.mu.Unlock()

	for _, actor := range oldActors {
		actor.mu.Lock()
		for sub := range actor.subscribers {
			delete(actor.subscribers, sub)
			sub.setErr(ErrBrokerClosed)
			close(sub.liveCh)
		}
		actor.mu.Unlock()
	}

	b.sweepWg.Wait()
	b.subWg.Wait()
	return nil
}
