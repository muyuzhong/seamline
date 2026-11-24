package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

// recordOverheadBytes approximates the fixed per-record bookkeeping cost so that floods of
// tiny records are still bounded by MaxHistoryBytes.
const recordOverheadBytes = 64

type streamState struct {
	epoch     string
	top       uint64
	records   []model.Record
	bytes     int64
	createdAt time.Time
	touchedAt time.Time
	meta      *model.SessionMeta
}

func (st *streamState) info() StreamInfo {
	earliest := st.top + 1
	if len(st.records) > 0 {
		earliest = st.records[0].Offset
	}
	return StreamInfo{
		Epoch:          st.epoch,
		EarliestOffset: earliest,
		LatestOffset:   st.top,
		RetainedBytes:  st.bytes,
		CreatedAt:      st.createdAt,
	}
}

// evictLocked drops the oldest records that are older than HistoryTTL (relative to now) or
// that push the stream over MaxHistoryBytes. The byte rule never evicts the newest record.
func (st *streamState) evictLocked(now time.Time, r Retention) {
	cutoff := now.Add(-r.HistoryTTL)
	n := 0
	for n < len(st.records) {
		head := st.records[n]
		expired := !head.CreatedAt.After(cutoff)
		overBytes := st.bytes > r.MaxHistoryBytes && len(st.records)-n > 1
		if !expired && !overBytes {
			break
		}
		st.bytes -= recordSize(head)
		n++
	}
	if n == 0 {
		return
	}
	// Zero evicted slots so their payloads become garbage even if no further append reallocates.
	clear(st.records[:n])
	st.records = st.records[n:]
	if len(st.records) == 0 {
		st.records = nil
	}
}

// Option configures a MemoryStorage.
type Option func(*MemoryStorage)

// WithRetention overrides the default hot-window retention limits.
func WithRetention(r Retention) Option {
	return func(m *MemoryStorage) { m.retention = r.normalized() }
}

// WithClock overrides the wall clock used for stream idle tracking (tests).
func WithClock(clock func() time.Time) Option {
	return func(m *MemoryStorage) { m.clock = clock }
}

// WithEpochSource overrides the generator of new stream epochs (tests).
func WithEpochSource(next func() string) Option {
	return func(m *MemoryStorage) { m.newEpoch = next }
}

// MemoryStorage is the default zero-dependency hot-window engine.
type MemoryStorage struct {
	mu        sync.RWMutex
	streams   map[string]*streamState
	retention Retention
	clock     func() time.Time
	newEpoch  func() string
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage(opts ...Option) *MemoryStorage {
	m := &MemoryStorage{
		streams:   make(map[string]*streamState),
		retention: Retention{}.normalized(),
		clock:     time.Now,
		newEpoch:  randomEpoch,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func randomEpoch() string {
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%012x", time.Now().UnixNano()&0xffffffffffff)
	}
	return hex.EncodeToString(buf[:])
}

func recordSize(r model.Record) int64 {
	return int64(len(r.Payload)+len(r.Type)+len(r.StreamID)) + recordOverheadBytes
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	cp := make(json.RawMessage, len(raw))
	copy(cp, raw)
	return cp
}

func cloneRecord(r model.Record) model.Record {
	r.Payload = cloneRaw(r.Payload)
	return r
}

func checkArgs(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessionID == "" {
		return ErrInvalidSessionID
	}
	return nil
}

func (m *MemoryStorage) EnsureStream(ctx context.Context, sessionID string) (StreamInfo, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return StreamInfo{}, err
	}
	now := m.clock()

	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.streams[sessionID]
	if !ok {
		st = &streamState{epoch: m.newEpoch(), createdAt: now.UTC()}
		m.streams[sessionID] = st
	}
	st.touchedAt = now
	return st.info(), nil
}

func (m *MemoryStorage) Stream(ctx context.Context, sessionID string) (StreamInfo, bool, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return StreamInfo{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	st, ok := m.streams[sessionID]
	if !ok {
		return StreamInfo{}, false, nil
	}
	return st.info(), true, nil
}

func (m *MemoryStorage) Append(ctx context.Context, sessionID string, rec model.Record) error {
	if err := checkArgs(ctx, sessionID); err != nil {
		return err
	}
	if rec.Offset == 0 {
		return ErrNonMonotonicOffset
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.streams[sessionID]
	if !ok {
		return ErrStreamNotFound
	}
	if rec.Epoch != st.epoch {
		return ErrEpochMismatch
	}
	if rec.Offset != st.top+1 {
		return ErrNonMonotonicOffset
	}

	stored := cloneRecord(rec)
	st.records = append(st.records, stored)
	st.bytes += recordSize(stored)
	st.top = rec.Offset
	st.touchedAt = m.clock()
	st.evictLocked(rec.CreatedAt, m.retention)
	return nil
}

func (m *MemoryStorage) ReadFrom(ctx context.Context, sessionID string, afterOffset uint64) ([]model.Record, StreamInfo, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return nil, StreamInfo{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	st, ok := m.streams[sessionID]
	if !ok {
		return nil, StreamInfo{}, ErrStreamNotFound
	}

	idx := sort.Search(len(st.records), func(i int) bool {
		return st.records[i].Offset > afterOffset
	})
	out := make([]model.Record, len(st.records)-idx)
	for i, r := range st.records[idx:] {
		out[i] = cloneRecord(r)
	}
	return out, st.info(), nil
}

func (m *MemoryStorage) Touch(ctx context.Context, sessionID string) error {
	if err := checkArgs(ctx, sessionID); err != nil {
		return err
	}
	now := m.clock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.streams[sessionID]; ok {
		st.touchedAt = now
	}
	return nil
}

func (m *MemoryStorage) Sweep(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, st := range m.streams {
		if now.Sub(st.touchedAt) > m.retention.MetaTTL {
			delete(m.streams, id)
			continue
		}
		st.evictLocked(now, m.retention)
	}
	return nil
}
