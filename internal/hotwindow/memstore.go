package hotwindow

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

// Option configures a MemoryStorage.
type Option func(*MemoryStorage)

// WithRetention overrides the default retention limits.
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

// MemoryStorage is the default zero-dependency Storage.
type MemoryStorage struct {
	mu        sync.RWMutex
	streams   map[string]*stream
	retention Retention
	clock     func() time.Time
	newEpoch  func() string
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage(opts ...Option) *MemoryStorage {
	m := &MemoryStorage{
		streams:   make(map[string]*stream),
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

	s, ok := m.streams[sessionID]
	if !ok {
		created := now.UTC()
		s = &stream{epoch: m.newEpoch(), state: model.StateIdle, createdAt: created, updatedAt: created}
		m.streams[sessionID] = s
	}
	s.touchedAt = now
	return s.info(sessionID), nil
}

func (m *MemoryStorage) Stream(ctx context.Context, sessionID string) (StreamInfo, bool, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return StreamInfo{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.streams[sessionID]
	if !ok {
		return StreamInfo{}, false, nil
	}
	return s.info(sessionID), true, nil
}

func (m *MemoryStorage) Append(ctx context.Context, sessionID, eventType string, payload json.RawMessage, now time.Time) (model.Event, StreamInfo, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return model.Event{}, StreamInfo{}, err
	}
	touched := m.clock()

	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.streams[sessionID]
	if !ok {
		return model.Event{}, StreamInfo{}, ErrStreamNotFound
	}
	ev, err := s.appendEvent(eventType, payload, now, m.retention)
	if err != nil {
		return model.Event{}, StreamInfo{}, err
	}
	s.touchedAt = touched
	s.evict(now, m.retention)
	return ev, s.info(sessionID), nil
}

func (m *MemoryStorage) ReadAfter(ctx context.Context, sessionID string, afterSeq uint64) ([]model.Event, StreamInfo, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return nil, StreamInfo{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.streams[sessionID]
	if !ok {
		return nil, StreamInfo{}, ErrStreamNotFound
	}
	idx := sort.Search(len(s.events), func(i int) bool {
		return s.events[i].Seq > afterSeq
	})
	// Stored events are immutable (Append keeps a private payload copy), so readers share them.
	out := append([]model.Event(nil), s.events[idx:]...)
	return out, s.info(sessionID), nil
}

func (m *MemoryStorage) Touch(ctx context.Context, sessionID string) error {
	if err := checkArgs(ctx, sessionID); err != nil {
		return err
	}
	now := m.clock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.streams[sessionID]; ok {
		s.touchedAt = now
	}
	return nil
}

func (m *MemoryStorage) Sweep(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, s := range m.streams {
		if now.Sub(s.touchedAt) > m.retention.MetaTTL {
			delete(m.streams, id)
			continue
		}
		s.evict(now, m.retention)
	}
	return nil
}

func (m *MemoryStorage) List(ctx context.Context) ([]StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	out := make([]StreamInfo, 0, len(m.streams))
	for id, s := range m.streams {
		out = append(out, s.info(id))
	}
	m.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].SessionID < out[j].SessionID
	})
	return out, nil
}
