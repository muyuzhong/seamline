package storage

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/muyuzhong/seamline/internal/model"
)

type MemoryStorage struct {
	mu      sync.RWMutex
	records map[string][]model.Record
	metas   map[string]model.SessionMeta
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		records: make(map[string][]model.Record),
		metas:   make(map[string]model.SessionMeta),
	}
}

func cloneRecord(r model.Record) model.Record {
	if r.Payload != nil {
		cp := make(json.RawMessage, len(r.Payload))
		copy(cp, r.Payload)
		r.Payload = cp
	}
	return r
}

func (m *MemoryStorage) Append(ctx context.Context, sessionID string, rec model.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessionID == "" {
		return ErrInvalidSessionID
	}
	if rec.Offset == 0 {
		return ErrNonMonotonicOffset
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.records[sessionID]
	if len(existing) > 0 && rec.Offset <= existing[len(existing)-1].Offset {
		return ErrNonMonotonicOffset
	}

	m.records[sessionID] = append(existing, cloneRecord(rec))
	return nil
}

func (m *MemoryStorage) ReadFrom(ctx context.Context, sessionID string, afterOffset uint64) ([]model.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sessionID == "" {
		return nil, ErrInvalidSessionID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	recs := m.records[sessionID]
	if len(recs) == 0 {
		return []model.Record{}, nil
	}

	idx := sort.Search(len(recs), func(i int) bool {
		return recs[i].Offset > afterOffset
	})
	if idx >= len(recs) {
		return []model.Record{}, nil
	}

	out := make([]model.Record, len(recs)-idx)
	for i, r := range recs[idx:] {
		out[i] = cloneRecord(r)
	}
	return out, nil
}

func (m *MemoryStorage) SaveMeta(ctx context.Context, meta model.SessionMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if meta.SessionID == "" {
		return ErrInvalidSessionID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.metas[meta.SessionID] = meta
	return nil
}

func (m *MemoryStorage) LoadMeta(ctx context.Context, sessionID string) (model.SessionMeta, bool, error) {
	if err := ctx.Err(); err != nil {
		return model.SessionMeta{}, false, err
	}
	if sessionID == "" {
		return model.SessionMeta{}, false, ErrInvalidSessionID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	meta, ok := m.metas[sessionID]
	return meta, ok, nil
}

func (m *MemoryStorage) ListSessions(ctx context.Context) ([]model.SessionMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]model.SessionMeta, 0, len(m.metas))
	for _, meta := range m.metas {
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].SessionID < out[j].SessionID
	})
	return out, nil
}
