package storage

import (
	"context"
	"sort"

	"github.com/muyuzhong/seamline/internal/model"
)

// reconciledMetaLocked merges the best-effort saved metadata with the authoritative stream
// state: Epoch, LastOffset and EarliestOffset always come from the stream, and a retained
// record newer than the saved LastOffset refreshes State and UpdatedAt.
func (st *streamState) reconciledMetaLocked(sessionID string) model.SessionMeta {
	meta := model.SessionMeta{
		State:     model.StateIdle,
		CreatedAt: st.createdAt,
		UpdatedAt: st.createdAt,
	}
	if st.meta != nil {
		meta = *st.meta
	}
	if n := len(st.records); n > 0 {
		newest := st.records[n-1]
		if newest.Offset > meta.LastOffset {
			meta.State = model.StateFromRecordType(newest.Type)
			meta.UpdatedAt = newest.CreatedAt
		}
	}
	if meta.State == "" {
		meta.State = model.StateIdle
	}
	meta.SessionID = sessionID
	meta.Epoch = st.epoch
	meta.LastOffset = st.top
	meta.EarliestOffset = st.info().EarliestOffset
	return meta
}

func (m *MemoryStorage) SaveMeta(ctx context.Context, meta model.SessionMeta) error {
	if err := checkArgs(ctx, meta.SessionID); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.streams[meta.SessionID]
	if !ok {
		return ErrStreamNotFound
	}
	cp := meta
	st.meta = &cp
	return nil
}

func (m *MemoryStorage) LoadMeta(ctx context.Context, sessionID string) (model.SessionMeta, bool, error) {
	if err := checkArgs(ctx, sessionID); err != nil {
		return model.SessionMeta{}, false, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	st, ok := m.streams[sessionID]
	if !ok {
		return model.SessionMeta{}, false, ErrStreamNotFound
	}
	return st.reconciledMetaLocked(sessionID), st.meta != nil, nil
}

func (m *MemoryStorage) ListSessions(ctx context.Context) ([]model.SessionMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	out := make([]model.SessionMeta, 0, len(m.streams))
	for id, st := range m.streams {
		out = append(out, st.reconciledMetaLocked(id))
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
