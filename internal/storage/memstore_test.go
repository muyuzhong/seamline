package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

func TestMemoryStorageMonotonicOffsetsAndReadFrom(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage()

	// Offset 0 must be rejected.
	err := s.Append(ctx, "sess-1", model.Record{Offset: 0, Payload: json.RawMessage(`{}`)})
	if !errors.Is(err, ErrNonMonotonicOffset) {
		t.Fatalf("expected ErrNonMonotonicOffset for offset 0, got %v", err)
	}

	// Empty sessionID must be rejected.
	if err := s.Append(ctx, "", model.Record{Offset: 1}); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("expected ErrInvalidSessionID, got %v", err)
	}

	for i := uint64(1); i <= 5; i++ {
		rec := model.Record{
			Offset:    i,
			Epoch:     "ep1",
			Type:      "msg",
			Payload:   json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
			CreatedAt: time.Now().UTC(),
		}
		if err := s.Append(ctx, "sess-1", rec); err != nil {
			t.Fatalf("append offset %d failed: %v", i, err)
		}
	}

	// Duplicate offset rejected.
	if err := s.Append(ctx, "sess-1", model.Record{Offset: 5}); !errors.Is(err, ErrNonMonotonicOffset) {
		t.Fatalf("expected ErrNonMonotonicOffset on duplicate offset 5, got %v", err)
	}
	// Decreasing offset rejected.
	if err := s.Append(ctx, "sess-1", model.Record{Offset: 3}); !errors.Is(err, ErrNonMonotonicOffset) {
		t.Fatalf("expected ErrNonMonotonicOffset on decreasing offset 3, got %v", err)
	}

	all, err := s.ReadFrom(ctx, "sess-1", 0)
	if err != nil {
		t.Fatalf("ReadFrom(0): %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("expected 5 records, got %d", len(all))
	}

	after3, err := s.ReadFrom(ctx, "sess-1", 3)
	if err != nil {
		t.Fatalf("ReadFrom(3): %v", err)
	}
	if len(after3) != 2 || after3[0].Offset != 4 || after3[1].Offset != 5 {
		t.Fatalf("expected offsets [4, 5], got %+v", after3)
	}

	after5, err := s.ReadFrom(ctx, "sess-1", 5)
	if err != nil {
		t.Fatalf("ReadFrom(5): %v", err)
	}
	if len(after5) != 0 {
		t.Fatalf("expected 0 records after offset 5, got %d", len(after5))
	}

	unknown, err := s.ReadFrom(ctx, "unknown-session", 0)
	if err != nil {
		t.Fatalf("ReadFrom unknown: %v", err)
	}
	if len(unknown) != 0 {
		t.Fatalf("expected 0 records for unknown session, got %d", len(unknown))
	}
}

func TestMemoryStorageDeepCopyPayloadIsolation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage()

	callerBuf := json.RawMessage(`{"content":"original"}`)
	if err := s.Append(ctx, "sess-copy", model.Record{
		Offset:  1,
		Epoch:   "ep1",
		Type:    "text",
		Payload: callerBuf,
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Mutate caller's slice in-place after Append.
	callerBuf[0] = 'X'

	recs1, err := s.ReadFrom(ctx, "sess-copy", 0)
	if err != nil {
		t.Fatalf("ReadFrom 1: %v", err)
	}
	if string(recs1[0].Payload) != `{"content":"original"}` {
		t.Fatalf("stored payload was mutated via input buffer: %s", string(recs1[0].Payload))
	}

	// Mutate returned slice in-place after ReadFrom.
	recs1[0].Payload[0] = 'Y'

	recs2, err := s.ReadFrom(ctx, "sess-copy", 0)
	if err != nil {
		t.Fatalf("ReadFrom 2: %v", err)
	}
	if string(recs2[0].Payload) != `{"content":"original"}` {
		t.Fatalf("stored payload was mutated via output buffer: %s", string(recs2[0].Payload))
	}
}

func TestMemoryStorageMetaAndContextCancellation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage()

	if _, ok, err := s.LoadMeta(ctx, "missing"); err != nil || ok {
		t.Fatalf("expected missing=false nil err, got ok=%v err=%v", ok, err)
	}

	t1 := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)

	if err := s.SaveMeta(ctx, model.SessionMeta{SessionID: "b", Epoch: "ep", State: model.StateIdle, CreatedAt: t2}); err != nil {
		t.Fatalf("SaveMeta b: %v", err)
	}
	if err := s.SaveMeta(ctx, model.SessionMeta{SessionID: "a", Epoch: "ep", State: model.StateRunning, LastOffset: 2, CreatedAt: t1}); err != nil {
		t.Fatalf("SaveMeta a: %v", err)
	}

	metaA, ok, err := s.LoadMeta(ctx, "a")
	if err != nil || !ok || metaA.State != model.StateRunning || metaA.LastOffset != 2 {
		t.Fatalf("unexpected LoadMeta a: meta=%+v ok=%v err=%v", metaA, ok, err)
	}

	list, err := s.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(list) != 2 || list[0].SessionID != "a" || list[1].SessionID != "b" {
		t.Fatalf("expected sorted sessions [a, b], got %+v", list)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Append(cctx, "a", model.Record{Offset: 3}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on Append, got %v", err)
	}
	if _, err := s.ReadFrom(cctx, "a", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on ReadFrom, got %v", err)
	}
	if err := s.SaveMeta(cctx, model.SessionMeta{SessionID: "a"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on SaveMeta, got %v", err)
	}
	if _, _, err := s.LoadMeta(cctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on LoadMeta, got %v", err)
	}
	if _, err := s.ListSessions(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on ListSessions, got %v", err)
	}
}

func TestMemoryStorageConcurrentRace(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage()

	const sessions = 10
	const recordsPerSession = 100

	var wg sync.WaitGroup
	for sid := 0; sid < sessions; sid++ {
		sessionID := fmt.Sprintf("sess-%d", sid)
		// Writer per session
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for off := uint64(1); off <= recordsPerSession; off++ {
				err := s.Append(ctx, id, model.Record{
					Offset:    off,
					Epoch:     "ep",
					Type:      "msg",
					Payload:   json.RawMessage(fmt.Sprintf(`{"off":%d}`, off)),
					CreatedAt: time.Now().UTC(),
				})
				if err != nil {
					t.Errorf("Append(%s, %d): %v", id, off, err)
					return
				}
				if err := s.SaveMeta(ctx, model.SessionMeta{
					SessionID:  id,
					Epoch:      "ep",
					State:      model.StateRunning,
					LastOffset: off,
					CreatedAt:  time.Now().UTC(),
					UpdatedAt:  time.Now().UTC(),
				}); err != nil {
					t.Errorf("SaveMeta(%s): %v", id, err)
					return
				}
			}
		}(sessionID)

		// Concurrent reader per session
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				recs, err := s.ReadFrom(ctx, id, uint64(i))
				if err != nil {
					t.Errorf("ReadFrom(%s): %v", id, err)
					return
				}
				for j := 1; j < len(recs); j++ {
					if recs[j].Offset <= recs[j-1].Offset {
						t.Errorf("non-monotonic ReadFrom: %d then %d", recs[j-1].Offset, recs[j].Offset)
						return
					}
				}
				_, _, _ = s.LoadMeta(ctx, id)
				_, _ = s.ListSessions(ctx)
			}
		}(sessionID)
	}
	wg.Wait()
}
