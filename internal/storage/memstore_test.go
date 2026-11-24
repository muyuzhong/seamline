package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

func fixedEpochs(epochs ...string) func() string {
	var mu sync.Mutex
	i := 0
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		e := epochs[min(i, len(epochs)-1)]
		i++
		return e
	}
}

func appendN(t *testing.T, s *MemoryStorage, id, epoch string, from, to uint64, payload string, at time.Time) {
	t.Helper()
	for off := from; off <= to; off++ {
		if err := s.Append(context.Background(), id, model.Record{
			Offset:    off,
			Epoch:     epoch,
			Type:      "msg",
			Payload:   json.RawMessage(payload),
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("Append(%s, %d): %v", id, off, err)
		}
	}
}

func offsetsOf(recs []model.Record) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Offset
	}
	return out
}

func TestMemoryStorageAppendContractAndReadFrom(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage(WithEpochSource(fixedEpochs("ep-1")))
	now := time.Now().UTC()

	if err := s.Append(ctx, "sess-1", model.Record{Offset: 1, Epoch: "ep-1"}); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("Append before EnsureStream: want ErrStreamNotFound, got %v", err)
	}
	info, err := s.EnsureStream(ctx, "sess-1")
	if err != nil || info.Epoch != "ep-1" || info.EarliestOffset != 1 || info.LatestOffset != 0 {
		t.Fatalf("EnsureStream on new stream: info=%+v err=%v", info, err)
	}

	bad := []struct {
		name string
		id   string
		rec  model.Record
		want error
	}{
		{"offset zero", "sess-1", model.Record{Offset: 0, Epoch: "ep-1"}, ErrNonMonotonicOffset},
		{"empty session", "", model.Record{Offset: 1, Epoch: "ep-1"}, ErrInvalidSessionID},
		{"foreign epoch", "sess-1", model.Record{Offset: 1, Epoch: "other"}, ErrEpochMismatch},
		{"gap", "sess-1", model.Record{Offset: 2, Epoch: "ep-1"}, ErrNonMonotonicOffset},
	}
	for _, tc := range bad {
		if err := s.Append(ctx, tc.id, tc.rec); !errors.Is(err, tc.want) {
			t.Fatalf("%s: want %v, got %v", tc.name, tc.want, err)
		}
	}

	appendN(t, s, "sess-1", "ep-1", 1, 5, `{"n":1}`, now)
	for _, off := range []uint64{5, 3, 7} {
		if err := s.Append(ctx, "sess-1", model.Record{Offset: off, Epoch: "ep-1"}); !errors.Is(err, ErrNonMonotonicOffset) {
			t.Fatalf("Append offset %d after 5: want ErrNonMonotonicOffset, got %v", off, err)
		}
	}

	all, info, err := s.ReadFrom(ctx, "sess-1", 0)
	if err != nil || fmt.Sprint(offsetsOf(all)) != "[1 2 3 4 5]" || info.EarliestOffset != 1 || info.LatestOffset != 5 {
		t.Fatalf("ReadFrom(0): offsets=%v info=%+v err=%v", offsetsOf(all), info, err)
	}
	after3, _, err := s.ReadFrom(ctx, "sess-1", 3)
	if err != nil || fmt.Sprint(offsetsOf(after3)) != "[4 5]" {
		t.Fatalf("ReadFrom(3): offsets=%v err=%v", offsetsOf(after3), err)
	}
	if after5, _, err := s.ReadFrom(ctx, "sess-1", 5); err != nil || len(after5) != 0 {
		t.Fatalf("ReadFrom(5): got %d records err=%v", len(after5), err)
	}
	if _, _, err := s.ReadFrom(ctx, "unknown", 0); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("ReadFrom unknown: want ErrStreamNotFound, got %v", err)
	}

	again, err := s.EnsureStream(ctx, "sess-1")
	if err != nil || again.Epoch != "ep-1" || again.LatestOffset != 5 {
		t.Fatalf("EnsureStream on existing stream must keep epoch and offsets: %+v err=%v", again, err)
	}
}

func TestMemoryStorageByteRetentionKeepsNewestAndOffsets(t *testing.T) {
	ctx := context.Background()
	payload := `"` + strings.Repeat("x", 98) + `"` // 100 bytes
	recSize := int64(100 + len("msg") + recordOverheadBytes)
	s := NewMemoryStorage(
		WithEpochSource(fixedEpochs("ep-b")),
		WithRetention(Retention{MaxHistoryBytes: 2*recSize + recSize/2}),
	)
	if _, err := s.EnsureStream(ctx, "b"); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	now := time.Now().UTC()
	appendN(t, s, "b", "ep-b", 1, 5, payload, now)

	recs, info, err := s.ReadFrom(ctx, "b", 0)
	if err != nil || fmt.Sprint(offsetsOf(recs)) != "[4 5]" {
		t.Fatalf("expected only the 2 newest records to fit, got %v err=%v", offsetsOf(recs), err)
	}
	if info.EarliestOffset != 4 || info.LatestOffset != 5 || info.RetainedBytes != 2*recSize {
		t.Fatalf("unexpected window after byte eviction: %+v", info)
	}

	// A single record larger than the whole budget is still kept as the newest record...
	huge := `"` + strings.Repeat("y", 1000) + `"`
	appendN(t, s, "b", "ep-b", 6, 6, huge, now)
	recs, info, _ = s.ReadFrom(ctx, "b", 0)
	if fmt.Sprint(offsetsOf(recs)) != "[6]" || info.EarliestOffset != 6 {
		t.Fatalf("oversized newest record must be retained alone, got %v (%+v)", offsetsOf(recs), info)
	}
	// ...and is evicted as soon as a newer record arrives. Offsets never restart.
	appendN(t, s, "b", "ep-b", 7, 7, payload, now)
	recs, info, _ = s.ReadFrom(ctx, "b", 0)
	if fmt.Sprint(offsetsOf(recs)) != "[7]" || info.LatestOffset != 7 || info.Epoch != "ep-b" {
		t.Fatalf("expected [7] after the oversized record, got %v (%+v)", offsetsOf(recs), info)
	}
}

func TestMemoryStorageHistoryTTLKeepsEpochAndOffsets(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	s := NewMemoryStorage(
		WithEpochSource(fixedEpochs("ep-ttl", "ep-other")),
		WithClock(func() time.Time { return t0 }),
		WithRetention(Retention{HistoryTTL: 5 * time.Minute, MetaTTL: time.Hour}),
	)
	if _, err := s.EnsureStream(ctx, "t"); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	appendN(t, s, "t", "ep-ttl", 1, 1, `{}`, t0)
	appendN(t, s, "t", "ep-ttl", 2, 2, `{}`, t0.Add(time.Minute))
	appendN(t, s, "t", "ep-ttl", 3, 3, `{}`, t0.Add(4*time.Minute))

	if err := s.Sweep(ctx, t0.Add(6*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	recs, info, _ := s.ReadFrom(ctx, "t", 0)
	if fmt.Sprint(offsetsOf(recs)) != "[3]" || info.EarliestOffset != 3 {
		t.Fatalf("expected records older than HistoryTTL to be evicted, got %v (%+v)", offsetsOf(recs), info)
	}

	// Every record expires, but the stream identity and offset counter survive (MetaTTL not reached).
	if err := s.Sweep(ctx, t0.Add(30*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	info, ok, err := s.Stream(ctx, "t")
	if err != nil || !ok || info.Epoch != "ep-ttl" || info.LatestOffset != 3 || info.EarliestOffset != 4 {
		t.Fatalf("expected empty window that keeps epoch and offsets, got ok=%v info=%+v err=%v", ok, info, err)
	}
	if err := s.Append(ctx, "t", model.Record{Offset: 1, Epoch: "ep-ttl"}); !errors.Is(err, ErrNonMonotonicOffset) {
		t.Fatalf("offsets must not restart after TTL eviction, got %v", err)
	}
	appendN(t, s, "t", "ep-ttl", 4, 4, `{}`, t0.Add(31*time.Minute))

	// Append also applies the TTL relative to the new record's timestamp.
	appendN(t, s, "t", "ep-ttl", 5, 5, `{}`, t0.Add(40*time.Minute))
	recs, _, _ = s.ReadFrom(ctx, "t", 0)
	if fmt.Sprint(offsetsOf(recs)) != "[5]" {
		t.Fatalf("expected Append to evict records older than HistoryTTL, got %v", offsetsOf(recs))
	}
}

func TestMemoryStorageMetaTTLDropsIdleStreamAndRotatesEpoch(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	clock := t0
	var clockMu sync.Mutex
	s := NewMemoryStorage(
		WithEpochSource(fixedEpochs("ep-1", "ep-2", "ep-3")),
		WithClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }),
		WithRetention(Retention{MetaTTL: time.Hour}),
	)
	setClock := func(c time.Time) { clockMu.Lock(); clock = c; clockMu.Unlock() }

	if _, err := s.EnsureStream(ctx, "idle"); err != nil {
		t.Fatalf("EnsureStream idle: %v", err)
	}
	if _, err := s.EnsureStream(ctx, "kept"); err != nil {
		t.Fatalf("EnsureStream kept: %v", err)
	}
	appendN(t, s, "idle", "ep-1", 1, 2, `{}`, t0)
	if err := s.SaveMeta(ctx, model.SessionMeta{SessionID: "idle"}); err != nil {
		t.Fatalf("SaveMeta: %v", err)
	}

	setClock(t0.Add(50 * time.Minute))
	if err := s.Touch(ctx, "kept"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if err := s.Sweep(ctx, t0.Add(61*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if _, ok, _ := s.Stream(ctx, "idle"); ok {
		t.Fatalf("stream idle for longer than MetaTTL must be dropped")
	}
	if _, _, err := s.LoadMeta(ctx, "idle"); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("meta of a dropped stream must be gone, got %v", err)
	}
	if kept, ok, _ := s.Stream(ctx, "kept"); !ok || kept.Epoch != "ep-2" {
		t.Fatalf("touched stream must survive the sweep, got ok=%v %+v", ok, kept)
	}
	list, _ := s.ListSessions(ctx)
	if len(list) != 1 || list[0].SessionID != "kept" {
		t.Fatalf("ListSessions after sweep: %+v", list)
	}

	// Recreating the dropped session starts a new incarnation: new epoch, offsets from 1.
	info, err := s.EnsureStream(ctx, "idle")
	if err != nil || info.Epoch != "ep-3" || info.LatestOffset != 0 {
		t.Fatalf("recreated stream must get a fresh epoch, got %+v err=%v", info, err)
	}
}

func TestMemoryStorageDeepCopyIsolation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage(WithEpochSource(fixedEpochs("ep1")))
	if _, err := s.EnsureStream(ctx, "sess-copy"); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	payload := json.RawMessage(`{"content":"original"}`)
	meta := json.RawMessage(`{"tool":"a"}`)
	if err := s.Append(ctx, "sess-copy", model.Record{Offset: 1, Epoch: "ep1", Type: "text", Payload: payload, Meta: meta}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	payload[0], meta[0] = 'X', 'X'

	recs1, _, _ := s.ReadFrom(ctx, "sess-copy", 0)
	if string(recs1[0].Payload) != `{"content":"original"}` || string(recs1[0].Meta) != `{"tool":"a"}` {
		t.Fatalf("stored record was mutated via input buffers: %s %s", recs1[0].Payload, recs1[0].Meta)
	}
	recs1[0].Payload[0], recs1[0].Meta[0] = 'Y', 'Y'

	recs2, _, _ := s.ReadFrom(ctx, "sess-copy", 0)
	if string(recs2[0].Payload) != `{"content":"original"}` || string(recs2[0].Meta) != `{"tool":"a"}` {
		t.Fatalf("stored record was mutated via output buffers: %s %s", recs2[0].Payload, recs2[0].Meta)
	}
}

func TestMemoryStorageMetaReconciliation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage(WithEpochSource(fixedEpochs("ep-a", "ep-b")))
	t1 := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	if err := s.SaveMeta(ctx, model.SessionMeta{SessionID: "a"}); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("SaveMeta without stream: want ErrStreamNotFound, got %v", err)
	}
	if _, err := s.EnsureStream(ctx, "a"); err != nil {
		t.Fatalf("EnsureStream a: %v", err)
	}
	if _, err := s.EnsureStream(ctx, "b"); err != nil {
		t.Fatalf("EnsureStream b: %v", err)
	}

	// No saved meta yet: synthesized from the stream.
	meta, saved, err := s.LoadMeta(ctx, "b")
	if err != nil || saved || meta.Epoch != "ep-b" || meta.State != model.StateIdle || meta.EarliestOffset != 1 {
		t.Fatalf("synthesized meta: %+v saved=%v err=%v", meta, saved, err)
	}

	// A stale saved copy (best-effort SaveMeta lagging behind) is reconciled from the stream.
	appendN(t, s, "a", "ep-a", 1, 1, `{}`, t1)
	if err := s.SaveMeta(ctx, model.SessionMeta{SessionID: "a", Epoch: "stale", State: model.StateRunning, LastOffset: 1, CreatedAt: t1}); err != nil {
		t.Fatalf("SaveMeta: %v", err)
	}
	if err := s.Append(ctx, "a", model.Record{Offset: 2, Epoch: "ep-a", Type: "run_finished", Payload: json.RawMessage(`{}`), CreatedAt: t1.Add(time.Minute)}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	meta, saved, err = s.LoadMeta(ctx, "a")
	if err != nil || !saved {
		t.Fatalf("LoadMeta a: saved=%v err=%v", saved, err)
	}
	if meta.Epoch != "ep-a" || meta.LastOffset != 2 || meta.State != model.StateCompleted || !meta.UpdatedAt.Equal(t1.Add(time.Minute)) {
		t.Fatalf("stale meta not reconciled: %+v", meta)
	}

	list, err := s.ListSessions(ctx)
	if err != nil || len(list) != 2 || list[0].SessionID != "a" || list[0].State != model.StateCompleted {
		t.Fatalf("ListSessions must be sorted by CreatedAt and reconciled, got %+v err=%v", list, err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	calls := map[string]error{
		"EnsureStream": func() error { _, err := s.EnsureStream(cctx, "a"); return err }(),
		"Stream":       func() error { _, _, err := s.Stream(cctx, "a"); return err }(),
		"Append":       s.Append(cctx, "a", model.Record{Offset: 3, Epoch: "ep-a"}),
		"ReadFrom":     func() error { _, _, err := s.ReadFrom(cctx, "a", 0); return err }(),
		"Touch":        s.Touch(cctx, "a"),
		"Sweep":        s.Sweep(cctx, time.Now()),
		"SaveMeta":     s.SaveMeta(cctx, model.SessionMeta{SessionID: "a"}),
		"LoadMeta":     func() error { _, _, err := s.LoadMeta(cctx, "a"); return err }(),
		"ListSessions": func() error { _, err := s.ListSessions(cctx); return err }(),
	}
	for name, err := range calls {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s with canceled context: want context.Canceled, got %v", name, err)
		}
	}
}

func TestMemoryStorageConcurrentRace(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage(WithRetention(Retention{MaxHistoryBytes: 4096}))

	const sessions = 10
	const recordsPerSession = 100

	var wg sync.WaitGroup
	for sid := 0; sid < sessions; sid++ {
		sessionID := fmt.Sprintf("sess-%d", sid)
		info, err := s.EnsureStream(ctx, sessionID)
		if err != nil {
			t.Fatalf("EnsureStream: %v", err)
		}

		wg.Add(1)
		go func(id, epoch string) {
			defer wg.Done()
			for off := uint64(1); off <= recordsPerSession; off++ {
				if err := s.Append(ctx, id, model.Record{
					Offset:    off,
					Epoch:     epoch,
					Type:      "msg",
					Payload:   json.RawMessage(fmt.Sprintf(`{"off":%d}`, off)),
					CreatedAt: time.Now().UTC(),
				}); err != nil {
					t.Errorf("Append(%s, %d): %v", id, off, err)
					return
				}
				if err := s.SaveMeta(ctx, model.SessionMeta{SessionID: id, State: model.StateRunning, LastOffset: off}); err != nil {
					t.Errorf("SaveMeta(%s): %v", id, err)
					return
				}
			}
		}(sessionID, info.Epoch)

		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				recs, info, err := s.ReadFrom(ctx, id, uint64(i))
				if err != nil {
					t.Errorf("ReadFrom(%s): %v", id, err)
					return
				}
				for j := 1; j < len(recs); j++ {
					if recs[j].Offset != recs[j-1].Offset+1 {
						t.Errorf("non-contiguous ReadFrom: %d then %d", recs[j-1].Offset, recs[j].Offset)
						return
					}
				}
				if len(recs) > 0 && recs[len(recs)-1].Offset != info.LatestOffset {
					t.Errorf("ReadFrom window mismatch: last=%d latest=%d", recs[len(recs)-1].Offset, info.LatestOffset)
					return
				}
				_, _, _ = s.LoadMeta(ctx, id)
				_, _ = s.ListSessions(ctx)
				_ = s.Sweep(ctx, time.Now())
			}
		}(sessionID)
	}
	wg.Wait()
}
