package hotwindow

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

// writer appends events of a given payload size to one stream.
type writer struct {
	t  *testing.T
	s  *MemoryStorage
	id string
}

func newWriter(t *testing.T, s *MemoryStorage, id string) *writer {
	t.Helper()
	if _, err := s.EnsureStream(context.Background(), id); err != nil {
		t.Fatalf("EnsureStream(%s): %v", id, err)
	}
	return &writer{t: t, s: s, id: id}
}

func (w *writer) append(eventType string, payloadLen int, at time.Time) (StreamInfo, error) {
	payload := json.RawMessage(`"` + strings.Repeat("x", max(payloadLen-2, 0)) + `"`)
	_, info, err := w.s.Append(context.Background(), w.id, eventType, payload, at)
	return info, err
}

func (w *writer) must(eventType string, payloadLen int, at time.Time) StreamInfo {
	w.t.Helper()
	info, err := w.append(eventType, payloadLen, at)
	if err != nil {
		w.t.Fatalf("append %s: %v", eventType, err)
	}
	return info
}

func (w *writer) window() (string, StreamInfo) {
	w.t.Helper()
	events, info, err := w.s.ReadAfter(context.Background(), w.id, 0)
	if err != nil {
		w.t.Fatalf("ReadAfter: %v", err)
	}
	seqs := make([]string, len(events))
	for i, ev := range events {
		seqs[i] = fmt.Sprint(ev.Seq)
	}
	return strings.Join(seqs, ","), info
}

// size of an event appended by writer with the given payload length and type.
func size(eventType string, payloadLen int) int64 {
	return int64(payloadLen+len(eventType)) + eventOverheadBytes
}

func TestAppendContractAndReadAfter(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage(WithEpochSource(fixedEpochs("ep-1")))
	now := time.Now().UTC()

	if _, _, err := s.Append(ctx, "a", "msg", nil, now); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("append before EnsureStream: want ErrStreamNotFound, got %v", err)
	}
	if _, _, err := s.Append(ctx, "", "msg", nil, now); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("empty session: want ErrInvalidSessionID, got %v", err)
	}
	info, err := s.EnsureStream(ctx, "a")
	if err != nil || info.Epoch != "ep-1" || info.EarliestSeq != 1 || info.LatestSeq != 0 || info.State != model.StateIdle {
		t.Fatalf("new stream: %+v err=%v", info, err)
	}
	for want := uint64(1); want <= 5; want++ {
		ev, info, err := s.Append(ctx, "a", "msg", json.RawMessage(`{"n":1}`), now)
		if err != nil || ev.Seq != want || ev.Epoch != "ep-1" || ev.Type != "msg" || !ev.CreatedAt.Equal(now) || info.LatestSeq != want {
			t.Fatalf("append %d: event=%+v info=%+v err=%v", want, ev, info, err)
		}
	}
	w := &writer{t: t, s: s, id: "a"}
	if got, info := w.window(); got != "1,2,3,4,5" || info.EarliestSeq != 1 || info.LatestSeq != 5 {
		t.Fatalf("window = %s (%+v)", got, info)
	}
	after3, _, err := s.ReadAfter(ctx, "a", 3)
	if err != nil || len(after3) != 2 || after3[0].Seq != 4 {
		t.Fatalf("ReadAfter(3) = %+v err=%v", after3, err)
	}
	if _, _, err := s.ReadAfter(ctx, "missing", 0); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("ReadAfter missing: want ErrStreamNotFound, got %v", err)
	}
	if again, _ := s.EnsureStream(ctx, "a"); again.Epoch != "ep-1" || again.LatestSeq != 5 {
		t.Fatalf("EnsureStream on an existing stream must keep it: %+v", again)
	}
}

func TestRunStateMachine(t *testing.T) {
	now := time.Now().UTC()
	w := newWriter(t, NewMemoryStorage(), "runs")
	steps := []struct {
		eventType string
		pin       uint64
		state     model.SessionState
	}{
		{"msg", 0, model.StateIdle},                           // 1: events outside a run change nothing
		{model.TypeRunFinished, 0, model.StateIdle},           // 2: a terminal event outside a run too
		{model.TypeRunStarted, 3, model.StateRunning},         // 3: only run_started opens a run
		{model.TypeRunStarted, 3, model.StateRunning},         // 4: a second run_started keeps the start
		{"assistant_delta", 3, model.StateRunning},            // 5
		{model.TypeRunFailed, 0, model.StateFailed},           // 6: a terminal event ends the run
		{"msg", 0, model.StateFailed},                         // 7: the last run's state stays
		{model.TypeRunStarted, 8, model.StateRunning},         // 8
		{model.TypeRunInterrupted, 0, model.StateInterrupted}, // 9
	}
	for i, st := range steps {
		if info := w.must(st.eventType, 2, now); info.ActiveRunStartSeq != st.pin || info.State != st.state {
			t.Fatalf("step %d (%s): pin=%d state=%s, want pin=%d state=%s", i+1, st.eventType, info.ActiveRunStartSeq, info.State, st.pin, st.state)
		}
	}
}

func TestActiveRunIsNeverEvicted(t *testing.T) {
	now := time.Now().UTC()
	s := NewMemoryStorage(WithRetention(Retention{MaxHistoryBytes: 3 * size("msg", 100), HistoryTTL: time.Minute}))
	w := newWriter(t, s, "pin")

	w.must("msg", 100, now)                // 1: history
	w.must(model.TypeRunStarted, 100, now) // 2: run starts
	for i := 0; i < 10; i++ {
		w.must("assistant_delta", 100, now) // 3..12: far beyond MaxHistoryBytes
	}
	if got, info := w.window(); !strings.HasPrefix(got, "1,2,") || info.ActiveRunStartSeq != 2 || info.State != model.StateRunning {
		t.Fatalf("active run must not push anything out: window=%s info=%+v", got, info)
	}

	// Even after HistoryTTL the active run stays, while expired history before it goes.
	if err := s.Sweep(context.Background(), now.Add(time.Hour)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	got, info := w.window()
	if !strings.HasPrefix(got, "2,3,") || info.EarliestSeq != 2 {
		t.Fatalf("expired history must go but the active run must stay: window=%s info=%+v", got, info)
	}
}

func TestLatestFinishedRunIsProtected(t *testing.T) {
	t0 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	ttl := 15 * time.Minute
	s := NewMemoryStorage(WithRetention(Retention{MaxHistoryBytes: 2 * size("d", 100), HistoryTTL: ttl}))
	w := newWriter(t, s, "last")

	// Run A spans 20 minutes and is larger than MaxHistoryBytes.
	w.must(model.TypeRunStarted, 10, t0)
	for i := 0; i < 5; i++ {
		w.must("d", 100, t0.Add(time.Duration(i)*5*time.Minute))
	}
	endA := t0.Add(20 * time.Minute)
	w.must(model.TypeRunFinished, 10, endA) // seq 7
	if got, info := w.window(); got != "1,2,3,4,5,6,7" || info.ActiveRunStartSeq != 0 || info.State != model.StateCompleted {
		t.Fatalf("finished run must survive both the byte limit and per-event TTL at its end: window=%s info=%+v", got, info)
	}

	// Its TTL counts from the run end, not from each event's creation.
	if err := s.Sweep(context.Background(), endA.Add(ttl-time.Second)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, _ := w.window(); got != "1,2,3,4,5,6,7" {
		t.Fatalf("latest run evicted before HistoryTTL after its end: window=%s", got)
	}

	// Once run B finishes, run A is ordinary history and the byte limit applies to it.
	endB := endA.Add(time.Minute)
	w.must(model.TypeRunStarted, 10, endB)
	w.must("d", 100, endB)
	w.must(model.TypeRunFinished, 10, endB) // seqs 8..10
	if got, _ := w.window(); got != "8,9,10" {
		t.Fatalf("previous run must become evictable once a newer run finished: window=%s", got)
	}
	if err := s.Sweep(context.Background(), endB.Add(ttl)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, info := w.window(); got != "" || info.EarliestSeq != 11 || info.LatestSeq != 10 {
		t.Fatalf("latest run must expire HistoryTTL after its end: window=%q info=%+v", got, info)
	}
}

func TestActiveRunQuota(t *testing.T) {
	now := time.Now().UTC()
	s := NewMemoryStorage(WithRetention(Retention{MaxActiveRunBytes: size(model.TypeRunStarted, 10) + 2*size("d", 100)}))
	w := newWriter(t, s, "quota")

	w.must(model.TypeRunStarted, 10, now)
	w.must("d", 100, now)
	w.must("d", 100, now)
	if _, err := w.append("d", 100, now); !errors.Is(err, ErrActiveRunQuotaExceeded) {
		t.Fatalf("want ErrActiveRunQuotaExceeded, got %v", err)
	}
	if _, info := w.window(); info.LatestSeq != 3 || info.ActiveRunStartSeq != 1 {
		t.Fatalf("rejected append must not change the stream: %+v", info)
	}
	// A terminal event is exempt, so the run can always end.
	if info := w.must(model.TypeRunFailed, 1000, now); info.ActiveRunStartSeq != 0 || info.State != model.StateFailed {
		t.Fatalf("terminal event must end the run: %+v", info)
	}
	// A new run starts with a fresh budget.
	w.must(model.TypeRunStarted, 10, now)
	w.must("d", 100, now)
}

func TestHistoryTTLKeepsEpochAndSeq(t *testing.T) {
	t0 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	s := NewMemoryStorage(
		WithEpochSource(fixedEpochs("ep-ttl")),
		WithClock(func() time.Time { return t0 }),
		WithRetention(Retention{HistoryTTL: 5 * time.Minute, MetaTTL: time.Hour}),
	)
	w := newWriter(t, s, "ttl")
	w.must("msg", 1, t0)
	w.must("msg", 1, t0.Add(4*time.Minute))

	if err := s.Sweep(context.Background(), t0.Add(6*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, info := w.window(); got != "2" || info.EarliestSeq != 2 {
		t.Fatalf("expected only seq 2 after TTL, got %s (%+v)", got, info)
	}
	if err := s.Sweep(context.Background(), t0.Add(30*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	got, info := w.window()
	if got != "" || info.Epoch != "ep-ttl" || info.LatestSeq != 2 || info.EarliestSeq != 3 {
		t.Fatalf("empty window must keep epoch and seq counter: window=%q info=%+v", got, info)
	}
	if info := w.must("msg", 1, t0.Add(31*time.Minute)); info.LatestSeq != 3 {
		t.Fatalf("seq must continue at 3, got %d", info.LatestSeq)
	}
}

func TestMetaTTLDropsIdleStreamAndRotatesEpoch(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := t0
	s := NewMemoryStorage(
		WithEpochSource(fixedEpochs("ep-1", "ep-2", "ep-3")),
		WithClock(func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }),
		WithRetention(Retention{MetaTTL: time.Hour}),
	)
	idle := newWriter(t, s, "idle")
	newWriter(t, s, "kept")
	idle.must("msg", 1, t0)

	mu.Lock()
	clock = t0.Add(50 * time.Minute)
	mu.Unlock()
	if err := s.Touch(ctx, "kept"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if err := s.Sweep(ctx, t0.Add(61*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, ok, _ := s.Stream(ctx, "idle"); ok {
		t.Fatalf("stream idle beyond MetaTTL must be dropped")
	}
	if kept, ok, _ := s.Stream(ctx, "kept"); !ok || kept.Epoch != "ep-2" {
		t.Fatalf("touched stream must survive: ok=%v %+v", ok, kept)
	}
	if list, _ := s.List(ctx); len(list) != 1 || list[0].SessionID != "kept" {
		t.Fatalf("List after sweep: %+v", list)
	}
	info, err := s.EnsureStream(ctx, "idle")
	if err != nil || info.Epoch != "ep-3" || info.LatestSeq != 0 {
		t.Fatalf("recreated stream must start a new epoch at seq 0: %+v err=%v", info, err)
	}
}

func TestAppendCopiesPayload(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage()
	if _, err := s.EnsureStream(ctx, "copy"); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	payload := json.RawMessage(`{"text":"original"}`)
	if _, _, err := s.Append(ctx, "copy", "t", payload, time.Now()); err != nil {
		t.Fatalf("Append: %v", err)
	}
	payload[0] = 'X'
	got, _, _ := s.ReadAfter(ctx, "copy", 0)
	if string(got[0].Payload) != `{"text":"original"}` {
		t.Fatalf("stored payload mutated via input buffer: %s", got[0].Payload)
	}
}

func TestConcurrentRace(t *testing.T) {
	s := NewMemoryStorage(WithRetention(Retention{MaxHistoryBytes: 2048}))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		w := newWriter(t, s, fmt.Sprintf("s-%d", i))
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				typ := "d"
				switch j % 20 {
				case 0:
					typ = model.TypeRunStarted
				case 19:
					typ = model.TypeRunFinished
				}
				if _, err := w.append(typ, 20, time.Now().UTC()); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				events, info, err := s.ReadAfter(context.Background(), w.id, 0)
				if err != nil {
					t.Errorf("ReadAfter: %v", err)
					return
				}
				for k := 1; k < len(events); k++ {
					if events[k].Seq != events[k-1].Seq+1 {
						t.Errorf("non-contiguous window: %d then %d", events[k-1].Seq, events[k].Seq)
						return
					}
				}
				if n := len(events); n > 0 && events[n-1].Seq != info.LatestSeq {
					t.Errorf("window/info mismatch: last=%d latest=%d", events[n-1].Seq, info.LatestSeq)
					return
				}
				_, _ = s.List(context.Background())
				_ = s.Sweep(context.Background(), time.Now())
			}
		}()
	}
	wg.Wait()
}

func TestOversizedNewestEventIsKept(t *testing.T) {
	now := time.Now().UTC()
	s := NewMemoryStorage(WithRetention(Retention{MaxHistoryBytes: size("msg", 100)}))
	w := newWriter(t, s, "big")

	w.must("msg", 1000, now) // larger than the whole history budget
	if got, info := w.window(); got != "1" || info.EarliestSeq != 1 {
		t.Fatalf("the newest event must be kept even when it exceeds MaxHistoryBytes: window=%q info=%+v", got, info)
	}
	w.must("msg", 10, now)
	if got, _ := w.window(); got != "2" {
		t.Fatalf("an older oversized event must go once a newer one exists: window=%q", got)
	}
}
