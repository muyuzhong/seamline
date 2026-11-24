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

// writer appends events to one stream and tracks the run bookkeeping like the broker does.
type writer struct {
	t     *testing.T
	s     *MemoryStorage
	id    string
	epoch string
	seq   uint64
	pin   uint64
}

func newWriter(t *testing.T, s *MemoryStorage, id string) *writer {
	t.Helper()
	info, err := s.EnsureStream(context.Background(), id)
	if err != nil {
		t.Fatalf("EnsureStream(%s): %v", id, err)
	}
	return &writer{t: t, s: s, id: id, epoch: info.Epoch, seq: info.LatestSeq}
}

// commit builds the next commit for eventType: run_started opens a run, terminal types end it.
func (w *writer) commit(eventType string, payloadLen int, at time.Time) Commit {
	seq := w.seq + 1
	pin, state := w.pin, model.StateIdle
	if eventType == model.TypeRunStarted && pin == 0 {
		pin = seq
	}
	if terminal, ok := model.TerminalState(eventType); ok && pin != 0 {
		pin, state = 0, terminal
	}
	if pin != 0 {
		state = model.StateRunning
	}
	payload := json.RawMessage(`"` + strings.Repeat("x", max(payloadLen-2, 0)) + `"`)
	return Commit{
		Event:             model.Event{Seq: seq, Epoch: w.epoch, Type: eventType, Payload: payload, CreatedAt: at},
		State:             state,
		ActiveRunStartSeq: pin,
	}
}

func (w *writer) append(eventType string, payloadLen int, at time.Time) error {
	c := w.commit(eventType, payloadLen, at)
	if err := w.s.Append(context.Background(), w.id, c); err != nil {
		return err
	}
	w.seq, w.pin = c.Event.Seq, c.ActiveRunStartSeq
	return nil
}

func (w *writer) must(eventType string, payloadLen int, at time.Time) {
	w.t.Helper()
	if err := w.append(eventType, payloadLen, at); err != nil {
		w.t.Fatalf("append %s at seq %d: %v", eventType, w.seq+1, err)
	}
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

	ev := func(seq uint64, epoch string) Commit {
		return Commit{Event: model.Event{Seq: seq, Epoch: epoch, Type: "msg", CreatedAt: now}, State: model.StateIdle}
	}
	if err := s.Append(ctx, "a", ev(1, "ep-1")); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("append before EnsureStream: want ErrStreamNotFound, got %v", err)
	}
	info, err := s.EnsureStream(ctx, "a")
	if err != nil || info.Epoch != "ep-1" || info.EarliestSeq != 1 || info.LatestSeq != 0 || info.State != model.StateIdle {
		t.Fatalf("new stream: %+v err=%v", info, err)
	}
	bad := []struct {
		name string
		id   string
		c    Commit
		want error
	}{
		{"empty session", "", ev(1, "ep-1"), ErrInvalidSessionID},
		{"foreign epoch", "a", ev(1, "other"), ErrEpochMismatch},
		{"seq gap", "a", ev(2, "ep-1"), ErrNonMonotonicSeq},
		{"seq zero", "a", ev(0, "ep-1"), ErrNonMonotonicSeq},
	}
	for _, tc := range bad {
		if err := s.Append(ctx, tc.id, tc.c); !errors.Is(err, tc.want) {
			t.Fatalf("%s: want %v, got %v", tc.name, tc.want, err)
		}
	}

	w := newWriter(t, s, "a")
	for i := 0; i < 5; i++ {
		w.must("msg", 10, now)
	}
	if err := s.Append(ctx, "a", ev(5, "ep-1")); !errors.Is(err, ErrNonMonotonicSeq) {
		t.Fatalf("duplicate seq: want ErrNonMonotonicSeq, got %v", err)
	}
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
	if err := w.append("d", 100, now); !errors.Is(err, ErrActiveRunQuotaExceeded) {
		t.Fatalf("want ErrActiveRunQuotaExceeded, got %v", err)
	}
	if _, info := w.window(); info.LatestSeq != 3 || info.ActiveRunBytes != size(model.TypeRunStarted, 10)+2*size("d", 100) {
		t.Fatalf("rejected append must not change the stream: %+v", info)
	}
	// A terminal event is exempt, so the run can always end.
	w.must(model.TypeRunFailed, 1000, now)
	if _, info := w.window(); info.ActiveRunStartSeq != 0 || info.ActiveRunBytes != 0 || info.State != model.StateFailed {
		t.Fatalf("terminal event must end the run: %+v", info)
	}
	// A new run starts with a fresh budget.
	w.must(model.TypeRunStarted, 10, now)
	w.must("d", 100, now)
}

func TestInvalidRunTransitions(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	s := NewMemoryStorage(WithEpochSource(fixedEpochs("ep")))
	w := newWriter(t, s, "tr")
	w.must("msg", 1, now)

	try := func(seq, pin uint64) error {
		return s.Append(ctx, "tr", Commit{Event: model.Event{Seq: seq, Epoch: "ep", Type: "x", CreatedAt: now}, ActiveRunStartSeq: pin})
	}
	if err := try(2, 1); !errors.Is(err, ErrInvalidRunTransition) {
		t.Fatalf("a run cannot start before its first event: %v", err)
	}
	if err := try(2, 2); err != nil { // run starts at seq 2
		t.Fatalf("starting a run: %v", err)
	}
	if err := try(3, 3); !errors.Is(err, ErrInvalidRunTransition) {
		t.Fatalf("a second run cannot start while one is active: %v", err)
	}
	if err := try(3, 2); err != nil {
		t.Fatalf("continuing the run: %v", err)
	}
	if err := try(4, 0); err != nil {
		t.Fatalf("ending the run: %v", err)
	}
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
	w.must("msg", 1, t0.Add(31*time.Minute))
	if w.seq != 3 {
		t.Fatalf("seq must continue at 3, got %d", w.seq)
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

func TestDeepCopyIsolation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStorage(WithEpochSource(fixedEpochs("ep")))
	if _, err := s.EnsureStream(ctx, "copy"); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	payload := json.RawMessage(`{"text":"original"}`)
	if err := s.Append(ctx, "copy", Commit{Event: model.Event{Seq: 1, Epoch: "ep", Type: "t", Payload: payload}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	payload[0] = 'X'
	got, _, _ := s.ReadAfter(ctx, "copy", 0)
	if string(got[0].Payload) != `{"text":"original"}` {
		t.Fatalf("stored payload mutated via input buffer: %s", got[0].Payload)
	}
	got[0].Payload[0] = 'Y'
	again, _, _ := s.ReadAfter(ctx, "copy", 0)
	if string(again[0].Payload) != `{"text":"original"}` {
		t.Fatalf("stored payload mutated via output buffer: %s", again[0].Payload)
	}
}

func TestContextCancellation(t *testing.T) {
	s := NewMemoryStorage()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := map[string]error{
		"EnsureStream": func() error { _, err := s.EnsureStream(ctx, "a"); return err }(),
		"Stream":       func() error { _, _, err := s.Stream(ctx, "a"); return err }(),
		"Append":       s.Append(ctx, "a", Commit{}),
		"ReadAfter":    func() error { _, _, err := s.ReadAfter(ctx, "a", 0); return err }(),
		"Touch":        s.Touch(ctx, "a"),
		"Sweep":        s.Sweep(ctx, time.Now()),
		"List":         func() error { _, err := s.List(ctx); return err }(),
	}
	for name, err := range calls {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: want context.Canceled, got %v", name, err)
		}
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
				if err := w.append(typ, 20, time.Now().UTC()); err != nil {
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
