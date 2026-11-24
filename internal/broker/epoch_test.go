package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

func TestSessionEpochSurvivesHistoryTTLButRotatesAfterMetaTTL(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now()
	store := storage.NewMemoryStorage(
		storage.WithEpochSource(fixedEpochs("ep-1", "ep-2")),
		storage.WithClock(func() time.Time { return t0 }),
		storage.WithRetention(storage.Retention{HistoryTTL: time.Minute, MetaTTL: time.Hour}),
	)
	b := New(store, Config{IdleSessionTTL: 20 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)
	const sid = "epochs"

	mustPublish(t, b, sid, model.PublishRequest{Type: "msg", Delta: "one"})
	if res := mustPublish(t, b, sid, model.PublishRequest{Type: "msg", Delta: "two"}); res.Epoch != "ep-1" || res.CommittedOffset != 2 {
		t.Fatalf("unexpected publish result: %+v", res)
	}

	// HistoryTTL elapses: records are gone but the stream identity and offset counter remain.
	if err := store.Sweep(ctx, t0.Add(2*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	sub, err := b.Subscribe(ctx, sid, 1, "ep-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ev := recvEvent(t, sub, 2*time.Second)
	want := model.CursorReset{Reason: model.ResetCursorExpired, RequestedEpoch: "ep-1", RequestedOffset: 1, CurrentEpoch: "ep-1", EarliestOffset: 3, LatestOffset: 2}
	if ev.Kind != model.EventCursorReset || *ev.Reset != want {
		t.Fatalf("after HistoryTTL want %+v, got %+v (%+v)", want, ev, ev.Reset)
	}
	if res := mustPublish(t, b, sid, model.PublishRequest{Type: "msg", Delta: "three"}); res.CommittedOffset != 3 || res.Epoch != "ep-1" {
		t.Fatalf("offsets must continue after history eviction: %+v", res)
	}
	if ev := recvEvent(t, sub, 2*time.Second); ev.Offset != 3 {
		t.Fatalf("want live record 3, got %+v", ev)
	}
	sub.Cancel()

	waitFor(t, "idle actor eviction", func() bool { return b.activeActorCount() == 0 })

	// MetaTTL elapses with no live actor: the stream is dropped and recreated with a new epoch.
	if err := store.Sweep(ctx, t0.Add(2*time.Hour)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	res := mustPublish(t, b, sid, model.PublishRequest{Type: "msg", Delta: "new life"})
	if res.Epoch != "ep-2" || res.CommittedOffset != 1 {
		t.Fatalf("recreated session must start a new epoch at offset 1: %+v", res)
	}
	sub2, err := b.Subscribe(ctx, sid, 3, "ep-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Cancel()
	ev = recvEvent(t, sub2, 2*time.Second)
	want = model.CursorReset{Reason: model.ResetEpochMismatch, RequestedEpoch: "ep-1", RequestedOffset: 3, CurrentEpoch: "ep-2", EarliestOffset: 1, LatestOffset: 1}
	if ev.Kind != model.EventCursorReset || *ev.Reset != want {
		t.Fatalf("after MetaTTL want %+v, got %+v (%+v)", want, ev, ev.Reset)
	}
	if ev := recvEvent(t, sub2, 2*time.Second); ev.Offset != 1 || ev.Epoch != "ep-2" {
		t.Fatalf("want record ep-2:1 after reset, got %+v", ev)
	}
}

func TestSubscribedSessionStreamDoesNotExpire(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now()
	var clockMu sync.Mutex
	clock := t0
	store := storage.NewMemoryStorage(
		storage.WithEpochSource(fixedEpochs("ep-live", "ep-new")),
		storage.WithClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }),
		storage.WithRetention(storage.Retention{MetaTTL: time.Hour}),
	)
	b := New(store, Config{SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)

	sub, err := b.Subscribe(ctx, "listening", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Two silent hours pass while the subscriber stays attached; the sweeper keeps refreshing the stream.
	clockMu.Lock()
	clock = t0.Add(2 * time.Hour)
	clockMu.Unlock()
	time.Sleep(50 * time.Millisecond)
	if err := store.Sweep(ctx, t0.Add(2*time.Hour+30*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	info, ok, err := store.Stream(ctx, "listening")
	if err != nil || !ok || info.Epoch != "ep-live" {
		t.Fatalf("stream of a live subscription must not expire: ok=%v info=%+v err=%v", ok, info, err)
	}
	if res := mustPublish(t, b, "listening", model.PublishRequest{Type: "msg", Delta: "still here"}); res.Epoch != "ep-live" {
		t.Fatalf("publish must reuse the live epoch, got %+v", res)
	}
	if ev := recvEvent(t, sub, 2*time.Second); ev.Offset != 1 || ev.Epoch != "ep-live" {
		t.Fatalf("subscriber must receive the record in its epoch, got %+v", ev)
	}
}
