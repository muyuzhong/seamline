package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
)

func TestSessionEpochSurvivesHistoryTTLButRotatesAfterMetaTTL(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now()
	store := hotwindow.NewMemoryStorage(
		hotwindow.WithEpochSource(fixedEpochs("ep-1", "ep-2")),
		hotwindow.WithClock(func() time.Time { return t0 }),
		hotwindow.WithRetention(hotwindow.Retention{HistoryTTL: time.Minute, MetaTTL: time.Hour}),
	)
	b := New(store, Config{IdleSessionTTL: 20 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)
	const sid = "epochs"

	mustPublish(t, b, sid, ev("msg", textPayload("one")))
	if res := mustPublish(t, b, sid, ev("msg", textPayload("two"))); res.Epoch != "ep-1" || res.Seq != 2 {
		t.Fatalf("unexpected publish result: %+v", res)
	}

	// HistoryTTL elapses: events are gone but the stream identity and seq counter remain.
	if err := store.Sweep(ctx, t0.Add(2*time.Minute)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if meta := mustInspect(t, b, sid); meta.Epoch != "ep-1" || meta.EarliestSeq != 3 || meta.LatestSeq != 2 {
		t.Fatalf("after HistoryTTL want an empty window of ep-1 at seq 2, got %+v", meta)
	}
	if res := mustPublish(t, b, sid, ev("msg", textPayload("three"))); res.Seq != 3 || res.Epoch != "ep-1" {
		t.Fatalf("seq must continue after history eviction: %+v", res)
	}

	waitFor(t, "idle actor eviction", func() bool { return b.activeActorCount() == 0 })

	// MetaTTL elapses with no live actor: the stream is dropped and recreated with a new epoch.
	if err := store.Sweep(ctx, t0.Add(2*time.Hour)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res := mustPublish(t, b, sid, ev("msg", textPayload("new life"))); res.Epoch != "ep-2" || res.Seq != 1 {
		t.Fatalf("recreated session must start a new epoch at seq 1: %+v", res)
	}
}

func TestSubscribedSessionStreamDoesNotExpire(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now()
	var clockMu sync.Mutex
	clock := t0
	store := hotwindow.NewMemoryStorage(
		hotwindow.WithEpochSource(fixedEpochs("ep-live", "ep-new")),
		hotwindow.WithClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }),
		hotwindow.WithRetention(hotwindow.Retention{MetaTTL: time.Hour}),
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
	if res := mustPublish(t, b, "listening", ev("msg", "")); res.Epoch != "ep-live" {
		t.Fatalf("publish must reuse the live epoch, got %+v", res)
	}
	if got := recvEvent(t, sub); got.Seq != 1 || got.Epoch != "ep-live" {
		t.Fatalf("subscriber must receive the event in its epoch, got %+v", got)
	}
}
