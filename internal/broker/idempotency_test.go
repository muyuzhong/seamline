package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

func TestIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{MemoryStorage: hotwindow.NewMemoryStorage()}
	b := New(st, Config{IdempotencyWindow: 150 * time.Millisecond})
	closeBroker(t, b)
	const sid = "idem"

	sub, err := b.Subscribe(ctx, sid, 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	keyed := func(key string) model.PublishRequest {
		req := ev("assistant_delta", textPayload(key))
		req.IdempotencyKey = key
		return req
	}
	first := mustPublish(t, b, sid, keyed("k1"))
	retry := mustPublish(t, b, sid, keyed("k1"))
	if first.Seq != 1 || !retry.Deduplicated || retry.Seq != 1 || retry.Epoch != first.Epoch {
		t.Fatalf("retry must return the original result: first=%+v retry=%+v", first, retry)
	}
	if res := mustPublish(t, b, sid, keyed("k2")); res.Seq != 2 || res.Deduplicated {
		t.Fatalf("a different key must commit: %+v", res)
	}
	for _, want := range []uint64{1, 2} {
		if got := recvEvent(t, sub); got.Seq != want {
			t.Fatalf("a duplicate must not be fanned out: got seq %d, want %d", got.Seq, want)
		}
	}

	// A key is only remembered after the event is committed.
	st.failNextAppend.Store(true)
	if _, err := b.Publish(ctx, sid, keyed("k3")); !errors.Is(err, errInjectedStorage) {
		t.Fatalf("want injected failure, got %v", err)
	}
	if res := mustPublish(t, b, sid, keyed("k3")); res.Seq != 3 || res.Deduplicated {
		t.Fatalf("retry after a failed append must commit: %+v", res)
	}

	time.Sleep(200 * time.Millisecond)
	if res := mustPublish(t, b, sid, keyed("k1")); res.Seq != 4 || res.Deduplicated {
		t.Fatalf("a key must expire after the window: %+v", res)
	}
	if events, _, _ := st.ReadAfter(ctx, sid, 0); len(events) != 4 {
		t.Fatalf("expected 4 stored events, got %d", len(events))
	}
}

func TestWindowIndexReinsertSurvivesPrune(t *testing.T) {
	w := newWindowIndex[int](10 * time.Second)
	t0 := time.Unix(1000, 0)
	w.put("k", 1, t0)
	w.put("k", 2, t0.Add(5*time.Second))      // re-insert extends the deadline to t0+15s
	w.put("other", 3, t0.Add(11*time.Second)) // prunes k's first, stale order slot

	if v, ok := w.get("k", t0.Add(12*time.Second)); !ok || v != 2 {
		t.Fatalf("re-inserted key must survive pruning of its stale slot: v=%d ok=%v", v, ok)
	}
	if _, ok := w.get("k", t0.Add(15*time.Second)); ok {
		t.Fatalf("key must expire at its latest deadline")
	}
	w.put("x", 0, t0.Add(30*time.Second))
	if w.len() != 1 {
		t.Fatalf("expired entries must be pruned, len=%d", w.len())
	}
}
