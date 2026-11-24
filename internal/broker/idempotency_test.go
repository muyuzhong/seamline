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
