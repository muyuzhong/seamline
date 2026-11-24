package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

func TestAtomicIdempotencyKey(t *testing.T) {
	st := &flakyStore{MemoryStorage: storage.NewMemoryStorage()}
	b := New(st, Config{IdempotencyWindow: 150 * time.Millisecond})
	closeBroker(t, b)
	ctx := context.Background()
	const sid = "idem"

	sub, err := b.Subscribe(ctx, sid, 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	first := mustPublish(t, b, sid, model.PublishRequest{Type: "user", Delta: "hi", IdempotencyKey: "k1"})
	retry := mustPublish(t, b, sid, model.PublishRequest{Type: "user", Delta: "hi", IdempotencyKey: "k1"})
	if first.CommittedOffset != 1 || !retry.Deduplicated || retry.CommittedOffset != 1 || retry.Epoch != first.Epoch {
		t.Fatalf("retry must return the original result: first=%+v retry=%+v", first, retry)
	}
	if res := mustPublish(t, b, sid, model.PublishRequest{Type: "user", Delta: "other", IdempotencyKey: "k2"}); res.CommittedOffset != 2 || res.Deduplicated {
		t.Fatalf("different key must commit: %+v", res)
	}
	for _, want := range []uint64{1, 2} {
		if ev := recvEvent(t, sub, 2*time.Second); ev.Offset != want {
			t.Fatalf("duplicate must not be fanned out: got offset %d, want %d", ev.Offset, want)
		}
	}

	// A key is only remembered after the record is committed.
	st.failNextAppend.Store(true)
	if _, err := b.Publish(ctx, sid, model.PublishRequest{Type: "user", Delta: "x", IdempotencyKey: "k3"}); !errors.Is(err, errInjectedStorage) {
		t.Fatalf("want injected failure, got %v", err)
	}
	if res := mustPublish(t, b, sid, model.PublishRequest{Type: "user", Delta: "x", IdempotencyKey: "k3"}); res.CommittedOffset != 3 || res.Deduplicated {
		t.Fatalf("retry after a failed append must commit: %+v", res)
	}

	time.Sleep(200 * time.Millisecond)
	if res := mustPublish(t, b, sid, model.PublishRequest{Type: "user", Delta: "hi", IdempotencyKey: "k1"}); res.CommittedOffset != 4 || res.Deduplicated {
		t.Fatalf("key must expire after the window: %+v", res)
	}
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "s", Seq: 1, Delta: "x", IdempotencyKey: "k"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("idempotency key on a stream chunk: want ErrInvalidInput, got %v", err)
	}
	if recs, _, _ := st.ReadFrom(ctx, sid, 0); len(recs) != 4 {
		t.Fatalf("expected 4 committed records, got %d", len(recs))
	}
}
