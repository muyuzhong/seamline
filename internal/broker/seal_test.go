package broker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

func TestIdleInFlightStreamIsSealed(t *testing.T) {
	b := New(storage.NewMemoryStorage(), Config{InFlightIdleTimeout: 80 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)
	ctx := context.Background()
	const sid = "seal"

	sub, err := b.Subscribe(ctx, sid, 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 1, Type: "assistant", Delta: "Hel", Meta: json.RawMessage(`{"call_id":"c1"}`)})
	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 2, Type: "assistant", Delta: "lo"})

	// s2 keeps producing well inside the timeout and must stay in flight while s1 is sealed.
	for i := 1; i <= 20; i++ {
		mustPublish(t, b, sid, model.PublishRequest{StreamID: "s2", Seq: uint64(i), Type: "assistant", Delta: "."})
		time.Sleep(10 * time.Millisecond)
	}

	var sealed model.OutEvent
	for sealed.Kind != model.EventCommitted {
		sealed = recvEvent(t, sub, 2*time.Second)
	}
	if sealed.StreamID != "s1" || sealed.Sealed != model.SealedIdleTimeout || decodeContent(t, sealed.Payload) != "Hello" ||
		string(sealed.Meta) != `{"call_id":"c1"}` || sealed.Offset != 1 || sealed.Type != "assistant" {
		t.Fatalf("unexpected sealed record: %+v (%s)", sealed, sealed.Payload)
	}

	snap, _, _ := b.Inspect(ctx, sid)
	if len(snap.InFlights) != 1 || snap.InFlights[0].StreamID != "s2" || snap.InFlights[0].LastSeq != 20 || snap.Meta.State != model.StateRunning {
		t.Fatalf("active stream must not be sealed: %+v", snap)
	}
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "s1", Seq: 3, Delta: "!"}); !errors.Is(err, ErrStreamFinalized) {
		t.Fatalf("late chunk after sealing: want ErrStreamFinalized, got %v", err)
	}
	if res := mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 2, Delta: "lo"}); !res.Deduplicated || res.CommittedOffset != 1 {
		t.Fatalf("retry of an accepted chunk must be deduplicated: %+v", res)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap, _, _ = b.Inspect(ctx, sid); len(snap.InFlights) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(snap.InFlights) != 0 || snap.Meta.State != model.StateIdle || snap.Meta.LastOffset != 2 {
		t.Fatalf("s2 must be sealed once it goes quiet: %+v", snap)
	}
}
