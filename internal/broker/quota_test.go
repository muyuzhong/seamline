package broker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

func TestInFlightAndEventByteQuotas(t *testing.T) {
	b := New(storage.NewMemoryStorage(), Config{MaxInFlightBytes: 12, MaxEventBytes: 64})
	closeBroker(t, b)
	ctx := context.Background()
	const sid = "quota"

	if _, err := b.Publish(ctx, sid, model.PublishRequest{Type: "user", Delta: strings.Repeat("x", 65)}); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized delta: want ErrEventTooLarge, got %v", err)
	}
	bigPayload := json.RawMessage(`"` + strings.Repeat("x", 63) + `"`)
	if _, err := b.Publish(ctx, sid, model.PublishRequest{Type: "user", Payload: bigPayload}); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized payload: want ErrEventTooLarge, got %v", err)
	}

	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 1, Delta: "aaaa"})
	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s2", Seq: 1, Delta: "bbbb"})

	// 8 buffered bytes + 5 > 12: rejected without touching the stream.
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "s1", Seq: 2, Delta: "ccccc"}); !errors.Is(err, ErrInFlightQuotaExceeded) {
		t.Fatalf("want ErrInFlightQuotaExceeded, got %v", err)
	}
	snap, _, _ := b.Inspect(ctx, sid)
	if len(snap.InFlights) != 2 || snap.InFlights[0].StreamID != "s1" || snap.InFlights[0].Merged != "aaaa" || snap.InFlights[0].LastSeq != 1 {
		t.Fatalf("rejected chunk must not change the stream: %+v", snap.InFlights)
	}
	// A single-chunk stream's text must fit the quota too, because it becomes the record.
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "one", Seq: 1, Delta: "xxxxx", Fin: true}); !errors.Is(err, ErrInFlightQuotaExceeded) {
		t.Fatalf("single-chunk stream over quota: want ErrInFlightQuotaExceeded, got %v", err)
	}

	// The producer can always finalize what it has with an empty fin chunk; that frees quota.
	if res := mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 2, Fin: true}); res.CommittedOffset != 1 {
		t.Fatalf("empty fin: %+v", res)
	}
	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s3", Seq: 1, Delta: "ccccccc"}) // 4 + 7 = 11

	// A fin chunk with an explicit payload does not grow the merged buffer.
	res := mustPublish(t, b, sid, model.PublishRequest{StreamID: "s2", Seq: 2, Delta: strings.Repeat("z", 13), Payload: json.RawMessage(`{"content":"p"}`), Fin: true})
	if res.CommittedOffset != 2 {
		t.Fatalf("fin with payload: %+v", res)
	}

	// Meta counts toward the quota: 7 buffered + 9 bytes of meta > 12.
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "s5", Seq: 1, Meta: json.RawMessage(`{"k":"v1"}`)}); !errors.Is(err, ErrInFlightQuotaExceeded) {
		t.Fatalf("meta must count toward the in-flight quota, got %v", err)
	}
}
