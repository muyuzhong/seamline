package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

type resetCase struct {
	name       string
	epoch      string
	offset     uint64
	wantReason string
	wantFirst  uint64 // first replayed record offset (0 = none)
}

func TestCursorResetReasons(t *testing.T) {
	ctx := context.Background()
	recSize := int64(len(`{"content":""}`) + 300 + len("msg") + 64)
	store := storage.NewMemoryStorage(
		storage.WithEpochSource(fixedEpochs("ep-A")),
		storage.WithRetention(storage.Retention{MaxHistoryBytes: 2*recSize + recSize/2}),
	)
	b := New(store, Config{})
	closeBroker(t, b)
	const sid = "resets"
	for i := 1; i <= 5; i++ {
		mustPublish(t, b, sid, model.PublishRequest{Type: "msg", Delta: strings.Repeat(fmt.Sprint(i), 300)})
	}

	cases := []resetCase{
		{"fresh", "", 0, "", 4},
		{"valid at earliest-1", "ep-A", 3, "", 4},
		{"valid mid-window", "ep-A", 4, "", 5},
		{"valid at head", "ep-A", 5, "", 0},
		{"expired", "ep-A", 2, model.ResetCursorExpired, 4},
		{"expired from zero", "ep-A", 0, model.ResetCursorExpired, 4},
		{"epoch mismatch", "old-epoch", 5, model.ResetEpochMismatch, 4},
		{"bare offset", "", 3, model.ResetEpochMismatch, 4},
		{"ahead", "ep-A", 9, model.ResetCursorAhead, 4},
	}
	subs := make([]*Subscription, len(cases))
	for i, tc := range cases {
		sub, err := b.Subscribe(ctx, sid, tc.offset, tc.epoch)
		if err != nil {
			t.Fatalf("%s: Subscribe: %v", tc.name, err)
		}
		defer sub.Cancel()
		subs[i] = sub
		checkResetPrefix(t, tc, sub)
	}

	mustPublish(t, b, sid, model.PublishRequest{Type: "msg", Delta: "live"})
	for i, tc := range cases {
		if ev := recvEvent(t, subs[i], 2*time.Second); ev.Kind != model.EventCommitted || ev.Offset != 6 {
			t.Fatalf("%s: live record after catch-up = %+v, want offset 6", tc.name, ev)
		}
	}
}

func checkResetPrefix(t *testing.T, tc resetCase, sub *Subscription) {
	t.Helper()
	if tc.wantReason != "" {
		ev := recvEvent(t, sub, 2*time.Second)
		want := model.CursorReset{Reason: tc.wantReason, RequestedEpoch: tc.epoch, RequestedOffset: tc.offset, CurrentEpoch: "ep-A", EarliestOffset: 4, LatestOffset: 5}
		if ev.Kind != model.EventCursorReset || ev.Reset == nil || *ev.Reset != want {
			t.Fatalf("%s: first event = %+v (reset=%+v), want cursor_reset %+v", tc.name, ev, ev.Reset, want)
		}
	}
	if tc.wantFirst == 0 {
		return
	}
	for off := tc.wantFirst; off <= 5; off++ {
		if ev := recvEvent(t, sub, 2*time.Second); ev.Kind != model.EventCommitted || ev.Offset != off {
			t.Fatalf("%s: replay got %+v, want record %d", tc.name, ev, off)
		}
	}
}

// gappyStore loses one record on reads, simulating history that disappears under a replay.
type gappyStore struct {
	*storage.MemoryStorage
	dropOffset atomic.Uint64
}

func (g *gappyStore) ReadFrom(ctx context.Context, id string, after uint64) ([]model.Record, storage.StreamInfo, error) {
	recs, info, err := g.MemoryStorage.ReadFrom(ctx, id, after)
	drop := g.dropOffset.Load()
	out := recs[:0]
	for _, r := range recs {
		if r.Offset != drop {
			out = append(out, r)
		}
	}
	return out, info, err
}

func TestReplayGapEndsWithInsufficientState(t *testing.T) {
	for _, drop := range []uint64{2, 3} {
		t.Run(fmt.Sprintf("drop-%d", drop), func(t *testing.T) {
			st := &gappyStore{MemoryStorage: storage.NewMemoryStorage()}
			b := New(st, Config{})
			closeBroker(t, b)
			for i := 1; i <= 3; i++ {
				mustPublish(t, b, "gap", model.PublishRequest{Type: "msg", Delta: fmt.Sprint(i)})
			}
			st.dropOffset.Store(drop)

			sub, err := b.Subscribe(context.Background(), "gap", 0, "")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			defer sub.Cancel()
			var got []uint64
			deadline := time.After(2 * time.Second)
			for open := true; open; {
				select {
				case ev, ok := <-sub.Events:
					if ok {
						got = append(got, ev.Offset)
					}
					open = ok
				case <-deadline:
					t.Fatalf("subscription must end on a replay gap; still open after %v", got)
				}
			}
			if !errors.Is(sub.Err(), ErrInsufficientState) {
				t.Fatalf("want ErrInsufficientState after receiving %v, got %v", got, sub.Err())
			}
			for _, off := range got {
				if off >= drop {
					t.Fatalf("records after the gap must not be delivered, got %v", got)
				}
			}
		})
	}
}
