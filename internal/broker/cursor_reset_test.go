package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

type resetCase struct {
	name       string
	epoch      string
	seq        uint64
	wantReason string
	wantFirst  uint64 // first replayed seq (0 = none)
}

func TestCursorResetReasons(t *testing.T) {
	ctx := context.Background()
	size := int64(len(textPayload(strings.Repeat("x", 300))) + len("msg") + 64)
	store := hotwindow.NewMemoryStorage(
		hotwindow.WithEpochSource(fixedEpochs("ep-A")),
		hotwindow.WithRetention(hotwindow.Retention{MaxHistoryBytes: 2*size + size/2}),
	)
	b := New(store, Config{})
	closeBroker(t, b)
	const sid = "resets"
	for i := 0; i < 5; i++ {
		mustPublish(t, b, sid, ev("msg", textPayload(strings.Repeat("x", 300))))
	}
	// History keeps seqs 4 and 5; an active run then starts at 6.
	mustPublish(t, b, sid, ev(model.TypeRunStarted, ""))
	mustPublish(t, b, sid, ev("assistant_delta", textPayload("partial")))

	cases := []resetCase{
		{"fresh", "", 0, "", 4},
		{"valid at earliest-1", "ep-A", 3, "", 4},
		{"valid mid-window", "ep-A", 5, "", 6},
		{"valid at head", "ep-A", 7, "", 0},
		{"expired", "ep-A", 2, model.ResetCursorExpired, 4},
		{"expired from zero", "ep-A", 0, model.ResetCursorExpired, 4},
		{"epoch mismatch", "old-epoch", 7, model.ResetEpochMismatch, 4},
		{"bare seq", "", 3, model.ResetEpochMismatch, 4},
		{"ahead", "ep-A", 9, model.ResetCursorAhead, 4},
	}
	subs := make([]*Subscription, len(cases))
	for i, tc := range cases {
		sub, err := b.Subscribe(ctx, sid, tc.seq, tc.epoch)
		if err != nil {
			t.Fatalf("%s: Subscribe: %v", tc.name, err)
		}
		defer sub.Cancel()
		subs[i] = sub
		checkResetPrefix(t, tc, sub)
	}

	mustPublish(t, b, sid, ev("assistant_delta", textPayload("live")))
	for i, tc := range cases {
		if got := recvEvent(t, subs[i]); got.Seq != 8 {
			t.Fatalf("%s: live event after catch-up = %+v, want seq 8", tc.name, got)
		}
	}
}

func checkResetPrefix(t *testing.T, tc resetCase, sub *Subscription) {
	t.Helper()
	if tc.wantReason != "" {
		d := recv(t, sub, 2*time.Second)
		want := model.CursorReset{
			Reason: tc.wantReason, RequestedEpoch: tc.epoch, RequestedSeq: tc.seq, CurrentEpoch: "ep-A",
			EarliestSeq: 4, LatestSeq: 7, ActiveRunStartSeq: 6,
		}
		if d.Kind != model.DeliveryCursorReset || d.Reset == nil || *d.Reset != want {
			t.Fatalf("%s: first delivery = %+v (reset=%+v), want cursor reset %+v", tc.name, d, d.Reset, want)
		}
	}
	if tc.wantFirst == 0 {
		return
	}
	for seq := tc.wantFirst; seq <= 7; seq++ {
		if got := recvEvent(t, sub); got.Seq != seq {
			t.Fatalf("%s: replay got seq %d, want %d", tc.name, got.Seq, seq)
		}
	}
}

// gappyStore loses one event on reads, simulating history that disappears under a replay.
type gappyStore struct {
	*hotwindow.MemoryStorage
	dropSeq atomic.Uint64
}

func (g *gappyStore) ReadAfter(ctx context.Context, id string, after uint64) ([]model.Event, hotwindow.StreamInfo, error) {
	events, info, err := g.MemoryStorage.ReadAfter(ctx, id, after)
	drop := g.dropSeq.Load()
	out := events[:0]
	for _, e := range events {
		if e.Seq != drop {
			out = append(out, e)
		}
	}
	return out, info, err
}

func TestReplayGapEndsWithInsufficientState(t *testing.T) {
	for _, drop := range []uint64{2, 3} {
		t.Run(fmt.Sprintf("drop-%d", drop), func(t *testing.T) {
			st := &gappyStore{MemoryStorage: hotwindow.NewMemoryStorage()}
			b := New(st, Config{})
			closeBroker(t, b)
			for i := 1; i <= 3; i++ {
				mustPublish(t, b, "gap", ev("msg", ""))
			}
			st.dropSeq.Store(drop)

			sub, err := b.Subscribe(context.Background(), "gap", 0, "")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			defer sub.Cancel()
			var got []uint64
			deadline := time.After(2 * time.Second)
			for open := true; open; {
				select {
				case d, ok := <-sub.Events:
					if ok {
						got = append(got, d.Event.Seq)
					}
					open = ok
				case <-deadline:
					t.Fatalf("subscription must end on a replay gap; still open after %v", got)
				}
			}
			if !errors.Is(sub.Err(), ErrInsufficientState) {
				t.Fatalf("want ErrInsufficientState after receiving %v, got %v", got, sub.Err())
			}
			for _, seq := range got {
				if seq >= drop {
					t.Fatalf("events after the gap must not be delivered, got %v", got)
				}
			}
		})
	}
}

// evictingStore runs a hook once, at the start of the next ReadAfter, to publish between the
// subscription snapshot and the catch-up read.
type evictingStore struct {
	*hotwindow.MemoryStorage
	beforeRead atomic.Pointer[func()]
}

func (s *evictingStore) ReadAfter(ctx context.Context, id string, after uint64) ([]model.Event, hotwindow.StreamInfo, error) {
	if hook := s.beforeRead.Swap(nil); hook != nil {
		(*hook)()
	}
	return s.MemoryStorage.ReadAfter(ctx, id, after)
}

// A caught-up cursor must not be reported as expired when publishes after the snapshot evicted
// snapSeq+1 from storage before the catch-up read: those events arrive through liveCh.
func TestCaughtUpCursorNotExpiredByPublishesAfterSnapshot(t *testing.T) {
	payload := textPayload(strings.Repeat("x", 300))
	size := int64(len(payload) + len("msg") + 64)
	st := &evictingStore{MemoryStorage: hotwindow.NewMemoryStorage(
		hotwindow.WithEpochSource(fixedEpochs("ep-A")),
		hotwindow.WithRetention(hotwindow.Retention{MaxHistoryBytes: size + size/2}),
	)}
	b := New(st, Config{})
	closeBroker(t, b)
	const sid = "caught-up"
	mustPublish(t, b, sid, ev("msg", payload))
	mustPublish(t, b, sid, ev("msg", payload))

	hook := func() {
		// Seqs 3 and 4 evict 2 and 3, so the window is [4, 4] when the replay reads it.
		mustPublish(t, b, sid, ev("msg", payload))
		mustPublish(t, b, sid, ev("msg", payload))
	}
	st.beforeRead.Store(&hook)
	sub, err := b.Subscribe(context.Background(), sid, 2, "ep-A")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()
	for _, want := range []uint64{3, 4} {
		if got := recvEvent(t, sub); got.Seq != want {
			t.Fatalf("got seq %d, want %d", got.Seq, want)
		}
	}
}
