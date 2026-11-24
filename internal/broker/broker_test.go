package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

func closeBroker(t *testing.T, b *Broker) {
	t.Helper()
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Broker.Close failed: %v", err)
		}
	})
}

func decodeContent(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var p struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("decode content payload %s: %v", string(raw), err)
	}
	return p.Content
}

func Test500DeltasCompactIntoSingleRecord(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStorage()
	b := New(store, Config{})
	closeBroker(t, b)

	if len(b.InstanceID()) != 12 {
		t.Fatalf("expected 12-char random hex instance id, got %q", b.InstanceID())
	}

	const totalChunks = 500
	var wantBuilder strings.Builder

	for i := 1; i <= totalChunks; i++ {
		tok := fmt.Sprintf("[tok-%03d]", i)
		_, _ = wantBuilder.WriteString(tok)
		fin := i == totalChunks
		typ := "assistant_delta"
		if fin {
			typ = "run_finished"
		}

		res, err := b.Publish(ctx, "session-500", model.PublishRequest{
			StreamID: "msg-1",
			Seq:      uint64(i),
			Type:     typ,
			Delta:    tok,
			Fin:      fin,
		})
		if err != nil {
			t.Fatalf("Publish seq %d failed: %v", i, err)
		}

		if !fin {
			if res.CommittedOffset != 0 || res.InFlightSeq != uint64(i) {
				t.Fatalf("unexpected non-fin result at seq %d: %+v", i, res)
			}
			if i == 250 {
				snap, ok, err := b.Inspect(ctx, "session-500")
				if err != nil || !ok {
					t.Fatalf("Inspect mid-stream failed: ok=%v err=%v", ok, err)
				}
				if snap.Meta.State != model.StateRunning || snap.Meta.LastOffset != 0 {
					t.Fatalf("expected running with LastOffset=0 mid-stream, got %+v", snap.Meta)
				}
				if snap.InFlight == nil || snap.InFlight.LastSeq != 250 || snap.InFlight.Merged != wantBuilder.String() {
					t.Fatalf("unexpected mid-stream InFlight: %+v", snap.InFlight)
				}
			}
		} else {
			if res.CommittedOffset != 1 || res.InFlightSeq != totalChunks {
				t.Fatalf("unexpected fin result: %+v", res)
			}
		}
	}

	// Storage must contain EXACTLY 1 compacted Record (NOT 500!).
	recs, _, err := store.ReadFrom(ctx, "session-500", 0)
	if err != nil {
		t.Fatalf("store.ReadFrom: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 500 deltas to compact into 1 single Record, got %d records", len(recs))
	}
	if recs[0].Offset != 1 || recs[0].StreamID != "msg-1" || recs[0].Type != "run_finished" {
		t.Fatalf("unexpected compacted record header: %+v", recs[0])
	}
	if got := decodeContent(t, recs[0].Payload); got != wantBuilder.String() {
		t.Fatalf("compacted text mismatch: len(got)=%d len(want)=%d", len(got), wantBuilder.Len())
	}

	snap, ok, err := b.Inspect(ctx, "session-500")
	if err != nil || !ok {
		t.Fatalf("Inspect post-compaction failed: ok=%v err=%v", ok, err)
	}
	if snap.InFlight != nil {
		t.Fatalf("expected InFlight == nil after compaction, got %+v", snap.InFlight)
	}
	if snap.Meta.LastOffset != 1 || snap.Meta.State != model.StateCompleted {
		t.Fatalf("expected LastOffset=1 and State=completed, got %+v", snap.Meta)
	}
}

// clientState simulates a real SSE client maintaining committed records + live in-flight buffers,
// supporting seamless disconnect/reconnect via Last-Event-ID (lastCommittedOffset + epoch).
type clientState struct {
	epoch       string
	lastOffset  uint64
	records     map[uint64]string
	inFlightID  string
	inFlightBuf strings.Builder
	streamTexts map[string]string
}

func newClientState() *clientState {
	return &clientState{
		records:     make(map[uint64]string),
		streamTexts: make(map[string]string),
	}
}

func (c *clientState) apply(t *testing.T, ev model.OutEvent) {
	t.Helper()
	switch ev.Kind {
	case model.EventCommitted:
		if ev.Offset <= c.lastOffset {
			t.Errorf("duplicate or non-monotonic EventCommitted offset %d (lastOffset=%d)", ev.Offset, c.lastOffset)
			return
		}
		if ev.Offset != c.lastOffset+1 {
			t.Errorf("gap in EventCommitted offset: got %d after %d", ev.Offset, c.lastOffset)
			return
		}
		c.lastOffset = ev.Offset
		c.epoch = ev.Epoch
		text := decodeContent(t, ev.Payload)
		c.records[ev.Offset] = text
		if ev.StreamID != "" && c.inFlightID == ev.StreamID {
			// Verify the text built from (inflight_snapshot + live deltas) matches the committed record!
			built := c.inFlightBuf.String()
			if built != text {
				t.Errorf("stream %s delta reconstruction mismatch:\nbuilt(%d)=%q\ncommitted(%d)=%q",
					ev.StreamID, len(built), built, len(text), text)
			}
			c.streamTexts[ev.StreamID] = built
			c.inFlightID = ""
			c.inFlightBuf.Reset()
		}
	case model.EventInFlightSnapshot:
		c.inFlightID = ev.StreamID
		c.inFlightBuf.Reset()
		_, _ = c.inFlightBuf.WriteString(ev.Delta)
	case model.EventDelta:
		if c.inFlightID != "" && c.inFlightID != ev.StreamID {
			t.Errorf("delta stream mismatch: active=%s got=%s", c.inFlightID, ev.StreamID)
			return
		}
		c.inFlightID = ev.StreamID
		_, _ = c.inFlightBuf.WriteString(ev.Delta)
	default:
		t.Errorf("unknown EventKind: %q", ev.Kind)
	}
}

func (c *clientState) resetUncommittedForReconnect() {
	c.inFlightID = ""
	c.inFlightBuf.Reset()
}

func Test50ConcurrentSubscribersZeroGapZeroDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store := storage.NewMemoryStorage()
	b := New(store, Config{
		SubscriberBufferSize: 1024,
	})
	closeBroker(t, b)

	const sessionID = "concurrent-50"
	const numSubscribers = 50
	const chunksPerStream = 150

	var wantStream1, wantStream2 strings.Builder
	for i := 1; i <= chunksPerStream; i++ {
		_, _ = wantStream1.WriteString(fmt.Sprintf("A%03d-", i))
		_, _ = wantStream2.WriteString(fmt.Sprintf("B%03d-", i))
	}

	wantOffsets := map[uint64]string{
		1: "User prompt 1: explain seamline",
		2: wantStream1.String(),
		3: "User prompt 2: test F5 reconnect",
		4: wantStream2.String(),
	}

	// Barrier so early subscribers are attached before producer starts.
	var earlyReady sync.WaitGroup
	var allSubsDone sync.WaitGroup

	for subIdx := 0; subIdx < numSubscribers; subIdx++ {
		if subIdx < 10 {
			earlyReady.Add(1)
		}
		allSubsDone.Add(1)

		go func(idx int) {
			defer allSubsDone.Done()
			rng := rand.New(rand.NewSource(int64(1000 + idx)))
			state := newClientState()

			switch {
			case idx < 10:
				// Group A: connected from the very beginning (afterOffset=0) through completion.
				sub, err := b.Subscribe(ctx, sessionID, 0, "")
				if err != nil {
					t.Errorf("sub %d Subscribe: %v", idx, err)
					earlyReady.Done()
					return
				}
				defer sub.Cancel()
				earlyReady.Done()

				for state.lastOffset < 4 {
					select {
					case ev, ok := <-sub.Events:
						if !ok {
							t.Errorf("sub %d closed early (err=%v, lastOffset=%d)", idx, sub.Err(), state.lastOffset)
							return
						}
						state.apply(t, ev)
					case <-ctx.Done():
						t.Errorf("sub %d timed out at offset %d", idx, state.lastOffset)
						return
					}
				}

			case idx < 30:
				// Group B: join at random staggered times mid-stream with afterOffset=0 (full page load mid-stream).
				time.Sleep(time.Duration(rng.Intn(45)+1) * time.Millisecond)
				sub, err := b.Subscribe(ctx, sessionID, 0, "")
				if err != nil {
					t.Errorf("sub %d Subscribe: %v", idx, err)
					return
				}
				defer sub.Cancel()

				for state.lastOffset < 4 {
					select {
					case ev, ok := <-sub.Events:
						if !ok {
							t.Errorf("sub %d closed early (err=%v, lastOffset=%d)", idx, sub.Err(), state.lastOffset)
							return
						}
						state.apply(t, ev)
					case <-ctx.Done():
						t.Errorf("sub %d timed out at offset %d", idx, state.lastOffset)
						return
					}
				}

			default:
				// Group C: connect, drop mid-stream 2 times (simulating F5 / Wi-Fi drop), and reconnect
				// from Last-Event-ID (state.lastOffset + state.epoch)!
				time.Sleep(time.Duration(rng.Intn(15)) * time.Millisecond)
				dropsRemaining := 2
				eventsUntilDrop := rng.Intn(40) + 10

				for state.lastOffset < 4 {
					sub, err := b.Subscribe(ctx, sessionID, state.lastOffset, state.epoch)
					if err != nil {
						t.Errorf("sub %d reconnect Subscribe: %v", idx, err)
						return
					}

					seenThisConn := 0
					reconnecting := false
					for state.lastOffset < 4 && !reconnecting {
						select {
						case ev, ok := <-sub.Events:
							if !ok {
								t.Errorf("sub %d closed unexpectedly (err=%v)", idx, sub.Err())
								sub.Cancel()
								return
							}
							state.apply(t, ev)
							seenThisConn++
							if dropsRemaining > 0 && seenThisConn >= eventsUntilDrop && state.lastOffset < 4 {
								dropsRemaining--
								eventsUntilDrop = rng.Intn(50) + 15
								sub.Cancel()
								state.resetUncommittedForReconnect()
								time.Sleep(time.Duration(rng.Intn(5)+1) * time.Millisecond)
								reconnecting = true
							}
						case <-ctx.Done():
							sub.Cancel()
							t.Errorf("sub %d timed out at offset %d", idx, state.lastOffset)
							return
						}
					}
					sub.Cancel()
				}
			}

			// Verify exact reconstruction of all 4 records with zero missing or duplicate characters!
			for off := uint64(1); off <= 4; off++ {
				got, ok := state.records[off]
				if !ok {
					t.Errorf("sub %d missing record at offset %d", idx, off)
					continue
				}
				if got != wantOffsets[off] {
					t.Errorf("sub %d mismatch at offset %d: len(got)=%d len(want)=%d", idx, off, len(got), len(wantOffsets[off]))
				}
			}
		}(subIdx)
	}

	earlyReady.Wait()

	// Turn 1: Atomic user prompt (Offset 1)
	if _, err := b.Publish(ctx, sessionID, model.PublishRequest{
		Type:  "user",
		Delta: wantOffsets[1],
	}); err != nil {
		t.Fatalf("publish offset 1: %v", err)
	}

	// Turn 2: Stream 1 (150 chunks -> Offset 2)
	for i := 1; i <= chunksPerStream; i++ {
		if _, err := b.Publish(ctx, sessionID, model.PublishRequest{
			StreamID: "stream-1",
			Seq:      uint64(i),
			Type:     "assistant",
			Delta:    fmt.Sprintf("A%03d-", i),
			Fin:      i == chunksPerStream,
		}); err != nil {
			t.Fatalf("publish stream-1 seq %d: %v", i, err)
		}
		if i%5 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Turn 3: Atomic user prompt 2 (Offset 3)
	if _, err := b.Publish(ctx, sessionID, model.PublishRequest{
		Type:  "user",
		Delta: wantOffsets[3],
	}); err != nil {
		t.Fatalf("publish offset 3: %v", err)
	}

	// Turn 4: Stream 2 (150 chunks -> Offset 4)
	for i := 1; i <= chunksPerStream; i++ {
		typ := "assistant"
		if i == chunksPerStream {
			typ = "run_finished"
		}
		if _, err := b.Publish(ctx, sessionID, model.PublishRequest{
			StreamID: "stream-2",
			Seq:      uint64(i),
			Type:     typ,
			Delta:    fmt.Sprintf("B%03d-", i),
			Fin:      i == chunksPerStream,
		}); err != nil {
			t.Fatalf("publish stream-2 seq %d: %v", i, err)
		}
		if i%5 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	allSubsDone.Wait()

	// Also verify storage compacted everything into exactly 4 records!
	recs, _, err := store.ReadFrom(ctx, sessionID, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(recs) != 4 {
		t.Fatalf("expected 4 compacted records in storage, got %d", len(recs))
	}
}

func TestIdempotentSeqRetryAndValidationErrors(t *testing.T) {
	ctx := context.Background()
	b := New(storage.NewMemoryStorage(), Config{})
	closeBroker(t, b)

	// Empty sessionID rejected.
	if _, err := b.Publish(ctx, "", model.PublishRequest{Delta: "hi"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on empty sessionID, got %v", err)
	}
	if _, err := b.Subscribe(ctx, "", 0, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on Subscribe empty sessionID, got %v", err)
	}

	// Empty atomic event rejected.
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{Type: "msg"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on empty atomic event, got %v", err)
	}

	// Invalid JSON payload rejected.
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{Payload: json.RawMessage(`{broken`)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on invalid JSON, got %v", err)
	}

	// Starting a stream at Seq > 1 fails with ErrSequenceGap.
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{StreamID: "st-1", Seq: 2, Delta: "x"}); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("expected ErrSequenceGap starting at Seq=2, got %v", err)
	}

	// Attach subscriber to confirm duplicate retries are NOT re-broadcast.
	sub, err := b.Subscribe(ctx, "s1", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Seq 1 & Seq 2
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{StreamID: "st-1", Seq: 1, Type: "assistant", Delta: "Hel"}); err != nil {
		t.Fatalf("Seq 1: %v", err)
	}
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{StreamID: "st-1", Seq: 2, Type: "assistant", Delta: "lo"}); err != nil {
		t.Fatalf("Seq 2: %v", err)
	}

	// Duplicate retry of Seq 1 and Seq 2 while in-flight -> Deduplicated: true!
	for _, dupSeq := range []uint64{1, 2} {
		res, err := b.Publish(ctx, "s1", model.PublishRequest{StreamID: "st-1", Seq: dupSeq, Delta: "DUPLICATE"})
		if err != nil {
			t.Fatalf("duplicate seq %d failed: %v", dupSeq, err)
		}
		if !res.Deduplicated || res.InFlightSeq != 2 {
			t.Fatalf("expected Deduplicated=true InFlightSeq=2, got %+v", res)
		}
	}

	// Sequence gap (Seq 4 when lastSeq is 2) -> ErrSequenceGap!
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{StreamID: "st-1", Seq: 4, Delta: "!"}); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("expected ErrSequenceGap on Seq=4, got %v", err)
	}

	// Concurrent stream with different StreamID -> ErrStreamConflict!
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{StreamID: "st-OTHER", Seq: 1, Delta: "hi"}); !errors.Is(err, ErrStreamConflict) {
		t.Fatalf("expected ErrStreamConflict on st-OTHER, got %v", err)
	}

	// Invalid JSON payload on Fin must NOT corrupt in-flight state!
	if _, err := b.Publish(ctx, "s1", model.PublishRequest{
		StreamID: "st-1",
		Seq:      3,
		Delta:    "!",
		Payload:  json.RawMessage(`{invalid`),
		Fin:      true,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on invalid Fin JSON, got %v", err)
	}

	// Auto-seq (Seq: 0 -> becomes Seq 3) with Fin: true!
	finRes, err := b.Publish(ctx, "s1", model.PublishRequest{
		StreamID: "st-1",
		Seq:      0,
		Delta:    "!",
		Fin:      true,
	})
	if err != nil {
		t.Fatalf("Fin publish: %v", err)
	}
	if finRes.CommittedOffset != 1 || finRes.InFlightSeq != 3 || finRes.Deduplicated {
		t.Fatalf("unexpected Fin result: %+v", finRes)
	}

	// Late duplicate retry of final Fin chunk after compaction -> Deduplicated: true!
	lateDup, err := b.Publish(ctx, "s1", model.PublishRequest{
		StreamID: "st-1",
		Seq:      3,
		Delta:    "!",
		Fin:      true,
	})
	if err != nil {
		t.Fatalf("late Fin retry failed: %v", err)
	}
	if !lateDup.Deduplicated || lateDup.CommittedOffset != 1 || lateDup.InFlightSeq != 3 {
		t.Fatalf("expected Deduplicated=true CommittedOffset=1 on late Fin retry, got %+v", lateDup)
	}

	// Verify subscriber saw exactly 3 deltas + 1 committed record = "Hello!"
	var gotEvents []model.OutEvent
	for len(gotEvents) < 4 {
		select {
		case ev := <-sub.Events:
			gotEvents = append(gotEvents, ev)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for events, got %d", len(gotEvents))
		}
	}
	if gotEvents[3].Kind != model.EventCommitted || decodeContent(t, gotEvents[3].Payload) != "Hello!" {
		t.Fatalf("expected compacted 'Hello!', got %+v (%s)", gotEvents[3], string(gotEvents[3].Payload))
	}
}

func TestEpochMismatchResetsAfterOffset(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStorage(storage.WithEpochSource(func() string { return "epoch-curr12" }))
	b := New(store, Config{})
	closeBroker(t, b)

	for i := 1; i <= 3; i++ {
		if _, err := b.Publish(ctx, "sess-ep", model.PublishRequest{
			Type:  "msg",
			Delta: fmt.Sprintf("m-%d", i),
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Matching epoch with afterOffset=2 -> receives ONLY offset 3.
	subMatch, err := b.Subscribe(ctx, "sess-ep", 2, "epoch-curr12")
	if err != nil {
		t.Fatalf("Subscribe matching epoch: %v", err)
	}
	defer subMatch.Cancel()

	ev := <-subMatch.Events
	if ev.Kind != model.EventCommitted || ev.Offset != 3 {
		t.Fatalf("expected offset 3 on matching epoch, got %+v", ev)
	}

	// Stale epoch with afterOffset=3 -> resets afterOffset to 0 and replays offsets 1, 2, 3!
	subStale, err := b.Subscribe(ctx, "sess-ep", 3, "epoch-old000")
	if err != nil {
		t.Fatalf("Subscribe stale epoch: %v", err)
	}
	defer subStale.Cancel()

	for wantOff := uint64(1); wantOff <= 3; wantOff++ {
		select {
		case got := <-subStale.Events:
			if got.Kind != model.EventCommitted || got.Offset != wantOff {
				t.Fatalf("stale epoch replay expected offset %d, got %+v", wantOff, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for offset %d on stale epoch replay", wantOff)
		}
	}
}

func TestSlowSubscriberEviction(t *testing.T) {
	ctx := context.Background()
	b := New(storage.NewMemoryStorage(), Config{
		SubscriberBufferSize: 4,
	})
	closeBroker(t, b)

	// Slow subscriber: never reads from Events while producer bursts.
	slowSub, err := b.Subscribe(ctx, "sess-slow", 0, "")
	if err != nil {
		t.Fatalf("slow Subscribe: %v", err)
	}
	defer slowSub.Cancel()

	// Fast subscriber: drains Events continuously.
	fastSub, err := b.Subscribe(ctx, "sess-slow", 0, "")
	if err != nil {
		t.Fatalf("fast Subscribe: %v", err)
	}
	defer fastSub.Cancel()

	var fastText strings.Builder
	fastDone := make(chan uint64, 1)
	go func() {
		for ev := range fastSub.Events {
			if ev.Kind == model.EventDelta {
				_, _ = fastText.WriteString(ev.Delta)
			} else if ev.Kind == model.EventCommitted {
				fastDone <- ev.Offset
				return
			}
		}
	}()

	const burst = 30
	var want strings.Builder
	for i := 1; i <= burst; i++ {
		tok := fmt.Sprintf("c%d.", i)
		_, _ = want.WriteString(tok)
		if _, err := b.Publish(ctx, "sess-slow", model.PublishRequest{
			StreamID: "burst-1",
			Seq:      uint64(i),
			Type:     "assistant",
			Delta:    tok,
			Fin:      i == burst,
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		time.Sleep(time.Millisecond)
	}

	// Slow subscriber channel must close promptly with ErrSlowSubscriber!
	slowClosed := make(chan struct{})
	go func() {
		for range slowSub.Events {
		}
		close(slowClosed)
	}()

	select {
	case <-slowClosed:
	case <-time.After(2 * time.Second):
		t.Fatalf("slow subscriber Events channel did not close")
	}

	if !errors.Is(slowSub.Err(), ErrSlowSubscriber) {
		t.Fatalf("expected ErrSlowSubscriber, got %v", slowSub.Err())
	}

	// Fast subscriber must have received everything cleanly without blocking!
	select {
	case off := <-fastDone:
		if off != 1 || fastText.String() != want.String() {
			t.Fatalf("fast subscriber mismatch: off=%d got=%q want=%q", off, fastText.String(), want.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("fast subscriber timed out")
	}
}

func TestIdleSessionEvictionAndTransparentReload(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStorage()
	b := New(store, Config{
		IdleSessionTTL: 25 * time.Millisecond,
		SweepInterval:  8 * time.Millisecond,
	})
	closeBroker(t, b)

	// Create session A (will go idle) and session B (kept alive by an in-flight stream).
	if _, err := b.Publish(ctx, "idle-sess", model.PublishRequest{Type: "user", Delta: "first"}); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	if _, err := b.Publish(ctx, "idle-sess", model.PublishRequest{Type: "assistant", Delta: "second"}); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if _, err := b.Publish(ctx, "busy-sess", model.PublishRequest{
		StreamID: "active-stream",
		Seq:      1,
		Type:     "assistant",
		Delta:    "still typing...",
		Fin:      false,
	}); err != nil {
		t.Fatalf("Publish busy-sess: %v", err)
	}

	if got := b.activeActorCount(); got != 2 {
		t.Fatalf("expected 2 active actors, got %d", got)
	}

	// Wait for sweeper to evict ONLY "idle-sess", keeping "busy-sess" in memory.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.activeActorCount() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.activeActorCount(); got != 1 {
		t.Fatalf("expected idle-sess to be evicted leaving 1 active actor, got %d", got)
	}

	// ListSessions must still include BOTH evicted "idle-sess" and active "busy-sess"!
	sessions, err := b.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions in ListSessions after eviction, got %d", len(sessions))
	}

	// Inspect on evicted "idle-sess" transparently reloads it from Storage!
	snap, ok, err := b.Inspect(ctx, "idle-sess")
	if err != nil || !ok {
		t.Fatalf("Inspect after eviction failed: ok=%v err=%v", ok, err)
	}
	if snap.Meta.LastOffset != 2 || snap.Meta.State != model.StateIdle {
		t.Fatalf("unexpected reloaded meta: %+v", snap.Meta)
	}

	// Publishing to reloaded "idle-sess" continues monotonically at Offset 3!
	res, err := b.Publish(ctx, "idle-sess", model.PublishRequest{Type: "user", Delta: "third"})
	if err != nil {
		t.Fatalf("Publish after reload: %v", err)
	}
	if res.CommittedOffset != 3 {
		t.Fatalf("expected CommittedOffset=3 after reload, got %d", res.CommittedOffset)
	}

	// Subscribing from offset 0 replays all 3 records seamlessly!
	sub, err := b.Subscribe(ctx, "idle-sess", 0, "")
	if err != nil {
		t.Fatalf("Subscribe after reload: %v", err)
	}
	defer sub.Cancel()

	var got []string
	for len(got) < 3 {
		ev := <-sub.Events
		got = append(got, decodeContent(t, ev.Payload))
	}
	if strings.Join(got, ",") != "first,second,third" {
		t.Fatalf("unexpected replayed records: %v", got)
	}
}

func TestCreateSessionInspectAndNoGoroutineLeaksOnClose(t *testing.T) {
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()

	ctx := context.Background()
	b := New(storage.NewMemoryStorage(), Config{
		SubscriberBufferSize: 8,
		SweepInterval:        20 * time.Millisecond,
	})

	// Auto-generated session ID on empty string, plus explicit session ID.
	autoMeta, err := b.CreateSession(ctx, "")
	if err != nil || autoMeta.SessionID == "" {
		t.Fatalf("CreateSession auto ID failed: meta=%+v err=%v", autoMeta, err)
	}
	explicitMeta, err := b.CreateSession(ctx, "explicit-1")
	if err != nil || explicitMeta.SessionID != "explicit-1" {
		t.Fatalf("CreateSession explicit failed: meta=%+v err=%v", explicitMeta, err)
	}

	// Unknown session Inspect returns ok == false.
	if _, ok, err := b.Inspect(ctx, "does-not-exist"); err != nil || ok {
		t.Fatalf("expected ok=false for unknown session, got ok=%v err=%v", ok, err)
	}

	// Open 20 subscribers with mixed termination modes.
	var subs []*Subscription
	for i := 0; i < 20; i++ {
		subCtx := ctx
		var subCancel context.CancelFunc
		if i%3 == 0 {
			subCtx, subCancel = context.WithCancel(ctx)
		}
		s, err := b.Subscribe(subCtx, "explicit-1", 0, "")
		if err != nil {
			if subCancel != nil {
				subCancel()
			}
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		if subCancel != nil {
			subCancel() // Abrupt context disconnect
		} else if i%3 == 1 {
			s.Cancel() // Explicit Cancel()
		} else {
			subs = append(subs, s) // Left open until b.Close()
		}
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Idempotent second Close().
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Open subscribers must close and report ErrBrokerClosed.
	for _, s := range subs {
		for range s.Events {
		}
		if !errors.Is(s.Err(), ErrBrokerClosed) {
			t.Fatalf("expected ErrBrokerClosed on closed subscriber, got %v", s.Err())
		}
	}

	// Operations after Close must return ErrBrokerClosed.
	if _, err := b.Publish(ctx, "explicit-1", model.PublishRequest{Delta: "x"}); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("expected ErrBrokerClosed on Publish after Close, got %v", err)
	}
	if _, err := b.Subscribe(ctx, "explicit-1", 0, ""); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("expected ErrBrokerClosed on Subscribe after Close, got %v", err)
	}

	runtime.GC()
	time.Sleep(15 * time.Millisecond)
	afterGoroutines := runtime.NumGoroutine()
	if afterGoroutines > baseGoroutines+2 {
		t.Fatalf("potential goroutine leak: before=%d after=%d", baseGoroutines, afterGoroutines)
	}
}

var errInjectedStorage = errors.New("injected storage failure")

type flakyStore struct {
	*storage.MemoryStorage
	failNextAppend     atomic.Bool
	failNextSaveMeta   atomic.Bool
	alwaysFailSaveMeta atomic.Bool

	slowSessionID string
	enteredAppend chan struct{}
	releaseAppend chan struct{}
}

func (f *flakyStore) Append(ctx context.Context, id string, r model.Record) error {
	if f.slowSessionID != "" && id == f.slowSessionID {
		select {
		case f.enteredAppend <- struct{}{}:
		default:
		}
		<-f.releaseAppend
	}
	if f.failNextAppend.CompareAndSwap(true, false) {
		return errInjectedStorage
	}
	return f.MemoryStorage.Append(ctx, id, r)
}

func (f *flakyStore) SaveMeta(ctx context.Context, m model.SessionMeta) error {
	if f.alwaysFailSaveMeta.Load() {
		return errInjectedStorage
	}
	if f.failNextSaveMeta.CompareAndSwap(true, false) {
		return errInjectedStorage
	}
	return f.MemoryStorage.SaveMeta(ctx, m)
}

func TestFinAppendFailureThenRetryCommits(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{MemoryStorage: storage.NewMemoryStorage()}
	b := New(st, Config{})
	closeBroker(t, b)

	// Case 1: Explicit same-Seq retry after Fin Append failure.
	if _, err := b.Publish(ctx, "s", model.PublishRequest{
		StreamID: "m1",
		Seq:      1,
		Type:     "assistant",
		Delta:    "Hel",
	}); err != nil {
		t.Fatalf("Seq 1: %v", err)
	}

	st.failNextAppend.Store(true)
	if _, err := b.Publish(ctx, "s", model.PublishRequest{
		StreamID: "m1",
		Seq:      2,
		Type:     "assistant",
		Delta:    "lo",
		Fin:      true,
	}); !errors.Is(err, errInjectedStorage) {
		t.Fatalf("expected errInjectedStorage on Fin Append, got %v", err)
	}

	// Verify inFlight was NOT mutated by the failed Fin attempt.
	snap, ok, err := b.Inspect(ctx, "s")
	if err != nil || !ok || snap.InFlight == nil {
		t.Fatalf("expected InFlight still active after failed Fin, got snap=%+v ok=%v err=%v", snap, ok, err)
	}
	if snap.InFlight.LastSeq != 1 || snap.InFlight.Merged != "Hel" {
		t.Fatalf("expected InFlight untouched (LastSeq=1, Merged='Hel'), got %+v", snap.InFlight)
	}

	// Retry with same Seq=2 must commit Offset=1 with "Hello" (NOT Deduplicated: true!).
	res, err := b.Publish(ctx, "s", model.PublishRequest{
		StreamID: "m1",
		Seq:      2,
		Type:     "assistant",
		Delta:    "lo",
		Fin:      true,
	})
	if err != nil || res.CommittedOffset != 1 || res.Deduplicated {
		t.Fatalf("fin retry must commit Offset=1, got res=%+v err=%v", res, err)
	}

	// Case 2: Seq=0 retry after Fin Append failure must NOT duplicate delta ("Hello", not "Hellolo")!
	if _, err := b.Publish(ctx, "s", model.PublishRequest{
		StreamID: "m2",
		Seq:      1,
		Type:     "assistant",
		Delta:    "Hel",
	}); err != nil {
		t.Fatalf("m2 Seq 1: %v", err)
	}
	st.failNextAppend.Store(true)
	if _, err := b.Publish(ctx, "s", model.PublishRequest{
		StreamID: "m2",
		Seq:      0,
		Type:     "assistant",
		Delta:    "lo",
		Fin:      true,
	}); !errors.Is(err, errInjectedStorage) {
		t.Fatalf("expected errInjectedStorage on m2 Fin, got %v", err)
	}
	res2, err := b.Publish(ctx, "s", model.PublishRequest{
		StreamID: "m2",
		Seq:      0,
		Type:     "assistant",
		Delta:    "lo",
		Fin:      true,
	})
	if err != nil || res2.CommittedOffset != 2 || res2.InFlightSeq != 2 {
		t.Fatalf("m2 Seq=0 retry must commit Offset=2 Seq=2, got res=%+v err=%v", res2, err)
	}

	recs, _, err := st.ReadFrom(ctx, "s", 0)
	if err != nil || len(recs) != 2 {
		t.Fatalf("expected 2 committed records, got len=%d err=%v", len(recs), err)
	}
	if got1 := decodeContent(t, recs[0].Payload); got1 != "Hello" {
		t.Fatalf("record 1 content=%q, want 'Hello'", got1)
	}
	if got2 := decodeContent(t, recs[1].Payload); got2 != "Hello" {
		t.Fatalf("record 2 content=%q, want 'Hello'", got2)
	}
}

func TestFirstChunkFailureLeavesNoOrphanInFlight(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{MemoryStorage: storage.NewMemoryStorage()}
	b := New(st, Config{})
	closeBroker(t, b)

	// 1. Single-chunk stream (Seq=1, Fin=true) failing Append must NOT leave an empty inFlight placeholder!
	st.failNextAppend.Store(true)
	if _, err := b.Publish(ctx, "s-first", model.PublishRequest{
		StreamID: "orphan-candidate",
		Seq:      1,
		Type:     "assistant",
		Delta:    "one-shot",
		Fin:      true,
	}); !errors.Is(err, errInjectedStorage) {
		t.Fatalf("expected errInjectedStorage, got %v", err)
	}

	snap, ok, err := b.Inspect(ctx, "s-first")
	if err != nil || !ok {
		t.Fatalf("Inspect failed: ok=%v err=%v", ok, err)
	}
	if snap.InFlight != nil {
		t.Fatalf("expected nil InFlight after failed first chunk, got %+v", snap.InFlight)
	}

	// 2. Subsequent stream with a brand-new StreamID must succeed without ErrStreamConflict,
	// even if SaveMeta fails on StateRunning transition!
	sub, err := b.Subscribe(ctx, "s-first", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	st.failNextSaveMeta.Store(true)
	if _, err := b.Publish(ctx, "s-first", model.PublishRequest{
		StreamID: "clean-stream",
		Seq:      1,
		Type:     "assistant",
		Delta:    "works",
		Fin:      false,
	}); err != nil {
		t.Fatalf("expected non-Fin first chunk to succeed despite SaveMeta failure, got %v", err)
	}

	select {
	case ev := <-sub.Events:
		if ev.Kind != model.EventDelta || ev.Delta != "works" {
			t.Fatalf("expected live EventDelta 'works', got %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for live EventDelta")
	}
}

func TestSaveMetaFailureAfterAppendPreservesCommitAndFanOut(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{MemoryStorage: storage.NewMemoryStorage()}
	b := New(st, Config{
		IdleSessionTTL: 20 * time.Millisecond,
		SweepInterval:  6 * time.Millisecond,
	})
	closeBroker(t, b)

	sub, err := b.Subscribe(ctx, "s-meta", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Subcase A: Atomic event where SaveMeta fails after Append succeeded.
	st.failNextSaveMeta.Store(true)
	atomicRes, err := b.Publish(ctx, "s-meta", model.PublishRequest{
		Type:  "user",
		Delta: "hello atomic",
	})
	if err != nil || atomicRes.CommittedOffset != 1 {
		t.Fatalf("atomic Publish must succeed after Append despite SaveMeta error: res=%+v err=%v", atomicRes, err)
	}

	select {
	case ev := <-sub.Events:
		if ev.Kind != model.EventCommitted || ev.Offset != 1 || decodeContent(t, ev.Payload) != "hello atomic" {
			t.Fatalf("expected live EventCommitted offset=1, got %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for atomic EventCommitted")
	}

	// Subcase B: Streaming Fin where SaveMeta fails permanently (even across sweeper flush) ->
	// live subscriber still receives delta + record, retry is deduplicated, and cold reload recovers meta from ReadFrom!
	if _, err := b.Publish(ctx, "s-meta", model.PublishRequest{
		StreamID: "m-fin",
		Seq:      1,
		Type:     "assistant",
		Delta:    "Hel",
	}); err != nil {
		t.Fatalf("Seq 1: %v", err)
	}

	st.alwaysFailSaveMeta.Store(true)
	finRes, err := b.Publish(ctx, "s-meta", model.PublishRequest{
		StreamID: "m-fin",
		Seq:      2,
		Type:     "run_finished",
		Delta:    "lo",
		Fin:      true,
	})
	if err != nil || finRes.CommittedOffset != 2 {
		t.Fatalf("Fin Publish must succeed after Append despite SaveMeta error: res=%+v err=%v", finRes, err)
	}

	// Late retry of Seq=2 returns Deduplicated: true without creating a 3rd record.
	dupRes, err := b.Publish(ctx, "s-meta", model.PublishRequest{
		StreamID: "m-fin",
		Seq:      2,
		Type:     "run_finished",
		Delta:    "lo",
		Fin:      true,
	})
	if err != nil || !dupRes.Deduplicated || dupRes.CommittedOffset != 2 {
		t.Fatalf("expected deduplicated retry at Offset=2, got res=%+v err=%v", dupRes, err)
	}

	// Live subscriber must receive Seq 1 delta, Seq 2 Fin delta, AND Offset 2 committed record!
	var sawFinDelta, sawRecord2 bool
	for !sawRecord2 {
		select {
		case ev := <-sub.Events:
			if ev.Kind == model.EventDelta && ev.Seq == 2 && ev.Fin {
				sawFinDelta = true
			}
			if ev.Kind == model.EventCommitted && ev.Offset == 2 {
				if decodeContent(t, ev.Payload) != "Hello" {
					t.Fatalf("unexpected Offset 2 payload: %s", string(ev.Payload))
				}
				sawRecord2 = true
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for Fin events (sawFinDelta=%v sawRecord2=%v)", sawFinDelta, sawRecord2)
		}
	}
	if !sawFinDelta {
		t.Fatalf("expected live subscriber to receive Fin delta before committed record")
	}

	// Disconnect subscriber so "s-meta" becomes idle and gets evicted from memory.
	sub.Cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.activeActorCount() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.activeActorCount(); got != 0 {
		t.Fatalf("expected actor eviction even when SaveMeta fails, active=%d", got)
	}

	st.alwaysFailSaveMeta.Store(false)

	// ListSessions and Inspect must recover accurate LastOffset=2 and terminal State=completed from stored records!
	list, err := b.ListSessions(ctx)
	if err != nil || len(list) != 1 || list[0].Meta.LastOffset != 2 || list[0].Meta.State != model.StateCompleted {
		t.Fatalf("expected ListSessions LastOffset=2 State=completed after eviction, got list=%+v err=%v", list, err)
	}

	snap, ok, err := b.Inspect(ctx, "s-meta")
	if err != nil || !ok || snap.Meta.LastOffset != 2 || snap.Meta.State != model.StateCompleted {
		t.Fatalf("expected Inspect LastOffset=2 State=completed after cold reload, got snap=%+v ok=%v err=%v", snap, ok, err)
	}
}

func TestSlowStorageOnSessionADoesNotBlockOtherSessions(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{
		MemoryStorage: storage.NewMemoryStorage(),
		slowSessionID: "slow-A",
		enteredAppend: make(chan struct{}, 1),
		releaseAppend: make(chan struct{}),
	}
	b := New(st, Config{
		IdleSessionTTL: 15 * time.Millisecond,
		SweepInterval:  5 * time.Millisecond,
	})
	closeBroker(t, b)

	// Create an idle session C first so we can verify sweeper evicts C while A is blocked!
	if _, err := b.Publish(ctx, "idle-C", model.PublishRequest{Type: "user", Delta: "c"}); err != nil {
		t.Fatalf("Publish idle-C: %v", err)
	}

	// Start slow Publish on session A (blocks inside store.Append while holding actorA.mu).
	slowDone := make(chan error, 1)
	go func() {
		_, err := b.Publish(ctx, "slow-A", model.PublishRequest{Type: "user", Delta: "slow write"})
		slowDone <- err
	}()

	select {
	case <-st.enteredAppend:
	case <-time.After(2 * time.Second):
		t.Fatalf("slow-A did not enter Append")
	}

	// Also queue a concurrent Inspect("slow-A") and ListSessions(ctx) — both wait on actorA.mu,
	// proving neither acquireActor nor ListSessions holds global b.mu while waiting on actorA.mu!
	inspectADone := make(chan struct{})
	go func() {
		_, _, _ = b.Inspect(ctx, "slow-A")
		close(inspectADone)
	}()
	listDone := make(chan error, 1)
	go func() {
		_, err := b.ListSessions(ctx)
		listDone <- err
	}()
	time.Sleep(10 * time.Millisecond)

	// Session B Publish + Subscribe + Inspect must complete rapidly without blocking on Session A!
	fastFinished := make(chan error, 1)
	go func() {
		if _, err := b.Publish(ctx, "fast-B", model.PublishRequest{Type: "user", Delta: "fast write"}); err != nil {
			fastFinished <- err
			return
		}
		sub, err := b.Subscribe(ctx, "fast-B", 0, "")
		if err != nil {
			fastFinished <- err
			return
		}
		ev := <-sub.Events
		sub.Cancel()
		if ev.Offset != 1 {
			fastFinished <- fmt.Errorf("unexpected fast-B offset %d", ev.Offset)
			return
		}
		if _, ok, err := b.Inspect(ctx, "fast-B"); err != nil || !ok {
			fastFinished <- fmt.Errorf("Inspect fast-B ok=%v err=%v", ok, err)
			return
		}
		fastFinished <- nil
	}()

	select {
	case err := <-fastFinished:
		if err != nil {
			close(st.releaseAppend)
			t.Fatalf("fast-B failed while slow-A blocked: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(st.releaseAppend)
		t.Fatalf("fast-B was blocked by slow-A storage I/O!")
	}

	evictedWhileABlocked := false
	evictDeadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(evictDeadline) {
		if b.activeActorCount() == 1 { // only slow-A remains!
			evictedWhileABlocked = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !evictedWhileABlocked {
		close(st.releaseAppend)
		t.Fatalf("expected sweeper to evict idle-C and fast-B while slow-A was blocked; active=%d", b.activeActorCount())
	}

	// Release slow-A and verify clean completion of slow-A, Inspect("slow-A"), and ListSessions.
	close(st.releaseAppend)

	if err := <-slowDone; err != nil {
		t.Fatalf("slow-A Publish failed: %v", err)
	}
	<-inspectADone
	if err := <-listDone; err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
}
