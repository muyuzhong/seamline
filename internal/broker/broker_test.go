package broker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

func TestPublishAssignsSeqAndFansOutOpaquePayload(t *testing.T) {
	ctx := context.Background()
	store := hotwindow.NewMemoryStorage(hotwindow.WithEpochSource(fixedEpochs("ep-1")))
	b := New(store, Config{})
	closeBroker(t, b)
	if len(b.InstanceID()) != 12 {
		t.Fatalf("expected a 12-char random instance id, got %q", b.InstanceID())
	}

	sub, err := b.Subscribe(ctx, "opaque", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	payloads := []string{
		`{"b": [1, {"c": "x"}],   "a": 1}`,
		`"half a JSON argument: {\"query\": \"sea"`,
		"",
	}
	for i, p := range payloads {
		res := mustPublish(t, b, "opaque", ev(fmt.Sprintf("type-%d", i), p))
		if res.Epoch != "ep-1" || res.Seq != uint64(i+1) || res.Deduplicated {
			t.Fatalf("publish %d: %+v", i, res)
		}
	}
	for i, p := range payloads {
		got := recvEvent(t, sub)
		if got.Seq != uint64(i+1) || got.Epoch != "ep-1" || got.Type != fmt.Sprintf("type-%d", i) || string(got.Payload) != p {
			t.Fatalf("event %d delivered as %+v (payload %q), want payload bytes %q unchanged", i, got, got.Payload, p)
		}
	}
	stored, _, err := store.ReadAfter(ctx, "opaque", 0)
	if err != nil || len(stored) != 3 || string(stored[0].Payload) != payloads[0] {
		t.Fatalf("stored events: %+v err=%v", stored, err)
	}
}

func TestPublishValidation(t *testing.T) {
	ctx := context.Background()
	b := New(hotwindow.NewMemoryStorage(), Config{MaxEventBytes: 64})
	closeBroker(t, b)

	cases := []struct {
		name string
		sid  string
		req  model.PublishRequest
		want error
	}{
		{"empty session", "", ev("x", ""), ErrInvalidInput},
		{"empty type", "s", ev("", `{}`), ErrInvalidInput},
		{"invalid JSON", "s", ev("x", `{broken`), ErrInvalidInput},
		{"too large", "s", ev("x", `"`+strings.Repeat("x", 63)+`"`), ErrEventTooLarge},
	}
	for _, tc := range cases {
		if _, err := b.Publish(ctx, tc.sid, tc.req); !errors.Is(err, tc.want) {
			t.Fatalf("%s: want %v, got %v", tc.name, tc.want, err)
		}
	}
	if _, err := b.Subscribe(ctx, "", 0, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Subscribe with empty session: want ErrInvalidInput, got %v", err)
	}
	if res := mustPublish(t, b, "s", ev("x", `"`+strings.Repeat("x", 62)+`"`)); res.Seq != 1 {
		t.Fatalf("a payload of exactly MaxEventBytes must be accepted: %+v", res)
	}
}

func TestAppendFailureLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{MemoryStorage: hotwindow.NewMemoryStorage()}
	b := New(st, Config{})
	closeBroker(t, b)

	sub, err := b.Subscribe(ctx, "s", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	mustPublish(t, b, "s", ev(model.TypeRunStarted, ""))
	st.failNextAppend.Store(true)
	if _, err := b.Publish(ctx, "s", ev(model.TypeRunFinished, "")); !errors.Is(err, errInjectedStorage) {
		t.Fatalf("want injected failure, got %v", err)
	}
	// Nothing about the failed event may survive: no seq consumed, run still active.
	if meta := mustInspect(t, b, "s"); meta.LatestSeq != 1 || meta.State != model.StateRunning || meta.ActiveRunStartSeq != 1 {
		t.Fatalf("failed append changed the session: %+v", meta)
	}
	if res := mustPublish(t, b, "s", ev(model.TypeRunFinished, "")); res.Seq != 2 {
		t.Fatalf("retry must take the seq the failed append did not consume: %+v", res)
	}
	for _, want := range []uint64{1, 2} {
		if got := recvEvent(t, sub); got.Seq != want {
			t.Fatalf("subscriber got seq %d, want %d", got.Seq, want)
		}
	}
	if meta := mustInspect(t, b, "s"); meta.State != model.StateCompleted || meta.ActiveRunStartSeq != 0 {
		t.Fatalf("retried run_finished must end the run: %+v", meta)
	}
}

// subscriberClient tracks a subscriber's cursor and fails on any gap or duplicate.
type subscriberClient struct {
	epoch string
	seq   uint64
	texts map[uint64]string
}

func (c *subscriberClient) apply(t *testing.T, d model.Delivery) {
	t.Helper()
	if d.Kind != model.DeliveryEvent {
		t.Errorf("unexpected delivery %+v", d)
		return
	}
	if d.Event.Seq != c.seq+1 {
		t.Errorf("got seq %d after %d (gap or duplicate)", d.Event.Seq, c.seq)
		return
	}
	c.epoch, c.seq = d.Event.Epoch, d.Event.Seq
	c.texts[d.Event.Seq] = string(d.Event.Payload)
}

func Test50ConcurrentSubscribersZeroGapZeroDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b := New(hotwindow.NewMemoryStorage(), Config{SubscriberBufferSize: 1024})
	closeBroker(t, b)
	const sid = "concurrent-50"

	var want []string
	for r := 0; r < 2; r++ {
		want = append(want, `{"run":"`+fmt.Sprint(r)+`"}`)
		for i := 0; i < 150; i++ {
			want = append(want, textPayload(fmt.Sprintf("r%d-%03d ", r, i)))
		}
		want = append(want, `{"done":true}`)
	}
	total := uint64(len(want))

	var ready, done sync.WaitGroup
	for i := 0; i < 50; i++ {
		if i < 10 {
			ready.Add(1)
		}
		done.Add(1)
		go func(idx int) {
			defer done.Done()
			c := runConcurrentSubscriber(ctx, t, b, sid, idx, total, &ready)
			for seq := uint64(1); c != nil && seq <= total; seq++ {
				if c.texts[seq] != want[seq-1] {
					t.Errorf("sub %d seq %d payload %q, want %q", idx, seq, c.texts[seq], want[seq-1])
					return
				}
			}
		}(i)
	}
	ready.Wait()

	for i, p := range want {
		typ := "assistant_delta"
		switch {
		case strings.HasPrefix(p, `{"run"`):
			typ = model.TypeRunStarted
		case strings.HasPrefix(p, `{"done"`):
			typ = model.TypeRunFinished
		}
		mustPublish(t, b, sid, ev(typ, p))
		if i%5 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	done.Wait()
}

// runConcurrentSubscriber: subscribers 0-9 attach before publishing starts, 10-29 join fresh at a
// random time mid-stream, 30-49 drop twice and resume from their cursor.
func runConcurrentSubscriber(ctx context.Context, t *testing.T, b *Broker, sid string, idx int, total uint64, ready *sync.WaitGroup) *subscriberClient {
	rng := rand.New(rand.NewSource(int64(1000 + idx)))
	c := &subscriberClient{texts: make(map[uint64]string)}
	if idx >= 10 {
		time.Sleep(time.Duration(rng.Intn(40)) * time.Millisecond)
	}
	drops := 0
	if idx >= 30 {
		drops = 2
	}
	for c.seq < total {
		sub, err := b.Subscribe(ctx, sid, c.seq, c.epoch)
		if idx < 10 && c.seq == 0 {
			ready.Done()
		}
		if err != nil {
			t.Errorf("sub %d Subscribe: %v", idx, err)
			return nil
		}
		dropAfter := rng.Intn(60) + 10
		for n := 0; c.seq < total; n++ {
			d, ok := nextOrDone(ctx, sub)
			if !ok {
				t.Errorf("sub %d ended at seq %d (err=%v)", idx, c.seq, sub.Err())
				sub.Cancel()
				return nil
			}
			c.apply(t, d)
			if drops > 0 && n >= dropAfter {
				drops--
				break
			}
		}
		sub.Cancel()
	}
	return c
}

func nextOrDone(ctx context.Context, sub *Subscription) (model.Delivery, bool) {
	select {
	case d, ok := <-sub.Events:
		return d, ok
	case <-ctx.Done():
		return model.Delivery{}, false
	}
}

func TestSlowSubscriberEviction(t *testing.T) {
	ctx := context.Background()
	b := New(hotwindow.NewMemoryStorage(), Config{SubscriberBufferSize: 4})
	closeBroker(t, b)

	slow, err := b.Subscribe(ctx, "slow", 0, "")
	if err != nil {
		t.Fatalf("slow Subscribe: %v", err)
	}
	defer slow.Cancel()
	fast, err := b.Subscribe(ctx, "slow", 0, "")
	if err != nil {
		t.Fatalf("fast Subscribe: %v", err)
	}
	defer fast.Cancel()

	const burst = 30
	fastDone := make(chan uint64, 1)
	go func() {
		var last uint64
		for d := range fast.Events {
			if d.Event.Seq != last+1 {
				break
			}
			if last = d.Event.Seq; last == burst {
				break
			}
		}
		fastDone <- last
	}()

	for i := 1; i <= burst; i++ {
		mustPublish(t, b, "slow", ev("assistant_delta", textPayload(fmt.Sprint(i))))
		time.Sleep(time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		for range slow.Events {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("slow subscriber was not evicted")
	}
	if !errors.Is(slow.Err(), ErrSlowSubscriber) {
		t.Fatalf("want ErrSlowSubscriber, got %v", slow.Err())
	}
	select {
	case last := <-fastDone:
		if last != burst {
			t.Fatalf("fast subscriber stopped at seq %d, want %d in order", last, burst)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("fast subscriber was blocked by the slow one")
	}
}

func TestIdleSessionEvictionAndTransparentReload(t *testing.T) {
	ctx := context.Background()
	b := New(hotwindow.NewMemoryStorage(), Config{IdleSessionTTL: 25 * time.Millisecond, SweepInterval: 8 * time.Millisecond})
	closeBroker(t, b)

	mustPublish(t, b, "idle", ev("msg", textPayload("first")))
	mustPublish(t, b, "idle", ev("msg", textPayload("second")))
	busy, err := b.Subscribe(ctx, "busy", 0, "") // a subscriber keeps its actor
	if err != nil {
		t.Fatalf("Subscribe busy: %v", err)
	}
	defer busy.Cancel()

	waitFor(t, "idle actor eviction", func() bool { return b.activeActorCount() == 1 })

	sessions, err := b.ListSessions(ctx)
	if err != nil || len(sessions) != 2 {
		t.Fatalf("evicted sessions must still be listed: %+v err=%v", sessions, err)
	}
	if meta := mustInspect(t, b, "idle"); meta.LatestSeq != 2 || meta.State != model.StateIdle {
		t.Fatalf("Inspect after eviction: %+v", meta)
	}
	if res := mustPublish(t, b, "idle", ev("msg", textPayload("third"))); res.Seq != 3 {
		t.Fatalf("seq must continue after reload: %+v", res)
	}
	sub, err := b.Subscribe(ctx, "idle", 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()
	var got []string
	for len(got) < 3 {
		got = append(got, payloadText(t, recvEvent(t, sub)))
	}
	if strings.Join(got, ",") != "first,second,third" {
		t.Fatalf("replay after reload: %v", got)
	}
}

func TestCreateSessionInspectAndClose(t *testing.T) {
	ctx := context.Background()
	b := New(hotwindow.NewMemoryStorage(), Config{SubscriberBufferSize: 8, SweepInterval: 20 * time.Millisecond})

	auto, err := b.CreateSession(ctx, "")
	if err != nil || auto.SessionID == "" || auto.Epoch == "" || auto.State != model.StateIdle {
		t.Fatalf("CreateSession auto ID: %+v err=%v", auto, err)
	}
	if meta, err := b.CreateSession(ctx, "explicit-1"); err != nil || meta.SessionID != "explicit-1" || meta.EarliestSeq != 1 {
		t.Fatalf("CreateSession explicit: %+v err=%v", meta, err)
	}
	if _, ok, err := b.Inspect(ctx, "does-not-exist"); err != nil || ok {
		t.Fatalf("unknown session: ok=%v err=%v", ok, err)
	}

	var open []*Subscription
	for i := 0; i < 20; i++ {
		subCtx, subCancel := context.WithCancel(ctx)
		s, err := b.Subscribe(subCtx, "explicit-1", 0, "")
		if err != nil {
			subCancel()
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		switch i % 3 {
		case 0:
			subCancel()
		case 1:
			s.Cancel()
			subCancel()
		default:
			open = append(open, s)
			defer subCancel()
		}
	}
	if snap, _, _ := b.Inspect(ctx, "explicit-1"); snap.SubscriberCount == 0 {
		t.Fatalf("Inspect must report live subscribers")
	}

	// Close waits for the sweeper and every subscriber pump, so returning proves none leaked.
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close must return once the sweeper and all subscriber pumps exited")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for _, s := range open {
		for range s.Events {
		}
		if !errors.Is(s.Err(), ErrBrokerClosed) {
			t.Fatalf("want ErrBrokerClosed, got %v", s.Err())
		}
	}
	if _, err := b.Publish(ctx, "explicit-1", ev("x", "")); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("Publish after Close: %v", err)
	}
	if _, err := b.Subscribe(ctx, "explicit-1", 0, ""); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("Subscribe after Close: %v", err)
	}
}

func TestSlowStorageOnSessionADoesNotBlockOtherSessions(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{
		MemoryStorage: hotwindow.NewMemoryStorage(),
		slowSessionID: "slow-A",
		enteredAppend: make(chan struct{}, 1),
		releaseAppend: make(chan struct{}),
	}
	b := New(st, Config{IdleSessionTTL: 15 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)

	mustPublish(t, b, "idle-C", ev("msg", ""))
	slowDone := make(chan error, 1)
	go func() {
		_, err := b.Publish(ctx, "slow-A", ev("msg", ""))
		slowDone <- err
	}()
	select {
	case <-st.enteredAppend:
	case <-time.After(2 * time.Second):
		t.Fatalf("slow-A did not enter Append")
	}

	// These wait on slow-A's actor.mu but must not hold b.mu while doing so.
	inspectA := make(chan struct{})
	go func() {
		_, _, _ = b.Inspect(ctx, "slow-A")
		close(inspectA)
	}()
	listDone := make(chan error, 1)
	go func() {
		_, err := b.ListSessions(ctx)
		listDone <- err
	}()
	time.Sleep(10 * time.Millisecond)

	fast := make(chan error, 1)
	go func() {
		if _, err := b.Publish(ctx, "fast-B", ev("msg", "")); err != nil {
			fast <- err
			return
		}
		sub, err := b.Subscribe(ctx, "fast-B", 0, "")
		if err != nil {
			fast <- err
			return
		}
		d := <-sub.Events
		sub.Cancel()
		if d.Event.Seq != 1 {
			fast <- fmt.Errorf("unexpected fast-B seq %d", d.Event.Seq)
			return
		}
		fast <- nil
	}()
	select {
	case err := <-fast:
		if err != nil {
			close(st.releaseAppend)
			t.Fatalf("fast-B failed while slow-A blocked: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(st.releaseAppend)
		t.Fatalf("fast-B was blocked by slow-A's storage I/O")
	}

	evicted := false
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if b.activeActorCount() == 1 {
			evicted = true
			break
		}
	}
	close(st.releaseAppend)
	if !evicted {
		t.Fatalf("sweeper must evict idle-C and fast-B while slow-A is blocked; active=%d", b.activeActorCount())
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("slow-A Publish: %v", err)
	}
	<-inspectA
	if err := <-listDone; err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
}
