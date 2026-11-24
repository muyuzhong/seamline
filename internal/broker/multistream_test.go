package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/storage"
)

// multiClient reconstructs committed records plus any number of concurrent in-flight streams,
// failing on any gap or duplicate in either cursor space.
type multiClient struct {
	epoch      string
	lastOffset uint64
	records    map[uint64]model.OutEvent
	bufs       map[string]*strings.Builder
	lastSeq    map[string]uint64
}

func newMultiClient() *multiClient {
	return &multiClient{
		records: make(map[uint64]model.OutEvent),
		bufs:    make(map[string]*strings.Builder),
		lastSeq: make(map[string]uint64),
	}
}

func (c *multiClient) dropInFlight() {
	c.bufs = make(map[string]*strings.Builder)
	c.lastSeq = make(map[string]uint64)
}

func (c *multiClient) apply(t *testing.T, ev model.OutEvent) {
	t.Helper()
	switch ev.Kind {
	case model.EventCommitted:
		c.applyRecord(t, ev)
	case model.EventInFlightSnapshot:
		b := &strings.Builder{}
		b.WriteString(ev.Delta)
		c.bufs[ev.StreamID] = b
		c.lastSeq[ev.StreamID] = ev.Seq
	case model.EventDelta:
		if want := c.lastSeq[ev.StreamID] + 1; ev.Seq != want {
			t.Errorf("stream %s: got seq %d, want %d (gap or duplicate)", ev.StreamID, ev.Seq, want)
			return
		}
		if c.bufs[ev.StreamID] == nil {
			c.bufs[ev.StreamID] = &strings.Builder{}
		}
		c.bufs[ev.StreamID].WriteString(ev.Delta)
		c.lastSeq[ev.StreamID] = ev.Seq
	default:
		t.Errorf("unexpected event kind %q", ev.Kind)
	}
}

func (c *multiClient) applyRecord(t *testing.T, ev model.OutEvent) {
	t.Helper()
	if ev.Offset != c.lastOffset+1 {
		t.Errorf("record offset %d after %d (gap or duplicate)", ev.Offset, c.lastOffset)
		return
	}
	c.lastOffset, c.epoch = ev.Offset, ev.Epoch
	c.records[ev.Offset] = ev
	if buf, ok := c.bufs[ev.StreamID]; ok && ev.StreamID != "" {
		if got := decodeContent(t, ev.Payload); buf.String() != got {
			t.Errorf("stream %s rebuilt %q, record has %q", ev.StreamID, buf.String(), got)
		}
		delete(c.bufs, ev.StreamID)
		delete(c.lastSeq, ev.StreamID)
	}
}

type parallelStream struct {
	id    string
	meta  string
	start int
	text  strings.Builder
}

func TestParallelStreamsWithMetaReconnectZeroGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store := storage.NewMemoryStorage(storage.WithEpochSource(fixedEpochs("ep-par")))
	b := New(store, Config{SubscriberBufferSize: 4096})
	closeBroker(t, b)

	const sessionID = "parallel"
	const chunks = 60
	streams := []*parallelStream{
		{id: "text", start: 0},
		{id: "call_a", meta: `{"tool":"a","call_id":"call_a"}`, start: 10},
		{id: "call_b", meta: `{"tool":"b","call_id":"call_b"}`, start: 20},
	}

	var ready, done sync.WaitGroup
	results := make([]*multiClient, 20)
	for i := range results {
		done.Add(1)
		if i < 5 {
			ready.Add(1)
		}
		go func(idx int) {
			defer done.Done()
			results[idx] = runParallelSubscriber(ctx, t, b, sessionID, idx, &ready)
		}(i)
	}
	ready.Wait()

	mustPublish(t, b, sessionID, model.PublishRequest{Type: "user", Delta: "run three tools"})
	for round := 0; round < 20+chunks; round++ {
		for _, s := range streams {
			seq := round - s.start + 1
			if seq < 1 || seq > chunks {
				continue
			}
			tok := fmt.Sprintf("%s%02d|", s.id, seq)
			s.text.WriteString(tok)
			req := model.PublishRequest{StreamID: s.id, Seq: uint64(seq), Type: "assistant", Delta: tok, Fin: seq == chunks}
			if seq == 1 && s.meta != "" {
				req.Meta = json.RawMessage(s.meta)
			}
			mustPublish(t, b, sessionID, req)
		}
		if round%4 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	mustPublish(t, b, sessionID, model.PublishRequest{Type: "run_finished", Delta: "done"})
	done.Wait()

	// Streams finish in start order, so they compact into offsets 2, 3, 4.
	for idx, c := range results {
		if c == nil {
			continue
		}
		for i, s := range streams {
			rec, ok := c.records[uint64(i+2)]
			if !ok || rec.StreamID != s.id || decodeContent(t, rec.Payload) != s.text.String() || string(rec.Meta) != s.meta {
				t.Errorf("client %d: record %d = %+v, want stream %s with meta %q", idx, i+2, rec, s.id, s.meta)
			}
		}
	}
}

func runParallelSubscriber(ctx context.Context, t *testing.T, b *Broker, sessionID string, idx int, ready *sync.WaitGroup) *multiClient {
	rng := rand.New(rand.NewSource(int64(7000 + idx)))
	c := newMultiClient()
	drops := 0
	if idx >= 13 {
		drops = 2
	}
	if idx >= 5 {
		time.Sleep(time.Duration(rng.Intn(40)) * time.Millisecond)
	}
	for c.lastOffset < 5 {
		sub, err := b.Subscribe(ctx, sessionID, c.lastOffset, c.epoch)
		if idx < 5 && c.lastOffset == 0 && drops == 0 {
			ready.Done()
		}
		if err != nil {
			t.Errorf("client %d Subscribe: %v", idx, err)
			return nil
		}
		dropAfter := rng.Intn(80) + 20
		for n := 0; c.lastOffset < 5; n++ {
			ev, ok := nextOrTimeout(ctx, sub)
			if !ok {
				t.Errorf("client %d stream ended at offset %d (err=%v)", idx, c.lastOffset, sub.Err())
				sub.Cancel()
				return nil
			}
			c.apply(t, ev)
			if drops > 0 && n >= dropAfter {
				drops--
				break
			}
		}
		sub.Cancel()
		c.dropInFlight()
	}
	return c
}

func nextOrTimeout(ctx context.Context, sub *Subscription) (model.OutEvent, bool) {
	select {
	case ev, ok := <-sub.Events:
		return ev, ok
	case <-ctx.Done():
		return model.OutEvent{}, false
	}
}

func TestTooManyConcurrentStreams(t *testing.T) {
	b := New(storage.NewMemoryStorage(), Config{MaxConcurrentStreams: 2})
	closeBroker(t, b)
	ctx := context.Background()
	const sid = "slots"

	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 1, Delta: "a"})
	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s2", Seq: 1, Delta: "b"})
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "s3", Seq: 1, Delta: "c"}); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("third concurrent stream: want ErrTooManyStreams, got %v", err)
	}
	// A single-chunk stream never occupies a slot, so it is accepted while all slots are busy.
	if res := mustPublish(t, b, sid, model.PublishRequest{StreamID: "one", Seq: 1, Delta: "x", Fin: true}); res.CommittedOffset != 1 {
		t.Fatalf("single-chunk stream: %+v", res)
	}
	// Finishing a stream frees its slot.
	if res := mustPublish(t, b, sid, model.PublishRequest{StreamID: "s1", Seq: 2, Fin: true}); res.CommittedOffset != 2 {
		t.Fatalf("fin s1: %+v", res)
	}
	mustPublish(t, b, sid, model.PublishRequest{StreamID: "s3", Seq: 1, Delta: "c"})

	snap, _, _ := b.Inspect(ctx, sid)
	if len(snap.InFlights) != 2 || snap.InFlights[0].StreamID != "s2" || snap.InFlights[1].StreamID != "s3" {
		t.Fatalf("expected s2 and s3 in flight (ordered by start), got %+v", snap.InFlights)
	}
	if _, err := b.Publish(ctx, sid, model.PublishRequest{StreamID: "s6", Seq: 1, Meta: json.RawMessage(`{bad`)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid meta JSON: want ErrInvalidInput, got %v", err)
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
