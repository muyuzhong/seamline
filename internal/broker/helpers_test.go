package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

func closeBroker(t *testing.T, b *Broker) {
	t.Helper()
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Broker.Close failed: %v", err)
		}
	})
}

// fixedEpochs returns an epoch generator that yields the given epochs in order (repeating the last).
func fixedEpochs(epochs ...string) func() string {
	var mu sync.Mutex
	i := 0
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		e := epochs[min(i, len(epochs)-1)]
		i++
		return e
	}
}

// ev builds a publish request; payload is raw JSON ("" for none).
func ev(eventType, payload string) model.PublishRequest {
	req := model.PublishRequest{Type: eventType}
	if payload != "" {
		req.Payload = json.RawMessage(payload)
	}
	return req
}

func textPayload(s string) string {
	raw, _ := json.Marshal(map[string]string{"text": s})
	return string(raw)
}

func payloadText(t *testing.T, e model.Event) string {
	t.Helper()
	var p struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatalf("decode payload %s: %v", e.Payload, err)
	}
	return p.Text
}

func recv(t *testing.T, sub *Subscription, timeout time.Duration) model.Delivery {
	t.Helper()
	select {
	case d, ok := <-sub.Events:
		if !ok {
			t.Fatalf("subscription closed unexpectedly (err=%v)", sub.Err())
		}
		return d
	case <-time.After(timeout):
		t.Fatalf("timed out after %v waiting for a delivery", timeout)
		return model.Delivery{}
	}
}

func recvEvent(t *testing.T, sub *Subscription) model.Event {
	t.Helper()
	d := recv(t, sub, 2*time.Second)
	if d.Kind != model.DeliveryEvent {
		t.Fatalf("expected an event delivery, got %+v", d)
	}
	return d.Event
}

func mustPublish(t *testing.T, b *Broker, sessionID string, req model.PublishRequest) model.PublishResult {
	t.Helper()
	res, err := b.Publish(context.Background(), sessionID, req)
	if err != nil {
		t.Fatalf("Publish(%s, %s): %v", sessionID, req.Type, err)
	}
	return res
}

func mustInspect(t *testing.T, b *Broker, sessionID string) model.SessionMeta {
	t.Helper()
	snap, ok, err := b.Inspect(context.Background(), sessionID)
	if err != nil || !ok {
		t.Fatalf("Inspect(%s): ok=%v err=%v", sessionID, ok, err)
	}
	return snap.Meta
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var errInjectedStorage = errors.New("injected storage failure")

// flakyStore injects Append failures and can block Append for one session.
type flakyStore struct {
	*hotwindow.MemoryStorage
	failNextAppend atomic.Bool

	slowSessionID string
	enteredAppend chan struct{}
	releaseAppend chan struct{}
}

func (f *flakyStore) Append(ctx context.Context, id string, c hotwindow.Commit) error {
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
	return f.MemoryStorage.Append(ctx, id, c)
}
