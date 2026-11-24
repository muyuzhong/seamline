package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

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

func recvEvent(t *testing.T, sub *Subscription, timeout time.Duration) model.OutEvent {
	t.Helper()
	select {
	case ev, ok := <-sub.Events:
		if !ok {
			t.Fatalf("subscription closed unexpectedly (err=%v)", sub.Err())
		}
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out after %v waiting for event", timeout)
		return model.OutEvent{}
	}
}

func mustPublish(t *testing.T, b *Broker, sessionID string, req model.PublishRequest) model.PublishResult {
	t.Helper()
	res, err := b.Publish(context.Background(), sessionID, req)
	if err != nil {
		t.Fatalf("Publish(%s, %+v): %v", sessionID, req, err)
	}
	return res
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
