package broker

import (
	"context"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
)

func TestRunStartsOnlyOnExplicitRunStarted(t *testing.T) {
	b := New(hotwindow.NewMemoryStorage(), Config{})
	closeBroker(t, b)
	const sid = "runs"

	steps := []struct {
		eventType string
		wantPin   uint64
		wantState model.SessionState
	}{
		{"user_message", 0, model.StateIdle},             // 1: not a run
		{model.TypeRunStarted, 2, model.StateRunning},    // 2: opens the run
		{"assistant_delta", 2, model.StateRunning},       // 3
		{model.TypeRunStarted, 2, model.StateRunning},    // 4: keeps the original start
		{model.TypeRunFinished, 0, model.StateCompleted}, // 5: ends it
		{model.TypeRunFailed, 0, model.StateCompleted},   // 6: terminal outside a run changes nothing
		{"note", 0, model.StateCompleted},                // 7
		{model.TypeRunStarted, 8, model.StateRunning},    // 8
		{model.TypeRunInterrupted, 0, model.StateInterrupted},
	}
	for i, s := range steps {
		mustPublish(t, b, sid, ev(s.eventType, ""))
		meta := mustInspect(t, b, sid)
		if meta.ActiveRunStartSeq != s.wantPin || meta.State != s.wantState || meta.LatestSeq != uint64(i+1) {
			t.Fatalf("after %s (seq %d): pin=%d state=%s, want pin=%d state=%s", s.eventType, i+1, meta.ActiveRunStartSeq, meta.State, s.wantPin, s.wantState)
		}
	}
}

func TestActorWithActiveRunIsNotEvicted(t *testing.T) {
	b := New(hotwindow.NewMemoryStorage(), Config{IdleSessionTTL: 20 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)

	mustPublish(t, b, "running", ev(model.TypeRunStarted, ""))
	mustPublish(t, b, "idle", ev("msg", ""))
	waitFor(t, "idle actor eviction", func() bool { return b.activeActorCount() == 1 })
	time.Sleep(50 * time.Millisecond)
	if b.activeActorCount() != 1 {
		t.Fatalf("the actor of an active run owns its idle timer and must stay")
	}
	mustPublish(t, b, "running", ev(model.TypeRunFinished, ""))
	waitFor(t, "eviction after the run ended", func() bool { return b.activeActorCount() == 0 })
}

func TestIdleRunIsSealedWithRunFailed(t *testing.T) {
	ctx := context.Background()
	b := New(hotwindow.NewMemoryStorage(), Config{ActiveRunIdleTimeout: 80 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	closeBroker(t, b)
	const sid = "seal"

	sub, err := b.Subscribe(ctx, sid, 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	mustPublish(t, b, sid, ev(model.TypeRunStarted, ""))
	// Events inside the run keep it alive well past the timeout.
	for i := 0; i < 20; i++ {
		mustPublish(t, b, sid, ev("assistant_delta", textPayload(".")))
		time.Sleep(10 * time.Millisecond)
	}
	if meta := mustInspect(t, b, sid); meta.State != model.StateRunning || meta.LatestSeq != 21 {
		t.Fatalf("an active producer must not be sealed: %+v", meta)
	}

	// Then the producer goes silent: the sweeper ends the run with run_failed {"reason":"idle_timeout"}.
	var sealed model.Event
	for sealed.Type != model.TypeRunFailed {
		sealed = recvEvent(t, sub)
	}
	if sealed.Seq != 22 || string(sealed.Payload) != `{"reason":"idle_timeout"}` {
		t.Fatalf("unexpected sealing event: %+v (%s)", sealed, sealed.Payload)
	}
	if meta := mustInspect(t, b, sid); meta.State != model.StateFailed || meta.ActiveRunStartSeq != 0 {
		t.Fatalf("sealed run must be failed: %+v", meta)
	}

	// Events outside a run never start the idle timer.
	mustPublish(t, b, sid, ev("note", ""))
	time.Sleep(200 * time.Millisecond)
	if meta := mustInspect(t, b, sid); meta.LatestSeq != 23 || meta.State != model.StateFailed {
		t.Fatalf("an event outside a run must not be sealed: %+v", meta)
	}
}
