package broker

import (
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
