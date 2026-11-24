package broker

import (
	"context"
	"fmt"
	"strings"
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

func TestActiveRunIsReplayableBeyondHistoryLimits(t *testing.T) {
	// History holds about two deltas; the run below is twenty times larger.
	store := hotwindow.NewMemoryStorage(hotwindow.WithRetention(hotwindow.Retention{MaxHistoryBytes: 300}))
	b := New(store, Config{})
	closeBroker(t, b)
	const sid = "long-run"

	mustPublish(t, b, sid, ev("msg", textPayload("before the run")))
	mustPublish(t, b, sid, ev(model.TypeRunStarted, ""))
	var want strings.Builder
	for i := 0; i < 40; i++ {
		tok := fmt.Sprintf("tok-%02d ", i)
		want.WriteString(tok)
		mustPublish(t, b, sid, ev("assistant_delta", textPayload(tok)))
	}

	// A page load in the middle of the run gets the whole run, from run_started on.
	if got := replayRunText(t, b, sid); got != want.String() {
		t.Fatalf("mid-run replay lost part of the active run:\n got %q\nwant %q", got, want.String())
	}

	// After run_finished the latest run is still fully retained (it is the latest finished run).
	mustPublish(t, b, sid, ev(model.TypeRunFinished, ""))
	if got := replayRunText(t, b, sid); got != want.String() {
		t.Fatalf("latest finished run was evicted at its end:\n got %q", got)
	}

	// Once another run finishes, the previous one is ordinary history and can be evicted.
	mustPublish(t, b, sid, ev(model.TypeRunStarted, ""))
	mustPublish(t, b, sid, ev(model.TypeRunFinished, ""))
	if meta := mustInspect(t, b, sid); meta.EarliestSeq <= 2 {
		t.Fatalf("previous run must become evictable once a newer run finished, earliest_seq=%d", meta.EarliestSeq)
	}
}

// replayRunText subscribes fresh and concatenates the assistant_delta texts of the retained window.
func replayRunText(t *testing.T, b *Broker, sid string) string {
	t.Helper()
	meta := mustInspect(t, b, sid)
	sub, err := b.Subscribe(context.Background(), sid, 0, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()
	var text strings.Builder
	sawStart := false
	for seq := meta.EarliestSeq; seq <= meta.LatestSeq; seq++ {
		e := recvEvent(t, sub)
		sawStart = sawStart || e.Type == model.TypeRunStarted
		if e.Type == "assistant_delta" && sawStart {
			text.WriteString(payloadText(t, e))
		}
	}
	return text.String()
}
