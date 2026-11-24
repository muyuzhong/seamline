package model

import "testing"

func TestStateFromRecordType(t *testing.T) {
	cases := []struct {
		recType string
		want    SessionState
	}{
		{recType: "run_finished", want: StateCompleted},
		{recType: "run_failed", want: StateFailed},
		{recType: "assistant", want: StateIdle},
		{recType: "user_message", want: StateIdle},
		{recType: "", want: StateIdle},
	}
	for _, tc := range cases {
		if got := StateFromRecordType(tc.recType); got != tc.want {
			t.Fatalf("StateFromRecordType(%q) = %q, want %q", tc.recType, got, tc.want)
		}
	}
}

func TestTerminalState(t *testing.T) {
	cases := []struct {
		eventType string
		want      SessionState
		terminal  bool
	}{
		{TypeRunFinished, StateCompleted, true},
		{TypeRunFailed, StateFailed, true},
		{TypeRunInterrupted, StateInterrupted, true},
		{TypeRunStarted, "", false},
		{"assistant_delta", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := TerminalState(tc.eventType)
		if got != tc.want || ok != tc.terminal {
			t.Fatalf("TerminalState(%q) = (%q, %v), want (%q, %v)", tc.eventType, got, ok, tc.want, tc.terminal)
		}
	}
}
