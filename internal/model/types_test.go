package model

import "testing"

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
