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
