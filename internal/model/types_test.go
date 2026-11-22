package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestModelJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	rec := Record{
		Offset:    1,
		Epoch:     "a1b2c3d4e5f6",
		StreamID:  "stream-1",
		Type:      "assistant",
		Payload:   json.RawMessage(`{"content":"hello"}`),
		CreatedAt: now,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal Record: %v", err)
	}
	var gotRec Record
	if err := json.Unmarshal(raw, &gotRec); err != nil {
		t.Fatalf("unmarshal Record: %v", err)
	}
	if gotRec.Offset != 1 || gotRec.Epoch != rec.Epoch || gotRec.StreamID != "stream-1" || string(gotRec.Payload) != `{"content":"hello"}` {
		t.Fatalf("unexpected Record roundtrip: %+v", gotRec)
	}

	snap := SessionSnapshot{
		Meta: SessionMeta{
			SessionID:  "s-1",
			Epoch:      "a1b2c3d4e5f6",
			State:      StateIdle,
			LastOffset: 5,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		InFlight:        nil,
		SubscriberCount: 2,
	}
	snapJSON, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal SessionSnapshot: %v", err)
	}
	if strings.Contains(string(snapJSON), `"in_flight"`) {
		t.Fatalf("expected nil InFlight to be omitted, got: %s", string(snapJSON))
	}

	ev := OutEvent{
		Kind:      EventInFlightSnapshot,
		Epoch:     "a1b2c3d4e5f6",
		StreamID:  "stream-1",
		Seq:       7,
		Type:      "assistant",
		Delta:     "partial text",
		CreatedAt: now,
	}
	evJSON, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal OutEvent: %v", err)
	}
	var gotEv OutEvent
	if err := json.Unmarshal(evJSON, &gotEv); err != nil {
		t.Fatalf("unmarshal OutEvent: %v", err)
	}
	if gotEv.Kind != EventInFlightSnapshot || gotEv.Seq != 7 || gotEv.Delta != "partial text" {
		t.Fatalf("unexpected OutEvent roundtrip: %+v", gotEv)
	}
}

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
