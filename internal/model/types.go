package model

import (
	"encoding/json"
	"time"
)

type SessionState string

const (
	StateIdle      SessionState = "idle"
	StateRunning   SessionState = "running"
	StateCompleted SessionState = "completed"
	StateFailed    SessionState = "failed"
)

// StateFromRecordType derives the SessionState implied by a committed Record's Type.
func StateFromRecordType(recordType string) SessionState {
	switch recordType {
	case "run_finished":
		return StateCompleted
	case "run_failed":
		return StateFailed
	default:
		return StateIdle
	}
}

type Record struct {
	Offset    uint64          `json:"offset"`
	Epoch     string          `json:"epoch"`
	StreamID  string          `json:"stream_id,omitempty"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Meta      json.RawMessage `json:"meta,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type PublishRequest struct {
	StreamID string          `json:"stream_id,omitempty"`
	Seq      uint64          `json:"seq,omitempty"`
	Type     string          `json:"type"`
	Delta    string          `json:"delta,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	// Meta is an optional JSON header kept from the first chunk of a stream (for example
	// {"tool_name":"exec_sql","call_id":"call_1"}). It is ignored on later chunks.
	Meta json.RawMessage `json:"meta,omitempty"`
	Fin  bool            `json:"fin"`
}

type PublishResult struct {
	CommittedOffset uint64 `json:"committed_offset,omitempty"`
	Epoch           string `json:"epoch"`
	InFlightSeq     uint64 `json:"in_flight_seq,omitempty"`
	Deduplicated    bool   `json:"deduplicated,omitempty"`
}

type EventKind string

const (
	EventCommitted        EventKind = "record"
	EventInFlightSnapshot EventKind = "inflight_snapshot"
	EventDelta            EventKind = "delta"
	EventCursorReset      EventKind = "cursor_reset"
)

// Cursor reset reasons carried by EventCursorReset.
const (
	ResetEpochMismatch = "epoch_mismatch"
	ResetCursorExpired = "cursor_expired"
	ResetCursorAhead   = "cursor_ahead"
)

// CursorReset tells a resuming subscriber that its cursor cannot be continued gaplessly.
type CursorReset struct {
	Reason          string `json:"reason"`
	RequestedEpoch  string `json:"requested_epoch"`
	RequestedOffset uint64 `json:"requested_offset"`
	CurrentEpoch    string `json:"current_epoch"`
	EarliestOffset  uint64 `json:"earliest_offset"`
	LatestOffset    uint64 `json:"latest_offset"`
}

type OutEvent struct {
	Kind      EventKind       `json:"kind"`
	Offset    uint64          `json:"offset,omitempty"`
	Epoch     string          `json:"epoch"`
	StreamID  string          `json:"stream_id,omitempty"`
	Seq       uint64          `json:"seq,omitempty"`
	Type      string          `json:"type,omitempty"`
	Delta     string          `json:"delta,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Meta      json.RawMessage `json:"meta,omitempty"`
	Fin       bool            `json:"fin,omitempty"`
	Reset     *CursorReset    `json:"reset,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type InFlightView struct {
	StreamID  string          `json:"stream_id"`
	Type      string          `json:"type"`
	Meta      json.RawMessage `json:"meta,omitempty"`
	Merged    string          `json:"merged"`
	LastSeq   uint64          `json:"last_seq"`
	StartedAt time.Time       `json:"started_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type SessionMeta struct {
	SessionID string       `json:"session_id"`
	Epoch     string       `json:"epoch"`
	State     SessionState `json:"state"`
	// LastOffset is the highest offset ever committed in this epoch; it survives history eviction.
	LastOffset uint64 `json:"last_offset"`
	// EarliestOffset is the first offset still retained in the hot window (LastOffset+1 when empty).
	EarliestOffset uint64    `json:"earliest_offset"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type SessionSnapshot struct {
	Meta            SessionMeta    `json:"meta"`
	InFlights       []InFlightView `json:"in_flights"`
	SubscriberCount int            `json:"subscriber_count"`
}
