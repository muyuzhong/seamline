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
	CreatedAt time.Time       `json:"created_at"`
}

type PublishRequest struct {
	StreamID string          `json:"stream_id,omitempty"`
	Seq      uint64          `json:"seq,omitempty"`
	Type     string          `json:"type"`
	Delta    string          `json:"delta,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Fin      bool            `json:"fin"`
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
)

type OutEvent struct {
	Kind      EventKind       `json:"kind"`
	Offset    uint64          `json:"offset,omitempty"`
	Epoch     string          `json:"epoch"`
	StreamID  string          `json:"stream_id,omitempty"`
	Seq       uint64          `json:"seq,omitempty"`
	Type      string          `json:"type,omitempty"`
	Delta     string          `json:"delta,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Fin       bool            `json:"fin,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type InFlightView struct {
	StreamID  string    `json:"stream_id"`
	Type      string    `json:"type"`
	Merged    string    `json:"merged"`
	LastSeq   uint64    `json:"last_seq"`
	StartedAt time.Time `json:"started_at"`
}

type SessionMeta struct {
	SessionID  string       `json:"session_id"`
	Epoch      string       `json:"epoch"`
	State      SessionState `json:"state"`
	LastOffset uint64       `json:"last_offset"`
	CreatedAt  time.Time    `json:"created_at"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

type SessionSnapshot struct {
	Meta            SessionMeta   `json:"meta"`
	InFlight        *InFlightView `json:"in_flight,omitempty"`
	SubscriberCount int           `json:"subscriber_count"`
}
