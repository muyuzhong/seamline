package model

import (
	"encoding/json"
	"time"
)

type SessionState string

const (
	StateIdle        SessionState = "idle"
	StateRunning     SessionState = "running"
	StateCompleted   SessionState = "completed"
	StateFailed      SessionState = "failed"
	StateInterrupted SessionState = "interrupted"
)

// Event types the broker interprets. Every other type is opaque to Seamline.
const (
	TypeRunStarted     = "run_started"
	TypeRunFinished    = "run_finished"
	TypeRunFailed      = "run_failed"
	TypeRunInterrupted = "run_interrupted"
)

// TerminalState returns the state a terminal event type ends a run in.
func TerminalState(eventType string) (SessionState, bool) {
	switch eventType {
	case TypeRunFinished:
		return StateCompleted, true
	case TypeRunFailed:
		return StateFailed, true
	case TypeRunInterrupted:
		return StateInterrupted, true
	default:
		return "", false
	}
}

// Event is the single, payload-opaque unit of a session stream. Its cursor is <Epoch>:<Seq>.
type Event struct {
	Seq       uint64          `json:"seq"`
	Epoch     string          `json:"epoch"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type PublishRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type PublishResult struct {
	Epoch        string `json:"epoch"`
	Seq          uint64 `json:"seq"`
	Deduplicated bool   `json:"deduplicated,omitempty"`
}

// Cursor reset reasons carried by CursorReset.
const (
	ResetEpochMismatch = "epoch_mismatch"
	ResetCursorExpired = "cursor_expired"
	ResetCursorAhead   = "cursor_ahead"
)

// CursorReset tells a resuming subscriber that its cursor cannot be continued gaplessly.
type CursorReset struct {
	Reason            string `json:"reason"`
	RequestedEpoch    string `json:"requested_epoch"`
	RequestedSeq      uint64 `json:"requested_seq"`
	CurrentEpoch      string `json:"current_epoch"`
	EarliestSeq       uint64 `json:"earliest_seq"`
	LatestSeq         uint64 `json:"latest_seq"`
	ActiveRunStartSeq uint64 `json:"active_run_start_seq"`
}

type DeliveryKind string

const (
	DeliveryEvent       DeliveryKind = "event"
	DeliveryCursorReset DeliveryKind = "cursor_reset"
)

// Delivery is one item of a subscription: a business event or a cursor reset notice.
type Delivery struct {
	Kind  DeliveryKind
	Event Event
	Reset *CursorReset
}

type SessionMeta struct {
	SessionID string       `json:"session_id"`
	Epoch     string       `json:"epoch"`
	State     SessionState `json:"state"`
	// EarliestSeq is the first retained seq (LatestSeq+1 when nothing is retained).
	EarliestSeq uint64 `json:"earliest_seq"`
	// LatestSeq is the highest seq ever assigned in this epoch; it survives eviction.
	LatestSeq uint64 `json:"latest_seq"`
	// ActiveRunStartSeq is the first seq of the active run (0 when no run is active).
	ActiveRunStartSeq uint64    `json:"active_run_start_seq"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type SessionSnapshot struct {
	Meta            SessionMeta `json:"meta"`
	SubscriberCount int         `json:"subscriber_count"`
}
