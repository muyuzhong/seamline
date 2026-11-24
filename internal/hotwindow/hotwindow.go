// Package hotwindow is Seamline's short-lived event store: per-session streams of
// payload-opaque events with a monotonic <epoch>:<seq> cursor, bounded by TTL and byte limits,
// that never evicts the events of the active run.
package hotwindow

import (
	"context"
	"errors"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

var (
	ErrInvalidSessionID       = errors.New("hotwindow: session_id must not be empty")
	ErrStreamNotFound         = errors.New("hotwindow: session stream not found")
	ErrEpochMismatch          = errors.New("hotwindow: event epoch does not match the session stream epoch")
	ErrNonMonotonicSeq        = errors.New("hotwindow: event seq must equal the stream's latest seq + 1")
	ErrInvalidRunTransition   = errors.New("hotwindow: invalid active run transition")
	ErrActiveRunQuotaExceeded = errors.New("hotwindow: active run would exceed MaxActiveRunBytes")
)

const (
	DefaultHistoryTTL        = 15 * time.Minute
	DefaultMaxHistoryBytes   = 8 << 20
	DefaultMaxActiveRunBytes = 16 << 20
	DefaultMetaTTL           = 24 * time.Hour
)

// Retention bounds what a session stream keeps.
type Retention struct {
	// HistoryTTL evicts history events older than this (default 15m). The latest finished run
	// counts from its end instead of each event's creation time.
	HistoryTTL time.Duration
	// MaxHistoryBytes evicts the oldest history events beyond this many accounted bytes per
	// session (default 8 MiB). The active run and the latest finished run are exempt, and the
	// newest event is always kept.
	MaxHistoryBytes int64
	// MaxActiveRunBytes bounds the retained bytes of the active run (default 16 MiB). Terminal
	// events are exempt so a run can always end.
	MaxActiveRunBytes int64
	// MetaTTL drops a whole stream (epoch, seq counter, state) after it has been idle this long
	// (default 24h). Recreating it yields a new epoch.
	MetaTTL time.Duration
}

func (r Retention) normalized() Retention {
	if r.HistoryTTL <= 0 {
		r.HistoryTTL = DefaultHistoryTTL
	}
	if r.MaxHistoryBytes <= 0 {
		r.MaxHistoryBytes = DefaultMaxHistoryBytes
	}
	if r.MaxActiveRunBytes <= 0 {
		r.MaxActiveRunBytes = DefaultMaxActiveRunBytes
	}
	if r.MetaTTL <= 0 {
		r.MetaTTL = DefaultMetaTTL
	}
	return r
}

// Commit is one append: the event plus the session state it leaves behind. Both are written
// atomically, so storage never holds an event without its run bookkeeping.
type Commit struct {
	Event model.Event
	// State is the session state after this event.
	State model.SessionState
	// ActiveRunStartSeq is the first seq of the active run after this event (0 = no active run).
	// Allowed transitions: 0 -> 0 or Event.Seq (a run starts); S -> S or 0 (the run ends).
	ActiveRunStartSeq uint64
}

// StreamInfo identifies a session stream incarnation and describes its retained window.
type StreamInfo struct {
	SessionID string
	// Epoch identifies this incarnation; it changes only when the stream is recreated.
	Epoch string
	// EarliestSeq is the first retained seq (LatestSeq+1 when nothing is retained).
	EarliestSeq uint64
	// LatestSeq is the highest seq ever appended in this epoch (survives eviction).
	LatestSeq         uint64
	ActiveRunStartSeq uint64
	State             model.SessionState
	RetainedBytes     int64
	ActiveRunBytes    int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Storage is the hot-window engine behind the broker. Each method maps onto Redis primitives so
// a RedisStorage can be added without changing the broker:
//
//	EnsureStream  HSETNX meta e <epoch>; HGETALL meta; EXPIRE meta <MetaTTL>
//	Append        one Lua script: check epoch and seq, active-run quota, XADD <seq>-0,
//	              HSET seq/pin/state/last-run, XTRIM MINID <first evictable seq>, EXPIRE
//	ReadAfter     MULTI: XRANGE (<after>-0 + ; HGETALL meta
//	Touch         EXPIRE
//	Sweep         no-op (Redis expires keys; trimming happens inside Append)
//	List          SCAN + HGETALL
type Storage interface {
	// EnsureStream returns the session stream, creating it with a fresh epoch when absent.
	EnsureStream(ctx context.Context, sessionID string) (StreamInfo, error)
	// Stream returns the session stream without creating it.
	Stream(ctx context.Context, sessionID string) (StreamInfo, bool, error)
	// Append validates and commits c atomically, then applies retention.
	Append(ctx context.Context, sessionID string, c Commit) error
	// ReadAfter returns the retained events with Seq > afterSeq and the stream window, read atomically.
	ReadAfter(ctx context.Context, sessionID string, afterSeq uint64) ([]model.Event, StreamInfo, error)
	// Touch refreshes the idle deadline of an existing stream. Missing streams are ignored.
	Touch(ctx context.Context, sessionID string) error
	// Sweep applies time-based retention and drops streams idle for longer than MetaTTL.
	Sweep(ctx context.Context, now time.Time) error
	// List returns every existing stream ordered by creation time.
	List(ctx context.Context) ([]StreamInfo, error)
}
