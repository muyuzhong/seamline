// Package hotwindow is Seamline's short-lived event store: per-session streams of
// payload-opaque events with a monotonic <epoch>:<seq> cursor, bounded by TTL and byte limits,
// that never evicts the events of the active run.
package hotwindow

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

var (
	ErrInvalidSessionID       = errors.New("hotwindow: session_id must not be empty")
	ErrStreamNotFound         = errors.New("hotwindow: session stream not found")
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

// Storage is the hot-window engine behind the broker. Each method maps onto Redis primitives so
// a RedisStorage can be added without changing the broker:
//
//	EnsureStream  HSETNX meta e <epoch>; HGETALL meta; EXPIRE meta <MetaTTL>
//	Append        one Lua script: next seq, run state machine, active-run quota, XADD <seq>-0,
//	              HSET seq/pin/state/last-run, XTRIM MINID <first evictable seq>, EXPIRE
//	ReadAfter     MULTI: XRANGE (<after>-0 + ; HGETALL meta
//	Touch         EXPIRE
//	Sweep         no-op (Redis expires keys; trimming happens inside Append)
//	List          SCAN + HGETALL
type Storage interface {
	// EnsureStream returns the session stream, creating it with a fresh epoch when absent.
	EnsureStream(ctx context.Context, sessionID string) (model.SessionMeta, error)
	// Stream returns the session stream without creating it.
	Stream(ctx context.Context, sessionID string) (model.SessionMeta, bool, error)
	// Append commits the next event of the stream (seq = LatestSeq+1, a private copy of payload)
	// atomically with the session state it leaves behind, then applies retention. It owns the run
	// state machine: run_started while no run is active opens a run at this event, a terminal
	// event (run_finished, run_failed, run_interrupted) ends the active run, and every other event
	// leaves the run as is. Nothing changes when it returns an error.
	Append(ctx context.Context, sessionID, eventType string, payload json.RawMessage, now time.Time) (model.Event, model.SessionMeta, error)
	// ReadAfter returns the retained events with Seq > afterSeq and the stream window, read atomically.
	ReadAfter(ctx context.Context, sessionID string, afterSeq uint64) ([]model.Event, model.SessionMeta, error)
	// Touch refreshes the idle deadline of an existing stream. Missing streams are ignored.
	Touch(ctx context.Context, sessionID string) error
	// Sweep applies time-based retention and drops streams idle for longer than MetaTTL.
	Sweep(ctx context.Context, now time.Time) error
	// List returns every existing stream ordered by creation time.
	List(ctx context.Context) ([]model.SessionMeta, error)
}
