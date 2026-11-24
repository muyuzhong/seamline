package storage

import (
	"context"
	"errors"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

var (
	ErrInvalidSessionID   = errors.New("storage: session_id must not be empty")
	ErrNonMonotonicOffset = errors.New("storage: record offset must equal the stream's last offset + 1")
	ErrStreamNotFound     = errors.New("storage: session stream not found")
	ErrEpochMismatch      = errors.New("storage: record epoch does not match the session stream epoch")
)

const (
	DefaultHistoryTTL      = 15 * time.Minute
	DefaultMaxHistoryBytes = 8 << 20
	DefaultMetaTTL         = 24 * time.Hour
)

// Retention bounds the committed-record hot window kept for every session stream.
type Retention struct {
	// HistoryTTL evicts committed records older than this (default 15m).
	HistoryTTL time.Duration
	// MaxHistoryBytes evicts the oldest committed records once a session's retained
	// records exceed this many accounted bytes (default 8 MiB). The newest record is always kept.
	MaxHistoryBytes int64
	// MetaTTL drops a whole stream (epoch, last offset, metadata) after it has been idle
	// this long (default 24h). It must be much larger than HistoryTTL so that a cursor whose
	// records were evicted is reported as expired instead of silently restarting offsets.
	MetaTTL time.Duration
}

func (r Retention) normalized() Retention {
	if r.HistoryTTL <= 0 {
		r.HistoryTTL = DefaultHistoryTTL
	}
	if r.MaxHistoryBytes <= 0 {
		r.MaxHistoryBytes = DefaultMaxHistoryBytes
	}
	if r.MetaTTL <= 0 {
		r.MetaTTL = DefaultMetaTTL
	}
	return r
}

// StreamInfo identifies a session stream incarnation and its retained window.
type StreamInfo struct {
	// Epoch identifies this incarnation of the session stream. A new epoch is generated
	// whenever the stream is (re)created, so offsets never silently restart under one epoch.
	Epoch string
	// EarliestOffset is the first retained offset; LatestOffset+1 when nothing is retained.
	EarliestOffset uint64
	// LatestOffset is the highest offset ever appended in this epoch (survives eviction).
	LatestOffset  uint64
	RetainedBytes int64
	CreatedAt     time.Time
}

// Storage is the hot-window engine behind the broker. Every method maps onto Redis
// primitives so a RedisStorage can be added later without changing the broker:
//
//	EnsureStream  HSETNX meta e <epoch>; HGETALL meta; EXPIRE meta <MetaTTL>
//	Append        one Lua script: check e and s+1, HINCRBY s, XADD <offset>-0, XTRIM MINID, EXPIRE
//	ReadFrom      XRANGE (<after>-0 + plus HGETALL meta (MULTI)
//	Touch         EXPIRE meta/stream <MetaTTL>
//	Sweep         no-op (Redis expires keys itself)
type Storage interface {
	// EnsureStream returns the session stream, creating it with a fresh epoch when it does not exist.
	EnsureStream(ctx context.Context, sessionID string) (StreamInfo, error)
	// Stream returns the session stream without creating it.
	Stream(ctx context.Context, sessionID string) (StreamInfo, bool, error)
	// Append commits rec. rec.Epoch must equal the stream epoch and rec.Offset must be LatestOffset+1.
	// The oldest committed records beyond the retention limits are evicted in the same step.
	Append(ctx context.Context, sessionID string, rec model.Record) error
	// ReadFrom returns the retained records with Offset > afterOffset and the stream window, read atomically.
	ReadFrom(ctx context.Context, sessionID string, afterOffset uint64) ([]model.Record, StreamInfo, error)
	// Touch refreshes the idle deadline of an existing stream. Missing streams are ignored.
	Touch(ctx context.Context, sessionID string) error
	// Sweep evicts records older than HistoryTTL and drops streams idle for longer than MetaTTL.
	Sweep(ctx context.Context, now time.Time) error
	// SaveMeta stores best-effort descriptive metadata for an existing stream.
	SaveMeta(ctx context.Context, meta model.SessionMeta) error
	// LoadMeta returns the session metadata reconciled with the stream (Epoch, LastOffset,
	// EarliestOffset and, when newer records exist, State/UpdatedAt). saved reports whether a
	// SaveMeta copy existed. It returns ErrStreamNotFound when the stream does not exist.
	LoadMeta(ctx context.Context, sessionID string) (meta model.SessionMeta, saved bool, err error)
	// ListSessions returns reconciled metadata for every existing stream.
	ListSessions(ctx context.Context) ([]model.SessionMeta, error)
}
