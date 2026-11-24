package storage

import (
	"context"
	"errors"

	"github.com/muyuzhong/seamline/internal/model"
)

var (
	ErrInvalidSessionID   = errors.New("storage: session_id must not be empty")
	ErrNonMonotonicOffset = errors.New("storage: record offset must be strictly increasing and >= 1")
)

type Storage interface {
	Append(ctx context.Context, sessionID string, rec model.Record) error
	ReadFrom(ctx context.Context, sessionID string, afterOffset uint64) ([]model.Record, error)
	SaveMeta(ctx context.Context, meta model.SessionMeta) error
	LoadMeta(ctx context.Context, sessionID string) (model.SessionMeta, bool, error)
	ListSessions(ctx context.Context) ([]model.SessionMeta, error)
}
