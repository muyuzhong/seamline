package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/muyuzhong/seamline/internal/broker"
	"github.com/muyuzhong/seamline/internal/model"
)

const defaultKeepaliveInterval = 15 * time.Second

// Config configures the Seamline HTTP/SSE transport layer.
type Config struct {
	KeepaliveInterval time.Duration
	EnableDemo        bool
}

type handler struct {
	broker            *broker.Broker
	keepaliveInterval time.Duration

	demoMu      sync.Mutex
	activeDemos map[string]struct{}
}

// NewHandler constructs an http.Handler serving the Seamline REST + Resumable SSE API.
func NewHandler(b *broker.Broker, cfg Config) http.Handler {
	keepalive := cfg.KeepaliveInterval
	if keepalive <= 0 {
		keepalive = defaultKeepaliveInterval
	}

	h := &handler{
		broker:            b,
		keepaliveInterval: keepalive,
		activeDemos:       make(map[string]struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.handleHealthz)
	mux.HandleFunc("POST /v1/sessions", h.handleCreateSession)
	mux.HandleFunc("GET /v1/sessions", h.handleListSessions)
	mux.HandleFunc("GET /v1/sessions/{id}", h.handleInspectSession)
	mux.HandleFunc("POST /v1/sessions/{id}/events", h.handlePublishEvent)
	mux.HandleFunc("GET /v1/sessions/{id}/stream", h.handleStream)

	if cfg.EnableDemo {
		mux.HandleFunc("GET /{$}", h.handleDemoIndex)
		mux.HandleFunc("POST /v1/demo/simulate", h.handleDemoSimulate)
	}

	return withCORS(mux)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *handler) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"epoch":  h.broker.Epoch(),
	})
}

func (h *handler) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
	}
	if r.Body != nil && r.Body != http.NoBody {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid JSON request body")
			return
		}
	}

	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		sessionID = generateID("sess_")
	}

	meta, err := h.broker.CreateSession(r.Context(), sessionID)
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, meta)
}

func (h *handler) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := h.broker.ListSessions(r.Context())
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	if sessions == nil {
		sessions = []model.SessionSnapshot{}
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (h *handler) handleInspectSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	snap, ok, err := h.broker.Inspect(r.Context(), sessionID)
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (h *handler) handlePublishEvent(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	var req model.PublishRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	res, err := h.broker.Publish(r.Context(), sessionID, req)
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handler) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported by response writer")
		return
	}

	sessionID := r.PathValue("id")
	afterEpoch, afterOffset, err := extractStreamCursor(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	sub, err := h.broker.Subscribe(r.Context(), sessionID, afterOffset, afterEpoch)
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	defer sub.Cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	if _, err := fmt.Fprintf(w, ": connected epoch=%s\n\n", sub.Epoch); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(h.keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, ok := <-sub.Events:
			if !ok {
				if errors.Is(sub.Err(), broker.ErrSlowSubscriber) {
					_, _ = io.WriteString(w, "event: stream_error\ndata: {\"error\":\"slow_subscriber\"}\n\n")
					flusher.Flush()
				}
				return
			}

			data, err := json.Marshal(ev)
			if err != nil {
				return
			}

			switch ev.Kind {
			case model.EventCommitted:
				if _, err := fmt.Fprintf(w, "id: %s:%d\nevent: record\ndata: %s\n\n", ev.Epoch, ev.Offset, data); err != nil {
					return
				}
			case model.EventInFlightSnapshot:
				if _, err := fmt.Fprintf(w, "event: inflight_snapshot\ndata: %s\n\n", data); err != nil {
					return
				}
			case model.EventDelta:
				if _, err := fmt.Fprintf(w, "event: delta\ndata: %s\n\n", data); err != nil {
					return
				}
			default:
				if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Kind, data); err != nil {
					return
				}
			}
			flusher.Flush()
		}
	}
}

func extractStreamCursor(r *http.Request) (epoch string, offset uint64, err error) {
	if lastID := strings.TrimSpace(r.Header.Get("Last-Event-ID")); lastID != "" {
		return parseCursor(lastID)
	}

	q := r.URL.Query()
	rawOffset := strings.TrimSpace(q.Get("after_offset"))
	rawEpoch := strings.TrimSpace(q.Get("epoch"))
	if rawOffset == "" {
		return rawEpoch, 0, nil
	}

	parsedEpoch, parsedOffset, err := parseCursor(rawOffset)
	if err != nil {
		return "", 0, err
	}
	if parsedEpoch == "" {
		parsedEpoch = rawEpoch
	}
	return parsedEpoch, parsedOffset, nil
}

func parseCursor(raw string) (epoch string, offset uint64, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, nil
	}
	if idx := strings.LastIndexByte(raw, ':'); idx != -1 {
		epoch = raw[:idx]
		offsetStr := raw[idx+1:]
		if epoch == "" || offsetStr == "" {
			return "", 0, errors.New("invalid cursor format")
		}
		n, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return "", 0, fmt.Errorf("invalid cursor offset: %w", err)
		}
		return epoch, n, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid cursor offset: %w", err)
	}
	return "", n, nil
}

func (h *handler) writeBrokerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, broker.ErrInvalidInput), errors.Is(err, broker.ErrSequenceGap):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, broker.ErrStreamConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, broker.ErrBrokerClosed):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func generateID(prefix string) string {
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
	}
	return prefix + hex.EncodeToString(buf[:])
}
