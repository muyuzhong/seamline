package server

import (
	"context"
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

// maxPublishBodyBytes bounds a publish request body.
const maxPublishBodyBytes = 4 << 20

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

// NewHandler constructs an http.Handler serving the Seamline REST + resumable SSE API.
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
		"status":      "ok",
		"instance_id": h.broker.InstanceID(),
	})
}

func (h *handler) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
	}
	if r.Body != nil && r.Body != http.NoBody {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid JSON request body")
			return
		}
	}

	meta, err := h.broker.CreateSession(r.Context(), strings.TrimSpace(req.SessionID))
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, meta)
}

func (h *handler) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := h.broker.ListSessions(r.Context())
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (h *handler) handleInspectSession(w http.ResponseWriter, r *http.Request) {
	snap, ok, err := h.broker.Inspect(r.Context(), r.PathValue("id"))
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "session not found")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (h *handler) handlePublishEvent(w http.ResponseWriter, r *http.Request) {
	var req model.PublishRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPublishBodyBytes))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid JSON request body")
		return
	}
	res, err := h.broker.Publish(r.Context(), r.PathValue("id"), req)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handler) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, codeInternal, "streaming unsupported by response writer")
		return
	}
	afterEpoch, afterSeq, err := extractStreamCursor(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, err.Error())
		return
	}
	sub, err := h.broker.Subscribe(r.Context(), r.PathValue("id"), afterSeq, afterEpoch)
	if err != nil {
		writeBrokerError(w, err)
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
	h.streamSSELoop(r.Context(), w, flusher, sub)
}

func (h *handler) streamSSELoop(ctx context.Context, w io.Writer, flusher http.Flusher, sub *broker.Subscription) {
	ticker := time.NewTicker(h.keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case d, ok := <-sub.Events:
			if !ok {
				writeTerminalStreamError(w, flusher, sub.Err())
				return
			}
			if err := writeDelivery(w, flusher, d); err != nil {
				return
			}
		}
	}
}

// writeTerminalStreamError tells the client why the broker ended its subscription. The native
// EventSource then reconnects with Last-Event-ID; for insufficient_state that reconnect receives
// a cursor_reset because the cursor fell out of the retained window.
func writeTerminalStreamError(w io.Writer, flusher http.Flusher, err error) {
	var code string
	switch {
	case errors.Is(err, broker.ErrSlowSubscriber):
		code = "slow_subscriber"
	case errors.Is(err, broker.ErrInsufficientState):
		code = "insufficient_state"
	default:
		return
	}
	_, _ = fmt.Fprintf(w, "event: stream_error\ndata: {\"error\":%q}\n\n", code)
	flusher.Flush()
}

// formatDelivery renders one SSE frame. Business events carry id: <epoch>:<seq> and no event:
// line (EventSource dispatches them as "message"), so producer event types can never collide
// with control frame names. cursor_reset carries no id:.
func formatDelivery(d model.Delivery) (string, error) {
	if d.Kind == model.DeliveryCursorReset {
		data, err := json.Marshal(d.Reset)
		return fmt.Sprintf("event: cursor_reset\ndata: %s\n\n", data), err
	}
	data, err := json.Marshal(d.Event)
	return fmt.Sprintf("id: %s:%d\ndata: %s\n\n", d.Event.Epoch, d.Event.Seq, data), err
}

func writeDelivery(w io.Writer, flusher http.Flusher, d model.Delivery) error {
	frame, err := formatDelivery(d)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, frame); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// extractStreamCursor reads the resume cursor <epoch>:<seq> from the Last-Event-ID header, or
// else from ?after_seq=.
func extractStreamCursor(r *http.Request) (epoch string, seq uint64, err error) {
	raw := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if raw == "" {
		raw = strings.TrimSpace(r.URL.Query().Get("after_seq"))
	}
	if raw == "" {
		return "", 0, nil
	}
	return parseCursor(raw)
}

// parseCursor parses <epoch>:<seq>. A bare <seq> yields an empty epoch, which never matches a
// session stream.
func parseCursor(raw string) (epoch string, seq uint64, err error) {
	seqStr := raw
	if idx := strings.LastIndexByte(raw, ':'); idx != -1 {
		epoch, seqStr = raw[:idx], raw[idx+1:]
		if epoch == "" {
			return "", 0, errors.New("invalid cursor format")
		}
	}
	n, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid cursor seq: %w", err)
	}
	return epoch, n, nil
}
