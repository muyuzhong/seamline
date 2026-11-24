package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/muyuzhong/seamline/internal/model"
)

//go:embed demo.html
var demoHTML []byte

const defaultDemoPrompt = "Explain how Seamline resumes an AI stream after a disconnect or an F5 reload."

const defaultDemoMarkdown = `### One cursor for every event

Seamline gives every event of a session the next **monotonic cursor** ` + "`<epoch>:<seq>`" + `, stores it unchanged in a short hot window, and fans it out over plain SSE. Payloads are opaque: text deltas, half-finished tool-call JSON and status events are all just events.

1. **Exact resume:** every frame carries ` + "`id: <epoch>:<seq>`" + `, so a reconnect with ` + "`Last-Event-ID`" + ` (or ` + "`?after_seq=`" + `) continues at the next seq with no gap and no duplicate.
2. **Active run pinned:** between ` + "`run_started`" + ` and ` + "`run_finished`" + ` nothing is evicted, so an F5 in the middle of a long answer replays the whole run.
3. **Explicit gaps:** a cursor from another epoch or one that fell out of the window gets a ` + "`cursor_reset`" + ` first, never a silent skip.`

type demoSimulateParams struct {
	sessionID string
	prompt    string
	interval  time.Duration
}

func (h *handler) handleDemoIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(demoHTML)
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err) // only called with maps of strings and ints, which always marshal
	}
	return raw
}

func (h *handler) handleDemoSimulate(w http.ResponseWriter, r *http.Request) {
	params, err := decodeDemoSimulateParams(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid JSON request body")
		return
	}

	snap, exists, err := h.broker.Inspect(r.Context(), params.sessionID)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	if exists && snap.Meta.State == model.StateRunning {
		writeError(w, http.StatusConflict, codeConflict, "a run is already active for this session")
		return
	}
	if !h.tryClaimDemoSlot(params.sessionID) {
		writeError(w, http.StatusConflict, codeConflict, "demo simulation already active for this session")
		return
	}

	if err := h.startDemoRun(r.Context(), params); err != nil {
		h.releaseDemoSlot(params.sessionID)
		writeBrokerError(w, err)
		return
	}
	chunks := splitIntoTokenChunks(defaultDemoMarkdown)
	go h.streamSimulatedTurn(params.sessionID, chunks, params.interval)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "started",
		"session_id": params.sessionID,
		"chunks":     len(chunks),
	})
}

// startDemoRun opens the run explicitly and publishes the user prompt.
func (h *handler) startDemoRun(ctx context.Context, params demoSimulateParams) error {
	if _, err := h.broker.Publish(ctx, params.sessionID, model.PublishRequest{
		Type:    model.TypeRunStarted,
		Payload: mustJSON(map[string]string{"source": "demo"}),
	}); err != nil {
		return err
	}
	_, err := h.broker.Publish(ctx, params.sessionID, model.PublishRequest{
		Type:    "user_message",
		Payload: mustJSON(map[string]string{"role": "user", "content": params.prompt}),
	})
	return err
}

func decodeDemoSimulateParams(w http.ResponseWriter, r *http.Request) (demoSimulateParams, error) {
	var req struct {
		SessionID string `json:"session_id"`
		DelayMs   int    `json:"delay_ms,omitempty"`
		Prompt    string `json:"prompt,omitempty"`
	}
	if r.Body != nil && r.Body != http.NoBody {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			return demoSimulateParams{}, err
		}
	}

	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		sessionID = "playground-1"
	}
	prompt := strings.TrimSpace(req.Prompt)
	if prompt == "" {
		prompt = defaultDemoPrompt
	}
	return demoSimulateParams{
		sessionID: sessionID,
		prompt:    prompt,
		interval:  time.Duration(clampDelayMs(req.DelayMs)) * time.Millisecond,
	}, nil
}

func clampDelayMs(delayMs int) int {
	if delayMs <= 0 {
		return 35
	}
	if delayMs > 500 {
		return 500
	}
	return delayMs
}

func (h *handler) tryClaimDemoSlot(sessionID string) bool {
	h.demoMu.Lock()
	defer h.demoMu.Unlock()
	if _, busy := h.activeDemos[sessionID]; busy {
		return false
	}
	h.activeDemos[sessionID] = struct{}{}
	return true
}

func (h *handler) releaseDemoSlot(sessionID string) {
	h.demoMu.Lock()
	delete(h.activeDemos, sessionID)
	h.demoMu.Unlock()
}

// streamSimulatedTurn publishes one assistant_delta event per chunk and ends the run. If a
// publish fails, the run is ended with run_failed (terminal events are exempt from the active-run
// quota) instead of being left for the idle timeout.
func (h *handler) streamSimulatedTurn(sessionID string, chunks []string, interval time.Duration) {
	defer h.releaseDemoSlot(sessionID)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ctx := context.Background()
	for i, chunk := range chunks {
		if i > 0 {
			<-ticker.C
		}
		if _, err := h.broker.Publish(ctx, sessionID, model.PublishRequest{
			Type:    "assistant_delta",
			Payload: mustJSON(map[string]string{"text": chunk}),
		}); err != nil {
			h.endDemoRun(ctx, sessionID, model.TypeRunFailed, map[string]string{"reason": err.Error()})
			return
		}
	}
	h.endDemoRun(ctx, sessionID, model.TypeRunFinished, map[string]int{"chunks": len(chunks)})
}

func (h *handler) endDemoRun(ctx context.Context, sessionID, eventType string, payload any) {
	_, _ = h.broker.Publish(ctx, sessionID, model.PublishRequest{Type: eventType, Payload: mustJSON(payload)})
}

// splitIntoTokenChunks divides text into realistic LLM-like token slices while preserving every
// character and whitespace byte-for-byte.
func splitIntoTokenChunks(text string) []string {
	var chunks []string
	var cur strings.Builder
	runes := []rune(text)

	for i, r := range runes {
		cur.WriteRune(r)
		if shouldFlushDemoChunk(runes, i, cur.Len()) {
			chunks = append(chunks, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		chunks = append(chunks, cur.String())
	}
	return chunks
}

func shouldFlushDemoChunk(runes []rune, idx, curLen int) bool {
	if unicode.IsSpace(runes[idx]) {
		return idx+1 < len(runes) && !unicode.IsSpace(runes[idx+1])
	}
	return curLen >= 9
}
