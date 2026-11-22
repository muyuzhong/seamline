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

const defaultDemoPrompt = "Explain how Seamline decouples high-frequency LLM token deltas from durable WAL records and survives mid-stream F5 reloads."

const defaultDemoMarkdown = `### Dual-Speed Stream Compaction in Seamline

Traditional chat backends treat every streamed LLM token identically: either they **skip persistence** until completion (so hitting **F5** mid-response loses everything generated so far), or they **write every token chunk** into Redis Streams / Postgres (causing massive 100x write amplification for a single paragraph).

**Seamline separates ephemeral token velocity from durable state:**
1. **Ephemeral In-Flight Coalescing (RAM):** Token deltas (` + "`seq=1..N`" + `) append into an in-memory buffer on the session actor and fan out immediately to live SSE subscribers with **no ` + "`id:`" + ` header** and zero storage writes.
2. **Instant Mid-Stream Catch-Up:** When a client reconnects via ` + "`Last-Event-ID: <epoch>:<offset>`" + ` (or reloads from ` + "`offset=0`" + `), Seamline replays finalized ` + "`record`" + ` frames, emits one coalesced ` + "`inflight_snapshot`" + ` containing the exact half-finished text up to ` + "`last_seq`" + `, and seamlessly resumes live ` + "`delta`" + ` tailing without gaps or duplicates.
3. **Single-Record Compaction (` + "`fin=true`" + `):** Once generation completes, all 100+ chunks collapse into **1 durable ` + "`Record`" + `** advancing the session offset by ` + "`+1`" + `.`

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

func (h *handler) handleDemoSimulate(w http.ResponseWriter, r *http.Request) {
	params, err := decodeDemoSimulateParams(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	snap, exists, err := h.broker.Inspect(r.Context(), params.sessionID)
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	if exists && snap.InFlight != nil {
		writeError(w, http.StatusConflict, "another stream is already in-flight for this session")
		return
	}

	if !h.tryClaimDemoSlot(params.sessionID) {
		writeError(w, http.StatusConflict, "demo simulation already active for this session")
		return
	}

	userPayload, _ := json.Marshal(map[string]string{
		"role":    "user",
		"content": params.prompt,
	})
	if _, err := h.broker.Publish(r.Context(), params.sessionID, model.PublishRequest{
		Type:    "user_message",
		Payload: userPayload,
	}); err != nil {
		h.releaseDemoSlot(params.sessionID)
		h.writeBrokerError(w, err)
		return
	}

	chunks := splitIntoTokenChunks(defaultDemoMarkdown)
	streamID := generateID("stream_")
	go h.streamSimulatedTurn(params.sessionID, streamID, chunks, params.interval)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "started",
		"session_id": params.sessionID,
		"stream_id":  streamID,
		"chunks":     len(chunks),
	})
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

func (h *handler) streamSimulatedTurn(sessionID, streamID string, chunks []string, interval time.Duration) {
	defer h.releaseDemoSlot(sessionID)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ctx := context.Background()
	for i, chunk := range chunks {
		if i > 0 {
			<-ticker.C
		}
		_, err := h.broker.Publish(ctx, sessionID, model.PublishRequest{
			StreamID: streamID,
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    chunk,
			Fin:      i == len(chunks)-1,
		})
		if err != nil {
			return
		}
	}
}

// splitIntoTokenChunks divides text into ~100 realistic LLM-like token slices
// while preserving every character and whitespace byte-for-byte.
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
