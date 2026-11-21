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

func (h *handler) handleDemoIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(demoHTML)
}

func (h *handler) handleDemoSimulate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
		DelayMs   int    `json:"delay_ms,omitempty"`
		Prompt    string `json:"prompt,omitempty"`
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
		sessionID = "playground-1"
	}

	delayMs := req.DelayMs
	if delayMs <= 0 {
		delayMs = 35
	} else if delayMs > 500 {
		delayMs = 500
	}

	prompt := strings.TrimSpace(req.Prompt)
	if prompt == "" {
		prompt = defaultDemoPrompt
	}

	snap, exists, err := h.broker.Inspect(r.Context(), sessionID)
	if err != nil {
		h.writeBrokerError(w, err)
		return
	}
	if exists && snap.InFlight != nil {
		writeError(w, http.StatusConflict, "another stream is already in-flight for this session")
		return
	}

	h.demoMu.Lock()
	if _, busy := h.activeDemos[sessionID]; busy {
		h.demoMu.Unlock()
		writeError(w, http.StatusConflict, "demo simulation already active for this session")
		return
	}
	h.activeDemos[sessionID] = struct{}{}
	h.demoMu.Unlock()

	userPayload, _ := json.Marshal(map[string]string{
		"role":    "user",
		"content": prompt,
	})
	if _, err := h.broker.Publish(r.Context(), sessionID, model.PublishRequest{
		Type:    "user_message",
		Payload: userPayload,
	}); err != nil {
		h.demoMu.Lock()
		delete(h.activeDemos, sessionID)
		h.demoMu.Unlock()
		h.writeBrokerError(w, err)
		return
	}

	chunks := splitIntoTokenChunks(defaultDemoMarkdown)
	streamID := generateID("stream_")
	interval := time.Duration(delayMs) * time.Millisecond

	go func() {
		defer func() {
			h.demoMu.Lock()
			delete(h.activeDemos, sessionID)
			h.demoMu.Unlock()
		}()

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
	}()

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "started",
		"session_id": sessionID,
		"stream_id":  streamID,
		"chunks":     len(chunks),
	})
}

// splitIntoTokenChunks divides text into ~100 realistic LLM-like token slices
// while preserving every character and whitespace byte-for-byte.
func splitIntoTokenChunks(text string) []string {
	var chunks []string
	var cur strings.Builder
	runes := []rune(text)

	for i, r := range runes {
		cur.WriteRune(r)
		flush := false
		if unicode.IsSpace(r) {
			if i+1 < len(runes) && !unicode.IsSpace(runes[i+1]) {
				flush = true
			}
		} else if cur.Len() >= 9 {
			flush = true
		}
		if flush {
			chunks = append(chunks, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		chunks = append(chunks, cur.String())
	}
	return chunks
}
