package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/muyuzhong/seamline/internal/model"
)

const defaultSimText = "Seamline streams ephemeral token deltas at memory speed while compacting finalized AI turns into a single durable record. Disconnect mid-stream or hit F5 in your browser: Last-Event-ID plus inflight_snapshot recovers the exact generation state without losing or duplicating a single character."

func main() {
	var (
		serverURL = flag.String("server", "http://localhost:8080", "Seamline base URL")
		sessionID = flag.String("session", "demo-1", "target session ID")
		delay     = flag.Duration("delay", 30*time.Millisecond, "delay between streamed token deltas")
		text      = flag.String("text", defaultSimText, "assistant text to stream as token deltas")
		prompt    = flag.String("prompt", "Simulate a resumable AI assistant stream.", "initial user message prompt")
		streamID  = flag.String("stream-id", "", "explicit stream ID (auto-generated if empty)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	base := strings.TrimRight(strings.TrimSpace(*serverURL), "/")
	sess := strings.TrimSpace(*sessionID)
	if sess == "" {
		logger.Error("session ID must not be empty")
		os.Exit(1)
	}

	sid := strings.TrimSpace(*streamID)
	if sid == "" {
		sid = newStreamID()
	}

	bodyText := *text
	if bodyText == "" {
		bodyText = defaultSimText
	}
	chunks := chunkText(bodyText)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{Timeout: 10 * time.Second}

	if err := createSession(ctx, client, base, sess); err != nil {
		logger.Error("failed to initialize session", "error", err)
		os.Exit(1)
	}

	userPayload, _ := json.Marshal(map[string]string{
		"role":    "user",
		"content": *prompt,
	})
	userRes, err := publishEvent(ctx, client, base, sess, model.PublishRequest{
		Type:    "user_message",
		Payload: userPayload,
	})
	if err != nil {
		logger.Error("failed to publish user prompt record", "error", err)
		os.Exit(1)
	}
	logger.Info("published user message record",
		"session", sess,
		"committed_offset", userRes.CommittedOffset,
		"epoch", userRes.Epoch,
	)

	logger.Info("streaming assistant deltas",
		"session", sess,
		"stream_id", sid,
		"chunks", len(chunks),
		"delay", delay.String(),
	)

	var finalRes model.PublishResult
	for i, delta := range chunks {
		if i > 0 && *delay > 0 {
			timer := time.NewTimer(*delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				logger.Warn("simworker interrupted mid-stream", "sent_chunks", i)
				os.Exit(130)
			case <-timer.C:
			}
		}

		res, err := publishEvent(ctx, client, base, sess, model.PublishRequest{
			StreamID: sid,
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    delta,
			Fin:      i == len(chunks)-1,
		})
		if err != nil {
			logger.Error("failed to publish delta", "seq", i+1, "error", err)
			os.Exit(1)
		}
		finalRes = res
	}

	logger.Info("stream completed and compacted",
		"session", sess,
		"stream_id", sid,
		"coalesced_chunks", len(chunks),
		"committed_offset", finalRes.CommittedOffset,
		"epoch", finalRes.Epoch,
	)
}

func createSession(ctx context.Context, client *http.Client, base, sessionID string) error {
	payload, _ := json.Marshal(map[string]string{"session_id": sessionID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/sessions", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST /v1/sessions status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

func publishEvent(ctx context.Context, client *http.Client, base, sessionID string, pub model.PublishRequest) (model.PublishResult, error) {
	body, err := json.Marshal(pub)
	if err != nil {
		return model.PublishResult{}, err
	}

	endpoint := fmt.Sprintf("%s/v1/sessions/%s/events", base, url.PathEscape(sessionID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return model.PublishResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return model.PublishResult{}, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return model.PublishResult{}, fmt.Errorf("POST %s status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var res model.PublishResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return model.PublishResult{}, err
	}
	return res, nil
}

func chunkText(text string) []string {
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}

	// For short strings, slice into 2-rune tokens so mid-stream disconnect tests have plenty of chunks.
	if len(runes) <= 120 {
		var out []string
		for i := 0; i < len(runes); i += 2 {
			end := i + 2
			if end > len(runes) {
				end = len(runes)
			}
			out = append(out, string(runes[i:end]))
		}
		return out
	}

	var chunks []string
	var cur strings.Builder
	for i, r := range runes {
		cur.WriteRune(r)
		flush := false
		if unicode.IsSpace(r) {
			if i+1 < len(runes) && !unicode.IsSpace(runes[i+1]) {
				flush = true
			}
		} else if cur.Len() >= 8 {
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

func newStreamID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("sim_%d", time.Now().UnixNano())
	}
	return "sim_" + hex.EncodeToString(b[:])
}
