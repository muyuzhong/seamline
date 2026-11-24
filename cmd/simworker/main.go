package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
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

const defaultSimText = "Seamline gives every event the next monotonic cursor <epoch>:<seq> and pins the active run, so a disconnect or an F5 in the middle of a long answer resumes exactly at the next event."

var errInterrupted = errors.New("interrupted")

type simConfig struct {
	base    string
	session string
	runID   string
	prompt  string
	text    string
	tools   int
	delay   time.Duration
}

func main() {
	var (
		serverURL = flag.String("server", "http://localhost:8080", "Seamline base URL")
		sessionID = flag.String("session", "demo-1", "target session ID")
		delay     = flag.Duration("delay", 30*time.Millisecond, "delay between streamed rounds of events")
		text      = flag.String("text", defaultSimText, "assistant text to stream as assistant_delta events")
		prompt    = flag.String("prompt", "Simulate a resumable AI assistant run.", "user_message content")
		runID     = flag.String("run-id", "", "run ID used in event payloads (auto-generated if empty)")
		tools     = flag.Int("tools", 0, "number of parallel tool calls whose JSON arguments stream interleaved with the text")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := simConfig{
		base:    strings.TrimRight(strings.TrimSpace(*serverURL), "/"),
		session: strings.TrimSpace(*sessionID),
		runID:   defaultString(strings.TrimSpace(*runID), newRunID()),
		prompt:  *prompt,
		text:    defaultString(*text, defaultSimText),
		tools:   max(*tools, 0),
		delay:   *delay,
	}
	if cfg.session == "" {
		logger.Error("session ID must not be empty")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	last, err := runSimulation(ctx, cfg, logger)
	if err != nil {
		logger.Error("simulation failed", "run_id", cfg.runID, "error", err)
		os.Exit(1)
	}
	logger.Info("run finished", "run_id", cfg.runID, "seamline_cursor", fmt.Sprintf("%s:%d", last.Epoch, last.Seq))
}

func defaultString(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

func newRunID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("run_%d", time.Now().UnixNano())
	}
	return "run_" + hex.EncodeToString(b[:])
}

// runSimulation publishes one run: run_started, the user message, interleaved assistant_delta and
// tool_call_delta events, and run_finished. On failure it ends the run with run_failed or
// run_interrupted so the session does not wait for the idle timeout.
func runSimulation(ctx context.Context, cfg simConfig, logger *slog.Logger) (model.PublishResult, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	pub := &publisher{client: client, endpoint: eventsURL(cfg.base, cfg.session), runID: cfg.runID}

	if _, err := pub.publish(ctx, model.TypeRunStarted, map[string]string{"run_id": cfg.runID}); err != nil {
		return model.PublishResult{}, err
	}
	if _, err := pub.publish(ctx, "user_message", map[string]string{"role": "user", "content": cfg.prompt}); err != nil {
		return model.PublishResult{}, pub.abort(err)
	}
	chunks, toolArgs := chunkText(cfg.text), buildToolArgs(cfg.tools)
	logger.Info("streaming run", "run_id", cfg.runID, "text_chunks", len(chunks), "tools", len(toolArgs), "delay", cfg.delay.String())
	if err := streamRun(ctx, pub, chunks, toolArgs, cfg.delay); err != nil {
		return model.PublishResult{}, pub.abort(err)
	}
	return pub.publish(ctx, model.TypeRunFinished, map[string]string{"run_id": cfg.runID})
}

func streamRun(ctx context.Context, pub *publisher, chunks []string, toolArgs [][]string, delay time.Duration) error {
	rounds := len(chunks)
	for _, parts := range toolArgs {
		rounds = max(rounds, len(parts))
	}
	for i := 0; i < rounds; i++ {
		if i > 0 && !sleepWithContext(ctx, delay) {
			return errInterrupted
		}
		if err := publishRound(ctx, pub, i, chunks, toolArgs); err != nil {
			return err
		}
	}
	return nil
}

// publishRound publishes the i-th text chunk and the i-th argument chunk of every tool call, so
// several logical streams interleave inside the single event sequence.
func publishRound(ctx context.Context, pub *publisher, i int, chunks []string, toolArgs [][]string) error {
	if i < len(chunks) {
		if _, err := pub.publish(ctx, "assistant_delta", map[string]string{"text": chunks[i]}); err != nil {
			return err
		}
	}
	for t, parts := range toolArgs {
		if i >= len(parts) {
			continue
		}
		payload := map[string]string{"call_id": fmt.Sprintf("call_%d", t+1), "name": "search", "args_delta": parts[i]}
		if _, err := pub.publish(ctx, "tool_call_delta", payload); err != nil {
			return err
		}
	}
	return nil
}

func buildToolArgs(n int) [][]string {
	out := make([][]string, n)
	for t := range out {
		args := fmt.Sprintf(`{"query":"seamline resume test %d","limit":%d}`, t+1, 5+t)
		out[t] = chunkFixedRunes([]rune(args), 6)
	}
	return out
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func eventsURL(base, sessionID string) string {
	return fmt.Sprintf("%s/v1/sessions/%s/events", base, url.PathEscape(sessionID))
}

func chunkText(text string) []string {
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}
	// For short strings, slice into 2-rune tokens so mid-stream disconnect tests have plenty of chunks.
	if len(runes) <= 120 {
		return chunkFixedRunes(runes, 2)
	}
	return chunkNaturalTokens(runes, 8)
}

func chunkFixedRunes(runes []rune, width int) []string {
	out := make([]string, 0, (len(runes)+width-1)/width)
	for i := 0; i < len(runes); i += width {
		end := min(i+width, len(runes))
		out = append(out, string(runes[i:end]))
	}
	return out
}

func chunkNaturalTokens(runes []rune, maxBytes int) []string {
	var chunks []string
	var cur strings.Builder
	for i, r := range runes {
		cur.WriteRune(r)
		if shouldFlushSimChunk(runes, i, cur.Len(), maxBytes) {
			chunks = append(chunks, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		chunks = append(chunks, cur.String())
	}
	return chunks
}

func shouldFlushSimChunk(runes []rune, idx, curLen, maxBytes int) bool {
	if unicode.IsSpace(runes[idx]) {
		return idx+1 < len(runes) && !unicode.IsSpace(runes[idx+1])
	}
	return curLen >= maxBytes
}
