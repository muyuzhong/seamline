package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

// statusError is a non-2xx response from Seamline.
type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("status %d: %s", e.code, e.body)
}

// retryable reports whether a publish may be retried: transport errors and 5xx responses are,
// 4xx responses (invalid event, quota) are not.
func retryable(err error) bool {
	var se *statusError
	if errors.As(err, &se) {
		return se.code >= http.StatusInternalServerError
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// publisher posts the events of one run. Every event gets the idempotency key <run_id>:<n>, so a
// retry after a timeout can never create a duplicate event.
type publisher struct {
	client   *http.Client
	endpoint string
	runID    string
	attempts int
	n        int
}

func (p *publisher) publish(ctx context.Context, eventType string, payload any) (model.PublishResult, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return model.PublishResult{}, err
	}
	p.n++
	req := model.PublishRequest{Type: eventType, Payload: raw, IdempotencyKey: fmt.Sprintf("%s:%d", p.runID, p.n)}

	for attempt := 1; ; attempt++ {
		res, err := postEvent(ctx, p.client, p.endpoint, req)
		if err == nil || attempt >= p.attempts || !retryable(err) {
			return res, err
		}
		if !sleepWithContext(ctx, time.Duration(attempt)*100*time.Millisecond) {
			return model.PublishResult{}, errInterrupted
		}
	}
}

// abort ends the run after a failure so subscribers see a terminal event right away.
func (p *publisher) abort(cause error) error {
	eventType, reason := model.TypeRunFailed, cause.Error()
	if errors.Is(cause, errInterrupted) {
		eventType, reason = model.TypeRunInterrupted, "interrupted"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = p.publish(ctx, eventType, map[string]string{"run_id": p.runID, "reason": reason})
	return cause
}

func postEvent(ctx context.Context, client *http.Client, endpoint string, pub model.PublishRequest) (model.PublishResult, error) {
	body, err := json.Marshal(pub)
	if err != nil {
		return model.PublishResult{}, err
	}
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
		return model.PublishResult{}, &statusError{code: resp.StatusCode, body: strings.TrimSpace(string(raw))}
	}
	var res model.PublishResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return model.PublishResult{}, err
	}
	return res, nil
}
