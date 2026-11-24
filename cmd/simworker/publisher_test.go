package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

// A publish whose response outlives the client timeout may already be committed, so it must be
// retried with the same idempotency key instead of failing the run.
func TestPublishRetriesClientTimeoutWithSameKey(t *testing.T) {
	var (
		mu   sync.Mutex
		keys []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req model.PublishRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		keys = append(keys, req.IdempotencyKey)
		first := len(keys) == 1
		mu.Unlock()
		if first {
			select { // answer only after the client has given up
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			return
		}
		_ = json.NewEncoder(w).Encode(model.PublishResult{Epoch: "ep", Seq: 1, Deduplicated: true})
	}))
	defer srv.Close()

	p := &publisher{client: &http.Client{Timeout: 50 * time.Millisecond}, endpoint: srv.URL, runID: "run_t", attempts: 3}
	res, err := p.publish(context.Background(), "msg", map[string]string{"text": "hi"})
	if err != nil {
		t.Fatalf("a publish that hit the client timeout must be retried, got %v", err)
	}
	if res.Seq != 1 || !res.Deduplicated {
		t.Fatalf("result = %+v, want the deduplicated original seq 1", res)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] != "run_t:1" || keys[1] != keys[0] {
		t.Fatalf("idempotency keys = %q, want two attempts with run_t:1", keys)
	}
}
