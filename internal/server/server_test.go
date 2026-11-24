package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/muyuzhong/seamline/internal/broker"
	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/server"
)

type sseFrame struct {
	Comment string
	ID      string
	Event   string
	Data    string
}

type sseClient struct {
	frames     chan sseFrame
	errCh      chan error
	cancel     context.CancelFunc
	body       io.Closer
	corsOrigin string
}

func openSSEStream(t *testing.T, url string, headers map[string]string) *sseClient {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed asynchronously when SSE stream reader exits
	if err != nil {
		cancel()
		t.Fatalf("GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		t.Fatalf("GET %s expected 200, got %d: %s", url, resp.StatusCode, string(raw))
	}

	sc := &sseClient{
		frames:     make(chan sseFrame, 256),
		errCh:      make(chan error, 1),
		cancel:     cancel,
		body:       resp.Body,
		corsOrigin: resp.Header.Get("Access-Control-Allow-Origin"),
	}

	go func() {
		defer close(sc.frames)
		defer func() {
			_ = resp.Body.Close()
		}()

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

		var (
			curID      string
			curEvent   string
			curComment []string
			curData    []string
		)

		flushFrame := func() {
			if curID == "" && curEvent == "" && len(curComment) == 0 && len(curData) == 0 {
				return
			}
			frame := sseFrame{
				Comment: strings.Join(curComment, "\n"),
				ID:      curID,
				Event:   curEvent,
				Data:    strings.Join(curData, "\n"),
			}
			curID = ""
			curEvent = ""
			curComment = nil
			curData = nil
			select {
			case sc.frames <- frame:
			case <-ctx.Done():
			}
		}

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				flushFrame()
				continue
			}
			if strings.HasPrefix(line, ":") {
				curComment = append(curComment, strings.TrimSpace(strings.TrimPrefix(line, ":")))
			} else if strings.HasPrefix(line, "id:") {
				curID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			} else if strings.HasPrefix(line, "event:") {
				curEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				curData = append(curData, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			sc.errCh <- err
		}
	}()

	t.Cleanup(func() {
		sc.Close()
	})

	return sc
}

func (c *sseClient) Close() {
	c.cancel()
	_ = c.body.Close()
}

func (c *sseClient) nextFrame(t *testing.T, timeout time.Duration) sseFrame {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case f, ok := <-c.frames:
		if !ok {
			select {
			case err := <-c.errCh:
				t.Fatalf("SSE stream closed unexpectedly with error: %v", err)
			default:
				t.Fatalf("SSE stream closed unexpectedly")
			}
		}
		return f
	case <-timer.C:
		t.Fatalf("timed out after %v waiting for SSE frame", timeout)
		return sseFrame{}
	}
}

func (c *sseClient) nextEvent(t *testing.T, timeout time.Duration) (sseFrame, model.Event) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rem := time.Until(deadline)
		if rem <= 0 {
			t.Fatalf("timed out after %v waiting for non-comment SSE event", timeout)
		}
		f := c.nextFrame(t, rem)
		if f.Event == "" && f.Data == "" {
			// Comment frame (: connected or : keepalive)
			continue
		}
		if f.Event != "" {
			t.Fatalf("expected a business event frame, got event: %s (%s)", f.Event, f.Data)
		}
		var ev model.Event
		if err := json.Unmarshal([]byte(f.Data), &ev); err != nil {
			t.Fatalf("unmarshal Event (%q): %v", f.Data, err)
		}
		return f, ev
	}
}

// newTestEnv starts a server whose session streams all get the given epoch.
func newTestEnv(t *testing.T, epoch string, bCfg broker.Config, sCfg server.Config, opts ...hotwindow.Option) (*broker.Broker, *httptest.Server) {
	t.Helper()
	store := hotwindow.NewMemoryStorage(append([]hotwindow.Option{hotwindow.WithEpochSource(func() string { return epoch })}, opts...)...)
	brk := broker.New(store, bCfg)
	srv := httptest.NewServer(server.NewHandler(brk, sCfg))
	t.Cleanup(func() {
		_ = brk.Close()
		srv.Close()
	})
	return brk, srv
}

func doPostJSON(t *testing.T, url string, body any, wantStatus int, dst any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		r = bytes.NewReader(raw)
	}
	resp, err := http.Post(url, "application/json", r)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read POST %s body: %v", url, err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d (want %d): %s", url, resp.StatusCode, wantStatus, strings.TrimSpace(string(rawResp)))
	}
	if dst != nil {
		if err := json.Unmarshal(rawResp, dst); err != nil {
			t.Fatalf("decode POST %s JSON (%s): %v", url, string(rawResp), err)
		}
	}
}

func doGet(t *testing.T, url string, wantStatus int, headers map[string]string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest GET %s: %v", url, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s body: %v", url, err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s status=%d (want %d): %s", url, resp.StatusCode, wantStatus, strings.TrimSpace(string(rawResp)))
	}
	return string(rawResp)
}

func doGetJSON(t *testing.T, url string, wantStatus int, dst any) {
	t.Helper()
	raw := doGet(t, url, wantStatus, nil)
	if dst != nil {
		if err := json.Unmarshal([]byte(raw), dst); err != nil {
			t.Fatalf("decode GET %s JSON (%s): %v", url, raw, err)
		}
	}
}

func doPostRaw(t *testing.T, url, body string, wantStatus int) map[string]string {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d (want %d): %v", url, resp.StatusCode, wantStatus, out)
	}
	return out
}

func ev(eventType string, payload any) model.PublishRequest {
	req := model.PublishRequest{Type: eventType}
	if payload != nil {
		raw, _ := json.Marshal(payload)
		req.Payload = raw
	}
	return req
}

func textEv(s string) model.PublishRequest {
	return ev("assistant_delta", map[string]string{"text": s})
}

func payloadText(t *testing.T, e model.Event) string {
	t.Helper()
	var p struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload (%s): %v", e.Payload, err)
	}
	return p.Text
}

func TestMidStreamDisconnectAndReconnectWithLastEventID(t *testing.T) {
	_, srv := newTestEnv(t, "ep1234", broker.Config{}, server.Config{KeepaliveInterval: 5 * time.Second})
	base := srv.URL + "/v1/sessions/resume-sess"

	var res model.PublishResult
	doPostJSON(t, base+"/events", ev(model.TypeRunStarted, nil), http.StatusOK, &res)
	if res.Seq != 1 || res.Epoch != "ep1234" {
		t.Fatalf("unexpected publish result %+v", res)
	}
	doPostJSON(t, base+"/events", ev("user_message", map[string]string{"role": "user", "content": "hi"}), http.StatusOK, nil)

	c1 := openSSEStream(t, base+"/stream", nil)
	if c1.corsOrigin != "*" {
		t.Fatalf("expected CORS header *, got %q", c1.corsOrigin)
	}
	if f := c1.nextFrame(t, 2*time.Second); !strings.Contains(f.Comment, "connected epoch=ep1234") {
		t.Fatalf("expected connected comment, got %+v", f)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if f, e := c1.nextEvent(t, 2*time.Second); f.ID != fmt.Sprintf("ep1234:%d", seq) || e.Seq != seq {
			t.Fatalf("frame %d: %+v / %+v", seq, f, e)
		}
	}

	const total = 60
	chunks := make([]string, total)
	for i := range chunks {
		chunks[i] = fmt.Sprintf("[tok-%02d]", i+1)
	}
	for i := 0; i < 15; i++ {
		doPostJSON(t, base+"/events", textEv(chunks[i]), http.StatusOK, nil)
	}
	var text strings.Builder
	var lastID string
	for i := 0; i < 15; i++ {
		f, e := c1.nextEvent(t, 2*time.Second)
		if f.ID != fmt.Sprintf("ep1234:%d", e.Seq) || e.Seq != uint64(3+i) {
			t.Fatalf("every event frame must carry id <epoch>:<seq>: %+v / %+v", f, e)
		}
		text.WriteString(payloadText(t, e))
		lastID = f.ID
	}
	c1.Close()

	// While the client is away, more events are published.
	for i := 15; i < 38; i++ {
		doPostJSON(t, base+"/events", textEv(chunks[i]), http.StatusOK, nil)
	}
	c2 := openSSEStream(t, base+"/stream", map[string]string{"Last-Event-ID": lastID})
	for i := 38; i < total; i++ {
		doPostJSON(t, base+"/events", textEv(chunks[i]), http.StatusOK, nil)
	}
	doPostJSON(t, base+"/events", ev(model.TypeRunFinished, nil), http.StatusOK, nil)

	// The reconnect resumes exactly at the next seq and sees every later event once.
	for seq := uint64(18); seq <= 63; seq++ {
		f, e := c2.nextEvent(t, 2*time.Second)
		if e.Seq != seq || f.ID != fmt.Sprintf("ep1234:%d", seq) {
			t.Fatalf("resumed stream: got seq %d (id %q), want %d", e.Seq, f.ID, seq)
		}
		if e.Type == "assistant_delta" {
			text.WriteString(payloadText(t, e))
		}
		if seq == 63 && e.Type != model.TypeRunFinished {
			t.Fatalf("last event must be run_finished, got %s", e.Type)
		}
	}
	if text.String() != strings.Join(chunks, "") {
		t.Fatalf("reconstructed text mismatch:\n got %q\nwant %q", text.String(), strings.Join(chunks, ""))
	}
}

func TestFreshPageLoadMidRunReplaysWholeRun(t *testing.T) {
	_, srv := newTestEnv(t, "f5epoch", broker.Config{}, server.Config{KeepaliveInterval: 5 * time.Second})
	base := srv.URL + "/v1/sessions/f5"

	doPostJSON(t, base+"/events", ev(model.TypeRunStarted, nil), http.StatusOK, nil)
	var want strings.Builder
	for i := 1; i <= 40; i++ {
		tok := fmt.Sprintf("word_%d ", i)
		want.WriteString(tok)
		if i <= 20 {
			doPostJSON(t, base+"/events", textEv(tok), http.StatusOK, nil)
		}
	}
	var mid model.SessionSnapshot
	doGetJSON(t, base, http.StatusOK, &mid)
	if mid.Meta.State != model.StateRunning || mid.Meta.ActiveRunStartSeq != 1 || mid.Meta.LatestSeq != 21 || mid.Meta.EarliestSeq != 1 {
		t.Fatalf("mid-run session: %+v", mid.Meta)
	}

	f5 := openSSEStream(t, base+"/stream", nil)
	var got strings.Builder
	for seq := uint64(1); seq <= 21; seq++ {
		f, e := f5.nextEvent(t, 2*time.Second)
		if e.Seq != seq || f.ID != fmt.Sprintf("f5epoch:%d", seq) {
			t.Fatalf("fresh load replay: got %+v / %+v, want seq %d", f, e, seq)
		}
		if seq > 1 {
			got.WriteString(payloadText(t, e))
		}
	}
	tokens := strings.SplitAfter(want.String(), " ")
	for i := 20; i < 40; i++ {
		doPostJSON(t, base+"/events", textEv(tokens[i]), http.StatusOK, nil)
	}
	doPostJSON(t, base+"/events", ev(model.TypeRunFinished, nil), http.StatusOK, nil)
	for seq := uint64(22); seq <= 41; seq++ {
		_, e := f5.nextEvent(t, 2*time.Second)
		if e.Seq != seq {
			t.Fatalf("live seq %d, want %d", e.Seq, seq)
		}
		got.WriteString(payloadText(t, e))
	}
	if f, e := f5.nextEvent(t, 2*time.Second); e.Type != model.TypeRunFinished || f.ID != "f5epoch:42" {
		t.Fatalf("expected run_finished as f5epoch:42, got %+v / %+v", f, e)
	}
	if got.String() != want.String() {
		t.Fatalf("assembled text mismatch: %q", got.String())
	}

	var post model.SessionSnapshot
	doGetJSON(t, base, http.StatusOK, &post)
	if post.Meta.State != model.StateCompleted || post.Meta.ActiveRunStartSeq != 0 || post.Meta.LatestSeq != 42 {
		t.Fatalf("after the run: %+v", post.Meta)
	}

	// The query-parameter cursor resumes the same way as Last-Event-ID.
	q := openSSEStream(t, base+"/stream?after_seq=f5epoch:40", nil)
	if _, e := q.nextEvent(t, 2*time.Second); e.Seq != 41 {
		t.Fatalf("?after_seq=f5epoch:40 must resume at 41, got %d", e.Seq)
	}
}

func TestSSEKeepaliveHeartbeat(t *testing.T) {
	_, srv := newTestEnv(t, "hb0001", broker.Config{}, server.Config{KeepaliveInterval: 30 * time.Millisecond})

	sc := openSSEStream(t, srv.URL+"/v1/sessions/idle-hb/stream", nil)
	if first := sc.nextFrame(t, time.Second); !strings.Contains(first.Comment, "connected epoch=hb0001") {
		t.Fatalf("expected connected comment, got %+v", first)
	}
	keepalives := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for keepalives < 2 {
		rem := time.Until(deadline)
		if rem <= 0 {
			t.Fatalf("expected at least 2 keepalive frames, got %d", keepalives)
		}
		if f := sc.nextFrame(t, rem); f.Comment == "keepalive" {
			keepalives++
		}
	}
}

func TestActiveRunQuotaAnswers413(t *testing.T) {
	_, srv := newTestEnv(t, "quota", broker.Config{}, server.Config{}, hotwindow.WithRetention(hotwindow.Retention{MaxActiveRunBytes: 2048}))
	events := srv.URL + "/v1/sessions/q/events"

	doPostJSON(t, events, ev(model.TypeRunStarted, nil), http.StatusOK, nil)
	big := `{"type":"assistant_delta","payload":"` + strings.Repeat("y", 900) + `"}`
	doPostRaw(t, events, big, http.StatusOK)
	doPostRaw(t, events, big, http.StatusOK)
	if out := doPostRaw(t, events, big, http.StatusRequestEntityTooLarge); out["code"] != "active_run_quota_exceeded" {
		t.Fatalf("active run quota: %v", out)
	}
	// The run can still be ended.
	doPostJSON(t, events, ev(model.TypeRunFinished, nil), http.StatusOK, nil)
}

func TestErrorStatusMappingAndRESTEndpoints(t *testing.T) {
	_, srv := newTestEnv(t, "err999", broker.Config{InstanceID: "inst-err999", MaxEventBytes: 1024}, server.Config{EnableDemo: true})
	api := srv.URL + "/v1/sessions"

	var health map[string]string
	doGetJSON(t, srv.URL+"/healthz", http.StatusOK, &health)
	if health["status"] != "ok" || health["instance_id"] != "inst-err999" {
		t.Fatalf("unexpected /healthz: %+v", health)
	}
	if raw := doGet(t, api, http.StatusOK, nil); strings.TrimSpace(raw) != "[]" {
		t.Fatalf("expected [], got %s", raw)
	}
	var notFound map[string]string
	doGetJSON(t, api+"/does-not-exist", http.StatusNotFound, &notFound)
	if notFound["code"] != "not_found" {
		t.Fatalf("404 body: %+v", notFound)
	}
	var created model.SessionMeta
	doPostJSON(t, api, nil, http.StatusCreated, &created)
	if created.SessionID == "" || created.Epoch != "err999" || created.State != model.StateIdle {
		t.Fatalf("created session: %+v", created)
	}

	events := api + "/s-err/events"
	codes := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"missing type", `{"payload":{}}`, http.StatusBadRequest, "invalid_request"},
		{"malformed JSON", `{"type":"x","payload":{`, http.StatusBadRequest, "invalid_request"},
		{"payload over MaxEventBytes", `{"type":"x","payload":"` + strings.Repeat("x", 2000) + `"}`, http.StatusRequestEntityTooLarge, "event_too_large"},
		{"body over the request limit", `{"type":"x","payload":"` + strings.Repeat("x", 40000) + `"}`, http.StatusRequestEntityTooLarge, "event_too_large"},
	}
	for _, tc := range codes {
		if out := doPostRaw(t, events, tc.body, tc.status); out["code"] != tc.code {
			t.Fatalf("%s: code %q, want %q (%v)", tc.name, out["code"], tc.code, out)
		}
	}

	_ = doGet(t, api+"/s-err/stream", http.StatusBadRequest, map[string]string{"Last-Event-ID": "not-a-valid-cursor"})

	page := doGet(t, srv.URL+"/", http.StatusOK, nil)
	for _, want := range []string{"Seamline Live Playground", "stream_error", "cursor_reset", "after_seq=", "onmessage"} {
		if !strings.Contains(page, want) {
			t.Fatalf("playground HTML must contain %q", want)
		}
	}
	if strings.Contains(page, "innerHTML") {
		t.Fatalf("playground HTML must not use innerHTML")
	}

	var sim struct {
		Chunks int `json:"chunks"`
	}
	doPostJSON(t, srv.URL+"/v1/demo/simulate", map[string]any{"session_id": "sim-test", "delay_ms": 1}, http.StatusAccepted, &sim)
	wantLatest := uint64(sim.Chunks + 3) // run_started + user_message + chunks + run_finished
	deadline := time.Now().Add(5 * time.Second)
	for {
		var snap model.SessionSnapshot
		doGetJSON(t, api+"/sim-test", http.StatusOK, &snap)
		if snap.Meta.State == model.StateCompleted && snap.Meta.LatestSeq == wantLatest {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("simulated run did not finish: %+v (want latest_seq %d)", snap.Meta, wantLatest)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func TestDemoRunEndsWhenPublishFails(t *testing.T) {
	// The active-run quota is far smaller than the simulated answer, so a publish fails mid-run.
	_, srv := newTestEnv(t, "demo", broker.Config{}, server.Config{EnableDemo: true}, hotwindow.WithRetention(hotwindow.Retention{MaxActiveRunBytes: 1500}))
	doPostJSON(t, srv.URL+"/v1/demo/simulate", map[string]any{"session_id": "sim-fail", "delay_ms": 1}, http.StatusAccepted, nil)

	deadline := time.Now().Add(3 * time.Second)
	for {
		var snap model.SessionSnapshot
		doGetJSON(t, srv.URL+"/v1/sessions/sim-fail", http.StatusOK, &snap)
		if snap.Meta.State == model.StateFailed && snap.Meta.ActiveRunStartSeq == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a failed demo run must end with run_failed instead of waiting for the idle timeout: %+v", snap.Meta)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

type stallResponseWriter struct {
	header      http.Header
	status      int
	connectedCh chan struct{}
	unblockCh   chan struct{}
	buf         bytes.Buffer
}

func (w *stallResponseWriter) Header() http.Header {
	return w.header
}

func (w *stallResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func (w *stallResponseWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *stallResponseWriter) Flush() {
	select {
	case <-w.connectedCh:
	default:
		close(w.connectedCh)
		<-w.unblockCh
	}
}

func TestSlowSubscriberEmitsSSEErrorFrame(t *testing.T) {
	brk := broker.New(hotwindow.NewMemoryStorage(), broker.Config{SubscriberBufferSize: 2})
	defer func() {
		_ = brk.Close()
	}()
	h := server.NewHandler(brk, server.Config{KeepaliveInterval: 10 * time.Second})

	rw := &stallResponseWriter{header: make(http.Header), connectedCh: make(chan struct{}), unblockCh: make(chan struct{})}
	req := httptest.NewRequest(http.MethodGet, "/v1/sessions/slow-sess/stream", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rw, req)
	}()
	<-rw.connectedCh

	// While Flush is stalled, overflow liveCh (buffer 2) and the pump with events.
	for i := 1; i <= 12; i++ {
		if _, err := brk.Publish(context.Background(), "slow-sess", textEv(fmt.Sprintf("chunk-%d ", i))); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	close(rw.unblockCh)
	<-done

	wire := rw.buf.String()
	if rw.status != http.StatusOK || !strings.Contains(wire, "event: stream_error\ndata: {\"error\":\"slow_subscriber\"}\n\n") {
		t.Fatalf("expected slow_subscriber stream_error frame (status %d), got:\n%s", rw.status, wire)
	}
	if strings.Contains(wire, "event: error\n") {
		t.Fatalf("must not emit a colliding event: error frame:\n%s", wire)
	}
}

func TestCursorResetFrameOnEpochMismatch(t *testing.T) {
	_, srv := newTestEnv(t, "newepoch", broker.Config{}, server.Config{KeepaliveInterval: 5 * time.Second})
	doPostJSON(t, srv.URL+"/v1/sessions/ep-sess/events", ev("user_message", map[string]string{"content": "kept"}), http.StatusOK, nil)

	sc := openSSEStream(t, srv.URL+"/v1/sessions/ep-sess/stream?after_seq=99&epoch=old_stale_epoch", nil)
	if f := sc.nextFrame(t, 2*time.Second); !strings.Contains(f.Comment, "connected epoch=newepoch") {
		t.Fatalf("expected connected comment with the session epoch, got %+v", f)
	}
	reset := sc.nextFrame(t, 2*time.Second)
	if reset.Event != "cursor_reset" || reset.ID != "" {
		t.Fatalf("expected cursor_reset frame without id, got %+v", reset)
	}
	var got model.CursorReset
	if err := json.Unmarshal([]byte(reset.Data), &got); err != nil {
		t.Fatalf("decode cursor_reset data %q: %v", reset.Data, err)
	}
	want := model.CursorReset{Reason: model.ResetEpochMismatch, RequestedEpoch: "old_stale_epoch", RequestedSeq: 99, CurrentEpoch: "newepoch", EarliestSeq: 1, LatestSeq: 1}
	if got != want {
		t.Fatalf("cursor_reset data = %+v, want %+v", got, want)
	}
	if f, e := sc.nextEvent(t, 2*time.Second); f.ID != "newepoch:1" || e.Seq != 1 {
		t.Fatalf("expected event newepoch:1 after the reset, got %+v / %+v", f, e)
	}
}

// gappyStore loses one event on reads so the broker detects a hole during catch-up replay.
type gappyStore struct {
	*hotwindow.MemoryStorage
	drop uint64
}

func (g *gappyStore) ReadAfter(ctx context.Context, id string, after uint64) ([]model.Event, hotwindow.StreamInfo, error) {
	events, info, err := g.MemoryStorage.ReadAfter(ctx, id, after)
	out := events[:0]
	for _, e := range events {
		if e.Seq != g.drop {
			out = append(out, e)
		}
	}
	return out, info, err
}

func TestInsufficientStateEmitsStreamErrorFrame(t *testing.T) {
	brk := broker.New(&gappyStore{MemoryStorage: hotwindow.NewMemoryStorage(), drop: 2}, broker.Config{})
	srv := httptest.NewServer(server.NewHandler(brk, server.Config{KeepaliveInterval: 5 * time.Second}))
	t.Cleanup(func() {
		_ = brk.Close()
		srv.Close()
	})
	for i := 1; i <= 3; i++ {
		doPostJSON(t, srv.URL+"/v1/sessions/gap/events", ev("msg", nil), http.StatusOK, nil)
	}

	sc := openSSEStream(t, srv.URL+"/v1/sessions/gap/stream", nil)
	var frames []string
	deadline := time.After(2 * time.Second)
	for open := true; open; {
		select {
		case f, ok := <-sc.frames:
			if ok && f.Data != "" {
				frames = append(frames, f.Event+"|"+f.ID)
			}
			open = ok
		case <-deadline:
			t.Fatalf("stream must end after insufficient_state; still open after %q", frames)
		}
	}
	// One business event frame (no event name, with an id), then stream_error without an id.
	if len(frames) != 2 || !strings.HasPrefix(frames[0], "|") || frames[0] == "|" || frames[1] != "stream_error|" {
		t.Fatalf("expected one event frame then stream_error, got %q", frames)
	}
}
