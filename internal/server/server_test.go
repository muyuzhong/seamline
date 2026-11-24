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
	"github.com/muyuzhong/seamline/internal/model"
	"github.com/muyuzhong/seamline/internal/server"
	"github.com/muyuzhong/seamline/internal/storage"
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

func (c *sseClient) nextEvent(t *testing.T, timeout time.Duration) (sseFrame, model.OutEvent) {
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
		var ev model.OutEvent
		if err := json.Unmarshal([]byte(f.Data), &ev); err != nil {
			t.Fatalf("unmarshal OutEvent (%q): %v", f.Data, err)
		}
		return f, ev
	}
}

// newTestEnv starts a server whose session streams all get the given epoch.
func newTestEnv(t *testing.T, epoch string, bCfg broker.Config, sCfg server.Config) (*broker.Broker, *httptest.Server) {
	t.Helper()
	store := storage.NewMemoryStorage(storage.WithEpochSource(func() string { return epoch }))
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

func extractPayloadContent(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var p struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal payload content (%s): %v", string(raw), err)
	}
	return p.Content
}

func TestMidStreamDisconnectAndReconnectWithLastEventID(t *testing.T) {
	_, srv := newTestEnv(t, "ep1234", broker.Config{}, server.Config{
		KeepaliveInterval: 5 * time.Second,
		EnableDemo:        true,
	})

	sessionID := "resume-sess"

	// 1. Publish an initial user message record (Committed Offset = 1).
	var pubRes model.PublishResult
	doPostJSON(t, srv.URL+"/v1/sessions/"+sessionID+"/events", model.PublishRequest{
		Type:  "user_message",
		Delta: "Tell me about resumable SSE streams.",
	}, http.StatusOK, &pubRes)
	if pubRes.CommittedOffset != 1 {
		t.Fatalf("expected CommittedOffset=1, got %d", pubRes.CommittedOffset)
	}

	// 2. Connect Client 1 from offset 0.
	c1 := openSSEStream(t, srv.URL+"/v1/sessions/"+sessionID+"/stream", nil)
	if c1.corsOrigin != "*" {
		t.Fatalf("expected CORS header *, got %q", c1.corsOrigin)
	}

	connFrame := c1.nextFrame(t, 2*time.Second)
	if !strings.Contains(connFrame.Comment, "connected epoch=ep1234") {
		t.Fatalf("expected connected epoch comment, got %+v", connFrame)
	}

	rec1Frame, rec1Ev := c1.nextEvent(t, 2*time.Second)
	if rec1Frame.Event != "record" || rec1Frame.ID != "ep1234:1" || rec1Ev.Offset != 1 {
		t.Fatalf("unexpected initial record frame: %+v / %+v", rec1Frame, rec1Ev)
	}
	savedLastEventID := rec1Frame.ID

	// Build a 60-chunk message.
	const totalChunks = 60
	chunks := make([]string, totalChunks)
	var fullBuilder strings.Builder
	for i := 0; i < totalChunks; i++ {
		chunks[i] = fmt.Sprintf("[tok-%02d]", i+1)
		fullBuilder.WriteString(chunks[i])
	}
	expectedFullText := fullBuilder.String()

	// 3. Publish chunks 1..15 while Client 1 is connected.
	for i := 0; i < 15; i++ {
		doPostJSON(t, srv.URL+"/v1/sessions/"+sessionID+"/events", model.PublishRequest{
			StreamID: "turn-1",
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    chunks[i],
			Fin:      false,
		}, http.StatusOK, nil)
	}

	var c1Partial strings.Builder
	for i := 0; i < 15; i++ {
		f, ev := c1.nextEvent(t, 2*time.Second)
		if f.Event != "delta" {
			t.Fatalf("expected delta event, got %s", f.Event)
		}
		if f.ID != "" {
			t.Fatalf("ephemeral delta frame MUST NOT set SSE id header, got %q", f.ID)
		}
		c1Partial.WriteString(ev.Delta)
	}
	if c1Partial.String() != strings.Join(chunks[:15], "") {
		t.Fatalf("client 1 partial mismatch: %q", c1Partial.String())
	}

	// 4. Disconnect Client 1 mid-stream!
	c1.Close()

	// 5. While Client 1 is offline, publish chunks 16..38 on the server.
	for i := 15; i < 38; i++ {
		doPostJSON(t, srv.URL+"/v1/sessions/"+sessionID+"/events", model.PublishRequest{
			StreamID: "turn-1",
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    chunks[i],
			Fin:      false,
		}, http.StatusOK, nil)
	}

	// 6. Reconnect as Client 2 passing Last-Event-ID: ep1234:1!
	c2 := openSSEStream(t, srv.URL+"/v1/sessions/"+sessionID+"/stream", map[string]string{
		"Last-Event-ID": savedLastEventID,
	})

	// Client 2 must NOT re-receive Record #1; its very first event must be inflight_snapshot at seq=38!
	snapFrame, snapEv := c2.nextEvent(t, 2*time.Second)
	if snapFrame.Event != "inflight_snapshot" {
		t.Fatalf("expected inflight_snapshot first after Last-Event-ID, got %q (%+v)", snapFrame.Event, snapEv)
	}
	if snapEv.Seq != 38 || snapEv.Delta != strings.Join(chunks[:38], "") {
		t.Fatalf("unexpected snapshot: seq=%d (want 38), delta=%q", snapEv.Seq, snapEv.Delta)
	}

	var reconstructed strings.Builder
	reconstructed.WriteString(snapEv.Delta)

	// 7. Publish remaining chunks 39..60 (chunk 60 has Fin=true).
	for i := 38; i < totalChunks; i++ {
		doPostJSON(t, srv.URL+"/v1/sessions/"+sessionID+"/events", model.PublishRequest{
			StreamID: "turn-1",
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    chunks[i],
			Fin:      i == totalChunks-1,
		}, http.StatusOK, nil)
	}

	// Read remaining 22 deltas (39..60) + 1 finalized compacted record!
	for i := 38; i < totalChunks; i++ {
		f, ev := c2.nextEvent(t, 2*time.Second)
		if f.Event != "delta" {
			t.Fatalf("expected delta at seq %d, got %q", i+1, f.Event)
		}
		if ev.Seq != uint64(i+1) {
			t.Fatalf("expected seq %d, got %d", i+1, ev.Seq)
		}
		reconstructed.WriteString(ev.Delta)
	}

	compactedFrame, compactedEv := c2.nextEvent(t, 2*time.Second)
	if compactedFrame.Event != "record" {
		t.Fatalf("expected finalized record frame, got %q", compactedFrame.Event)
	}
	if compactedFrame.ID != "ep1234:2" {
		t.Fatalf("expected SSE id=ep1234:2, got %q", compactedFrame.ID)
	}
	if compactedEv.Offset != 2 || compactedEv.StreamID != "turn-1" {
		t.Fatalf("unexpected compacted record metadata: %+v", compactedEv)
	}

	if reconstructed.String() != expectedFullText {
		t.Fatalf("reconstructed text from snapshot+deltas mismatch:\ngot:  %q\nwant: %q", reconstructed.String(), expectedFullText)
	}
	if gotPayload := extractPayloadContent(t, compactedEv.Payload); gotPayload != expectedFullText {
		t.Fatalf("compacted record payload mismatch:\ngot:  %q\nwant: %q", gotPayload, expectedFullText)
	}
}

func TestFullPageReloadMidStreamAndCompactionInspect(t *testing.T) {
	_, srv := newTestEnv(t, "f5epoch", broker.Config{}, server.Config{
		KeepaliveInterval: 5 * time.Second,
	})

	sessionID := "f5-reload-sess"
	const totalChunks = 40
	var expected strings.Builder
	chunks := make([]string, totalChunks)
	for i := 0; i < totalChunks; i++ {
		chunks[i] = fmt.Sprintf("word_%d ", i+1)
		expected.WriteString(chunks[i])
	}

	// Stream chunks 1..20 before any browser tab connects (or simulating F5 reload from after_offset=0).
	for i := 0; i < 20; i++ {
		doPostJSON(t, srv.URL+"/v1/sessions/"+sessionID+"/events", model.PublishRequest{
			StreamID: "msg-f5",
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    chunks[i],
			Fin:      false,
		}, http.StatusOK, nil)
	}

	// Inspect mid-stream: LastOffset must still be 0 while InFlight holds 20 coalesced chunks.
	var midSnap model.SessionSnapshot
	doGetJSON(t, srv.URL+"/v1/sessions/"+sessionID, http.StatusOK, &midSnap)

	if midSnap.Meta.LastOffset != 0 {
		t.Fatalf("expected LastOffset=0 mid-stream, got %d", midSnap.Meta.LastOffset)
	}
	if midSnap.Meta.State != model.StateRunning {
		t.Fatalf("expected StateRunning mid-stream, got %q", midSnap.Meta.State)
	}
	if midSnap.InFlight == nil || midSnap.InFlight.LastSeq != 20 {
		t.Fatalf("expected InFlight.LastSeq=20, got %+v", midSnap.InFlight)
	}

	// Connect F5 browser client with ?after_offset=0.
	f5Client := openSSEStream(t, srv.URL+"/v1/sessions/"+sessionID+"/stream?after_offset=0", nil)
	snapFrame, snapEv := f5Client.nextEvent(t, 2*time.Second)
	if snapFrame.Event != "inflight_snapshot" || snapEv.Seq != 20 {
		t.Fatalf("expected inflight_snapshot at seq=20, got %s seq=%d", snapFrame.Event, snapEv.Seq)
	}

	var assembled strings.Builder
	assembled.WriteString(snapEv.Delta)

	// Publish remaining chunks 21..40 with Fin=true on chunk 40.
	for i := 20; i < totalChunks; i++ {
		doPostJSON(t, srv.URL+"/v1/sessions/"+sessionID+"/events", model.PublishRequest{
			StreamID: "msg-f5",
			Seq:      uint64(i + 1),
			Type:     "assistant_message",
			Delta:    chunks[i],
			Fin:      i == totalChunks-1,
		}, http.StatusOK, nil)
	}

	for i := 20; i < totalChunks; i++ {
		f, ev := f5Client.nextEvent(t, 2*time.Second)
		if f.Event != "delta" {
			t.Fatalf("expected delta, got %s", f.Event)
		}
		assembled.WriteString(ev.Delta)
	}

	recFrame, recEv := f5Client.nextEvent(t, 2*time.Second)
	if recFrame.Event != "record" || recFrame.ID != "f5epoch:1" || recEv.Offset != 1 {
		t.Fatalf("expected compacted record #1, got %+v / %+v", recFrame, recEv)
	}
	if assembled.String() != expected.String() {
		t.Fatalf("assembled text mismatch: got %q, want %q", assembled.String(), expected.String())
	}

	// Inspect post-compaction: 40 chunks compacted into 1 single Record, InFlight == nil!
	var postSnap model.SessionSnapshot
	doGetJSON(t, srv.URL+"/v1/sessions/"+sessionID, http.StatusOK, &postSnap)

	if postSnap.Meta.LastOffset != 1 {
		t.Fatalf("expected LastOffset=1 after compaction, got %d", postSnap.Meta.LastOffset)
	}
	if postSnap.InFlight != nil {
		t.Fatalf("expected InFlight=nil after compaction, got %+v", postSnap.InFlight)
	}
	if postSnap.Meta.State != model.StateIdle {
		t.Fatalf("expected StateIdle after compaction, got %q", postSnap.Meta.State)
	}
}

func TestSSEKeepaliveHeartbeat(t *testing.T) {
	_, srv := newTestEnv(t, "hb0001", broker.Config{}, server.Config{
		KeepaliveInterval: 30 * time.Millisecond,
	})

	sc := openSSEStream(t, srv.URL+"/v1/sessions/idle-hb/stream", nil)

	first := sc.nextFrame(t, 1*time.Second)
	if !strings.Contains(first.Comment, "connected epoch=hb0001") {
		t.Fatalf("expected connected comment, got %+v", first)
	}

	keepalives := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for keepalives < 2 {
		rem := time.Until(deadline)
		if rem <= 0 {
			t.Fatalf("expected at least 2 keepalive frames, got %d", keepalives)
		}
		f := sc.nextFrame(t, rem)
		if f.Comment == "keepalive" {
			keepalives++
		}
	}
}

func TestErrorStatusMappingAndRESTEndpoints(t *testing.T) {
	_, srv := newTestEnv(t, "err999", broker.Config{InstanceID: "inst-err999"}, server.Config{
		EnableDemo: true,
	})

	// 1. Healthz reports the broker instance (session cursors carry per-session epochs).
	var hBody map[string]string
	doGetJSON(t, srv.URL+"/healthz", http.StatusOK, &hBody)
	if hBody["status"] != "ok" || hBody["instance_id"] != "inst-err999" {
		t.Fatalf("unexpected /healthz response: %+v", hBody)
	}

	// 2. Empty session list returns [] (not null) and missing session returns 404.
	rawList := doGet(t, srv.URL+"/v1/sessions", http.StatusOK, nil)
	if strings.TrimSpace(rawList) != "[]" {
		t.Fatalf("expected empty JSON array [], got %s", rawList)
	}

	_ = doGet(t, srv.URL+"/v1/sessions/does-not-exist", http.StatusNotFound, nil)

	// 3. POST /v1/sessions with optional empty body generates a session and returns 201.
	var created model.SessionMeta
	doPostJSON(t, srv.URL+"/v1/sessions", nil, http.StatusCreated, &created)
	if created.SessionID == "" {
		t.Fatalf("expected 201 Created with generated session_id, got %+v", created)
	}

	// 4. 400 Bad Request on empty atomic event (neither payload nor delta).
	doPostJSON(t, srv.URL+"/v1/sessions/s-err/events", model.PublishRequest{
		Type: "user_message",
	}, http.StatusBadRequest, nil)

	// 5. 400 Bad Request on sequence gap (starting at seq=3 instead of seq=1).
	doPostJSON(t, srv.URL+"/v1/sessions/s-err/events", model.PublishRequest{
		StreamID: "stream-a",
		Seq:      3,
		Type:     "assistant_message",
		Delta:    "oops",
	}, http.StatusBadRequest, nil)

	// Start valid stream-a at seq=1.
	doPostJSON(t, srv.URL+"/v1/sessions/s-err/events", model.PublishRequest{
		StreamID: "stream-a",
		Seq:      1,
		Type:     "assistant_message",
		Delta:    "hello ",
		Fin:      false,
	}, http.StatusOK, nil)

	// Sequence gap mid-stream (seq=1 -> seq=4) -> 400 Bad Request.
	doPostJSON(t, srv.URL+"/v1/sessions/s-err/events", model.PublishRequest{
		StreamID: "stream-a",
		Seq:      4,
		Type:     "assistant_message",
		Delta:    "gap",
	}, http.StatusBadRequest, nil)

	// 6. 409 Conflict when concurrent stream-b attempts to publish while stream-a is in-flight!
	doPostJSON(t, srv.URL+"/v1/sessions/s-err/events", model.PublishRequest{
		StreamID: "stream-b",
		Seq:      1,
		Type:     "assistant_message",
		Delta:    "intruder",
	}, http.StatusConflict, nil)

	// 7. Malformed Last-Event-ID returns 400 Bad Request.
	_ = doGet(t, srv.URL+"/v1/sessions/s-err/stream", http.StatusBadRequest, map[string]string{
		"Last-Event-ID": "not-a-valid-cursor",
	})

	// 8. Demo Playground GET / returns XSS-safe HTML and POST /v1/demo/simulate streams & compacts cleanly.
	idxHTML := doGet(t, srv.URL+"/", http.StatusOK, nil)
	if !strings.Contains(idxHTML, "Seamline Live Playground") {
		t.Fatalf("expected Playground HTML on GET /")
	}
	if strings.Contains(idxHTML, "innerHTML") {
		t.Fatalf("expected Playground HTML to avoid innerHTML assignments for DOM XSS safety")
	}
	if !strings.Contains(idxHTML, "stream_error") {
		t.Fatalf("expected Playground HTML to listen for stream_error SSE event")
	}
	if !strings.Contains(idxHTML, "after_offset=") {
		t.Fatalf("expected Playground HTML connectClientA to support ?after_offset= cursor fallback")
	}

	doPostJSON(t, srv.URL+"/v1/demo/simulate", map[string]any{
		"session_id": "sim-test",
		"delay_ms":   1,
	}, http.StatusAccepted, nil)

	// Wait for the simulated stream (delay_ms=1) to compact into offset=2 (user record #1 + compacted assistant record #2).
	deadline := time.Now().Add(5 * time.Second)
	for {
		var snap model.SessionSnapshot
		doGetJSON(t, srv.URL+"/v1/sessions/sim-test", http.StatusOK, &snap)
		if snap.Meta.LastOffset == 2 && snap.InFlight == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for /v1/demo/simulate compaction; last snapshot=%+v", snap)
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
	store := storage.NewMemoryStorage()
	brk := broker.New(store, broker.Config{
		SubscriberBufferSize: 2,
	})
	defer func() {
		_ = brk.Close()
	}()

	h := server.NewHandler(brk, server.Config{KeepaliveInterval: 10 * time.Second})

	rw := &stallResponseWriter{
		header:      make(http.Header),
		connectedCh: make(chan struct{}),
		unblockCh:   make(chan struct{}),
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/sessions/slow-sess/stream", nil)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		h.ServeHTTP(rw, req)
	}()

	// Wait until the SSE handler has subscribed and flushed `: connected`.
	<-rw.connectedCh

	// While rw.Flush() is stalled, overflow liveCh (buffer=2) + pump buffer with deltas.
	for i := 1; i <= 12; i++ {
		_, err := brk.Publish(context.Background(), "slow-sess", model.PublishRequest{
			StreamID: "flood-1",
			Seq:      uint64(i),
			Type:     "assistant_message",
			Delta:    fmt.Sprintf("chunk-%d ", i),
			Fin:      false,
		})
		if err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Unblock Flush() so handleStream drains any buffered events and sees ErrSlowSubscriber.
	close(rw.unblockCh)
	<-doneCh

	if rw.status != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rw.status)
	}
	wire := rw.buf.String()
	if !strings.Contains(wire, "event: stream_error\ndata: {\"error\":\"slow_subscriber\"}\n\n") {
		t.Fatalf("expected slow_subscriber SSE stream_error frame in output, got:\n%s", wire)
	}
	if strings.Contains(wire, "event: error\n") {
		t.Fatalf("must not emit colliding event: error frame, got:\n%s", wire)
	}
}

func TestEpochMismatchResetsCursorToZero(t *testing.T) {
	_, srv := newTestEnv(t, "newepoch", broker.Config{}, server.Config{
		KeepaliveInterval: 5 * time.Second,
	})

	doPostJSON(t, srv.URL+"/v1/sessions/ep-sess/events", model.PublishRequest{
		Type:  "user_message",
		Delta: "persisted across epoch check",
	}, http.StatusOK, nil)

	// Reconnect with stale epoch and high offset; broker must reset after_offset to 0 and replay offset 1.
	sc := openSSEStream(t, srv.URL+"/v1/sessions/ep-sess/stream?after_offset=99&epoch=old_stale_epoch", nil)
	f, ev := sc.nextEvent(t, 2*time.Second)
	if f.Event != "record" || ev.Offset != 1 {
		t.Fatalf("expected stale epoch cursor reset to replay offset 1, got %+v / %+v", f, ev)
	}
}
