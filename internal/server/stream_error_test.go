package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/muyuzhong/seamline/internal/broker"
)

func TestWriteTerminalStreamError(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{broker.ErrSlowSubscriber, "event: stream_error\ndata: {\"error\":\"slow_subscriber\"}\n\n"},
		{broker.ErrInsufficientState, "event: stream_error\ndata: {\"error\":\"insufficient_state\"}\n\n"},
		{context.Canceled, ""},
		{nil, ""},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		writeTerminalStreamError(rec, rec, tc.err)
		if got := rec.Body.String(); got != tc.want {
			t.Fatalf("writeTerminalStreamError(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}
