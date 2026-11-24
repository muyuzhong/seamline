package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/muyuzhong/seamline/internal/broker"
)

// Machine-readable error codes returned as {"error": "...", "code": "..."}.
const (
	codeInvalidRequest         = "invalid_request"
	codeNotFound               = "not_found"
	codeConflict               = "conflict"
	codeEventTooLarge          = "event_too_large"
	codeActiveRunQuotaExceeded = "active_run_quota_exceeded"
	codeBrokerClosed           = "broker_closed"
	codeInternal               = "internal_error"
)

// brokerErrors maps broker errors to HTTP status and error code.
var brokerErrors = []struct {
	err    error
	status int
	code   string
}{
	{broker.ErrInvalidInput, http.StatusBadRequest, codeInvalidRequest},
	{broker.ErrEventTooLarge, http.StatusRequestEntityTooLarge, codeEventTooLarge},
	{broker.ErrActiveRunQuotaExceeded, http.StatusRequestEntityTooLarge, codeActiveRunQuotaExceeded},
	{broker.ErrBrokerClosed, http.StatusServiceUnavailable, codeBrokerClosed},
}

func writeBrokerError(w http.ResponseWriter, err error) {
	for _, m := range brokerErrors {
		if errors.Is(err, m.err) {
			writeError(w, m.status, m.code, err.Error())
			return
		}
	}
	writeError(w, http.StatusInternalServerError, codeInternal, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}
