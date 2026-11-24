package hotwindow

import (
	"encoding/json"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

// eventOverheadBytes approximates the fixed per-event bookkeeping cost so that floods of tiny
// events are still bounded by the byte limits.
const eventOverheadBytes = 64

func eventSize(ev model.Event) int64 {
	return int64(len(ev.Payload)+len(ev.Type)) + eventOverheadBytes
}

// finishedRun remembers the most recently finished run, which retention treats specially.
type finishedRun struct {
	start   uint64
	end     uint64
	endedAt time.Time
}

func (r finishedRun) contains(seq uint64) bool {
	return r.end != 0 && seq >= r.start && seq <= r.end
}

// stream is one session stream incarnation. All methods run under MemoryStorage.mu.
type stream struct {
	epoch       string
	top         uint64
	events      []model.Event
	bytes       int64
	activeBytes int64
	pin         uint64 // active_run_start_seq; 0 when no run is active
	lastRun     finishedRun
	state       model.SessionState
	createdAt   time.Time
	updatedAt   time.Time
	touchedAt   time.Time
}

func (s *stream) info(sessionID string) model.SessionMeta {
	earliest := s.top + 1
	if len(s.events) > 0 {
		earliest = s.events[0].Seq
	}
	return model.SessionMeta{
		SessionID:         sessionID,
		Epoch:             s.epoch,
		EarliestSeq:       earliest,
		LatestSeq:         s.top,
		ActiveRunStartSeq: s.pin,
		State:             s.state,
		CreatedAt:         s.createdAt,
		UpdatedAt:         s.updatedAt,
	}
}

// nextRun is the run state machine: only run_started while no run is active opens a run (at
// seq), a terminal event ends the active run, and every other event leaves the run as is.
func (s *stream) nextRun(eventType string, seq uint64) (uint64, model.SessionState) {
	running := s.pin != 0
	if eventType == model.TypeRunStarted && !running {
		return seq, model.StateRunning
	}
	if terminal, ok := model.TerminalState(eventType); ok && running {
		return 0, terminal
	}
	return s.pin, s.state
}

// activeBytesAfter is the active run's retained size once ev is appended with run pin (0 when ev
// leaves no run, so a terminal event is never rejected by the active-run quota).
func (s *stream) activeBytesAfter(ev model.Event, pin uint64) int64 {
	switch {
	case pin == 0:
		return 0
	case pin == s.pin:
		return s.activeBytes + eventSize(ev)
	default:
		return eventSize(ev)
	}
}

// appendEvent commits the next event with the run bookkeeping it implies. It changes nothing
// when the active-run quota rejects the event.
func (s *stream) appendEvent(eventType string, payload json.RawMessage, now time.Time, r Retention) (model.Event, error) {
	ev := model.Event{Seq: s.top + 1, Epoch: s.epoch, Type: eventType, Payload: payload, CreatedAt: now}
	pin, state := s.nextRun(eventType, ev.Seq)
	active := s.activeBytesAfter(ev, pin)
	if active > r.MaxActiveRunBytes {
		return model.Event{}, ErrActiveRunQuotaExceeded
	}
	if payload != nil {
		ev.Payload = append(json.RawMessage(nil), payload...)
	}
	if s.pin != 0 && pin == 0 {
		s.lastRun = finishedRun{start: s.pin, end: ev.Seq, endedAt: now}
	}
	s.events = append(s.events, ev)
	s.bytes += eventSize(ev)
	s.activeBytes = active
	s.pin, s.state, s.top, s.updatedAt = pin, state, ev.Seq, now
	return ev, nil
}

// evict drops the oldest evictable events. It stops at the first event it must keep, so the
// retained window is always contiguous.
func (s *stream) evict(now time.Time, r Retention) {
	n := 0
	for n < len(s.events) && s.evictable(n, now, r) {
		s.bytes -= eventSize(s.events[n])
		n++
	}
	if n == 0 {
		return
	}
	// Zero evicted slots so their payloads become garbage even if no later append reallocates.
	clear(s.events[:n])
	s.events = s.events[n:]
	if len(s.events) == 0 {
		s.events = nil
	}
}

// evictable applies the retention rules to the event at index i:
//   - the active run (seq >= pin) is never evicted;
//   - the latest finished run expires HistoryTTL after the run ended and ignores the byte limit;
//   - other history expires HistoryTTL after creation or when history exceeds MaxHistoryBytes,
//     but the newest event is always kept.
func (s *stream) evictable(i int, now time.Time, r Retention) bool {
	ev := s.events[i]
	if s.pin != 0 && ev.Seq >= s.pin {
		return false
	}
	if s.lastRun.contains(ev.Seq) {
		return now.Sub(s.lastRun.endedAt) >= r.HistoryTTL
	}
	if now.Sub(ev.CreatedAt) >= r.HistoryTTL {
		return true
	}
	return s.bytes-s.activeBytes > r.MaxHistoryBytes && i < len(s.events)-1
}
