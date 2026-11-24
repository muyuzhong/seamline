package hotwindow

import (
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

func (s *stream) info(sessionID string) StreamInfo {
	earliest := s.top + 1
	if len(s.events) > 0 {
		earliest = s.events[0].Seq
	}
	return StreamInfo{
		SessionID:         sessionID,
		Epoch:             s.epoch,
		EarliestSeq:       earliest,
		LatestSeq:         s.top,
		ActiveRunStartSeq: s.pin,
		State:             s.state,
		RetainedBytes:     s.bytes,
		ActiveRunBytes:    s.activeBytes,
		CreatedAt:         s.createdAt,
		UpdatedAt:         s.updatedAt,
	}
}

// validTransition allows a run to start only from "no run" (at this event's seq) and to end
// only from the current run.
func (s *stream) validTransition(next, seq uint64) bool {
	if s.pin == 0 {
		return next == 0 || next == seq
	}
	return next == 0 || next == s.pin
}

// activeBytesAfter is the active run's retained size once c is appended (0 when c leaves no run).
func (s *stream) activeBytesAfter(c Commit) int64 {
	switch {
	case c.ActiveRunStartSeq == 0:
		return 0
	case c.ActiveRunStartSeq == s.pin:
		return s.activeBytes + eventSize(c.Event)
	default:
		return eventSize(c.Event)
	}
}

// check validates c without changing anything. A terminal event (ActiveRunStartSeq 0) is never
// rejected by the active-run quota, so a run can always end.
func (s *stream) check(c Commit, r Retention) error {
	switch {
	case c.Event.Epoch != s.epoch:
		return ErrEpochMismatch
	case c.Event.Seq != s.top+1:
		return ErrNonMonotonicSeq
	case !s.validTransition(c.ActiveRunStartSeq, c.Event.Seq):
		return ErrInvalidRunTransition
	case s.activeBytesAfter(c) > r.MaxActiveRunBytes:
		return ErrActiveRunQuotaExceeded
	default:
		return nil
	}
}

// apply appends a checked commit and moves the run bookkeeping.
func (s *stream) apply(c Commit) {
	ev := cloneEvent(c.Event)
	active := s.activeBytesAfter(c)
	if s.pin != 0 && c.ActiveRunStartSeq == 0 {
		s.lastRun = finishedRun{start: s.pin, end: ev.Seq, endedAt: ev.CreatedAt}
	}
	s.events = append(s.events, ev)
	s.bytes += eventSize(ev)
	s.activeBytes = active
	s.pin = c.ActiveRunStartSeq
	s.top = ev.Seq
	s.state = c.State
	s.updatedAt = ev.CreatedAt
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
