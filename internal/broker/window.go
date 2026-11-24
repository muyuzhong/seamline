package broker

import "time"

type windowEntry[V any] struct {
	value     V
	expiresAt time.Time
}

type windowKey struct {
	key       string
	expiresAt time.Time
}

// windowIndex is a time-bounded deduplication index: an entry stays visible for the window
// after its last insertion. Callers synchronize access (it lives under actor.mu).
type windowIndex[V any] struct {
	window time.Duration
	items  map[string]windowEntry[V]
	order  []windowKey
}

func newWindowIndex[V any](window time.Duration) *windowIndex[V] {
	return &windowIndex[V]{
		window: window,
		items:  make(map[string]windowEntry[V]),
	}
}

func (w *windowIndex[V]) get(key string, now time.Time) (V, bool) {
	e, ok := w.items[key]
	if !ok || !now.Before(e.expiresAt) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (w *windowIndex[V]) put(key string, value V, now time.Time) {
	w.prune(now)
	expiresAt := now.Add(w.window)
	w.items[key] = windowEntry[V]{value: value, expiresAt: expiresAt}
	w.order = append(w.order, windowKey{key: key, expiresAt: expiresAt})
}

// prune drops expired entries in insertion order. An order slot whose key was re-inserted later
// no longer matches the map entry's deadline and is skipped.
func (w *windowIndex[V]) prune(now time.Time) {
	n := 0
	for n < len(w.order) && !now.Before(w.order[n].expiresAt) {
		k := w.order[n]
		if e, ok := w.items[k.key]; ok && e.expiresAt.Equal(k.expiresAt) {
			delete(w.items, k.key)
		}
		n++
	}
	if n == 0 {
		return
	}
	clear(w.order[:n])
	w.order = w.order[n:]
}

func (w *windowIndex[V]) len() int {
	return len(w.items)
}
