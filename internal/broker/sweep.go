package broker

import (
	"context"
	"time"
)

func (b *Broker) sweepLoop() {
	defer b.sweepWg.Done()
	ticker := time.NewTicker(b.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.doneCh:
			return
		case <-ticker.C:
			b.sweepOnce(time.Now())
		}
	}
}

// sweepOnce runs one maintenance pass: evict idle actors, then keep live sessions' streams alive
// and let storage apply its retention.
func (b *Broker) sweepOnce(now time.Time) {
	b.evictIdleActors(now)
	b.maintainStorage(now)
}

func (b *Broker) evictIdleActors(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	for id, actor := range b.actors {
		if b.isEvictableLocked(actor, now) {
			delete(b.actors, id)
		}
	}
}

// isEvictableLocked runs under b.mu and never blocks on actor.mu. An actor with an active run is
// kept because it owns the run's idle timer.
func (b *Broker) isEvictableLocked(actor *sessionActor, now time.Time) bool {
	if actor.pins.Load() > 0 || !actor.mu.TryLock() {
		return false
	}
	defer actor.mu.Unlock()
	idle := now.Sub(time.Unix(0, actor.lastActiveNano.Load())) > b.cfg.IdleSessionTTL
	return idle && actor.pins.Load() == 0 && len(actor.subscribers) == 0 && actor.activeRunStartSeq == 0
}

func (b *Broker) liveSessionIDs() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ids := make([]string, 0, len(b.actors))
	for id := range b.actors {
		ids = append(ids, id)
	}
	return ids
}

// maintainStorage refreshes the idle deadline of every live session's stream (so a stream with
// attached subscribers never expires underneath them) and then lets storage apply retention.
func (b *Broker) maintainStorage(now time.Time) {
	ctx := context.Background()
	for _, id := range b.liveSessionIDs() {
		_ = b.store.Touch(ctx, id)
	}
	_ = b.store.Sweep(ctx, now)
}
