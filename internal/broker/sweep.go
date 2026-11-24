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

// sweepOnce runs one maintenance pass: evict idle actors, keep the streams of the live ones
// alive (subscribers may be attached), then let storage apply its retention.
func (b *Broker) sweepOnce(now time.Time) {
	ctx := context.Background()
	for _, id := range b.evictIdleActors(now) {
		_ = b.store.Touch(ctx, id)
	}
	_ = b.store.Sweep(ctx, now)
}

// evictIdleActors drops idle actors and returns the IDs of the actors that stay.
func (b *Broker) evictIdleActors(now time.Time) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	live := make([]string, 0, len(b.actors))
	for id, actor := range b.actors {
		if b.isEvictableLocked(actor, now) {
			delete(b.actors, id)
			continue
		}
		live = append(live, id)
	}
	return live
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
