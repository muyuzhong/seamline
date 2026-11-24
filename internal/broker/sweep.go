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

// sweepOnce runs one maintenance pass: end silent runs, evict idle actors, then keep live
// sessions' streams alive and let storage apply its retention.
func (b *Broker) sweepOnce(now time.Time) {
	b.sealIdleRuns(now)
	b.evictIdleActors(now)
	b.maintainStorage(now)
}

// pinAllActors pins every live actor under b.mu.RLock without touching actor.mu.
func (b *Broker) pinAllActors() []*sessionActor {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil
	}
	out := make([]*sessionActor, 0, len(b.actors))
	for _, actor := range b.actors {
		actor.pins.Add(1)
		out = append(out, actor)
	}
	return out
}

// sealIdleRuns ends silent runs. It runs outside b.mu and skips busy actors (TryLock); a storage
// failure leaves the run active so the next tick retries.
func (b *Broker) sealIdleRuns(now time.Time) {
	for _, actor := range b.pinAllActors() {
		if actor.mu.TryLock() {
			_ = b.sealIdleRunLocked(actor, now)
			actor.mu.Unlock()
		}
		// Release the sweeper's pin without touch(): sweeping must not keep sessions alive.
		actor.pins.Add(-1)
	}
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
