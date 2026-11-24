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

// sweepOnce runs one maintenance pass: evict idle actors, then keep live sessions' streams
// alive and let storage apply its retention.
func (b *Broker) sweepOnce(now time.Time) {
	b.evictIdleActors(now)
	b.maintainStorage(now)
}

func (b *Broker) evictIdleActors(now time.Time) {
	var toFlush []*sessionActor

	b.mu.Lock()
	if !b.closed {
		for id, actor := range b.actors {
			if dirty := b.tryEvictOrStageFlushLocked(id, actor, now); dirty != nil {
				toFlush = append(toFlush, dirty)
			}
		}
	}
	b.mu.Unlock()

	for _, actor := range toFlush {
		b.flushDirtyActorMeta(actor)
	}
}

func (b *Broker) tryEvictOrStageFlushLocked(id string, actor *sessionActor, now time.Time) *sessionActor {
	if actor.pins.Load() > 0 || !actor.mu.TryLock() {
		return nil
	}
	defer actor.mu.Unlock()

	lastActive := time.Unix(0, actor.lastActiveNano.Load())
	isIdle := actor.pins.Load() == 0 && len(actor.subscribers) == 0 && actor.inFlight == nil
	if !isIdle || now.Sub(lastActive) <= b.cfg.IdleSessionTTL {
		return nil
	}

	if !actor.metaPersisted && !actor.metaFlushTried {
		actor.pins.Add(1)
		return actor
	}

	delete(b.actors, id)
	return nil
}

func (b *Broker) flushDirtyActorMeta(actor *sessionActor) {
	defer actor.pins.Add(-1)
	actor.mu.Lock()
	defer actor.mu.Unlock()

	if actor.metaPersisted {
		return
	}
	actor.metaFlushTried = true
	if err := b.store.SaveMeta(context.Background(), actor.meta); err == nil {
		actor.metaPersisted = true
	}
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
