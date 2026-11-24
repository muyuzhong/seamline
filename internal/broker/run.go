package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/muyuzhong/seamline/internal/model"
)

// idleTimeoutPayload is the payload of the run_failed event that ends a silent run.
var idleTimeoutPayload = json.RawMessage(`{"reason":"idle_timeout"}`)

// sealIdleRunLocked ends an active run whose producer has been silent for ActiveRunIdleTimeout
// by appending run_failed {"reason":"idle_timeout"} through the normal commit path. A failed
// append leaves the run active, so the next sweep retries.
func (b *Broker) sealIdleRunLocked(actor *sessionActor, now time.Time) {
	if actor.activeRunStartSeq == 0 || now.Sub(actor.runActiveAt) < b.cfg.ActiveRunIdleTimeout {
		return
	}
	_, _ = b.appendLocked(context.Background(), actor, model.TypeRunFailed, idleTimeoutPayload, now.UTC())
}
