package tessara

import (
	"log"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/mrbryside/tessara/metric"
)

// retryableHandler configures the consumer to start consuming from the newest offset.
type retryableHandler struct {
	messageHandler  messageHandler
	maxRetry        int
	retryMultiplier float64
	fromSubqueueID  int
}

// newRetryableHandler configures the consumer to start consuming from the newest offset.
func newRetryableHandler(messageHandler messageHandler, maxRetry int, retryMultiplier float64) retryableHandler {
	return retryableHandler{
		messageHandler:  messageHandler,
		maxRetry:        maxRetry,
		retryMultiplier: retryMultiplier,
	}
}

// retryableHandler configures the consumer to start consuming from the newest offset.
func (h retryableHandler) Perform(pm PerformMessage) error {
	switch h.maxRetry {
	case 0:
		return h.performWithoutRetry(pm)
	default:
		return h.performWithRetry(pm)
	}
}

// Fallback configures the consumer to start consuming from the newest offset.
func (h retryableHandler) Fallback(pm PerformMessage, err error) {
	h.messageHandler.Fallback(pm, err)
}

// performWithoutRetry configures the consumer to start consuming from the newest offset.
func (h retryableHandler) performWithoutRetry(pm PerformMessage) error {
	return h.messageHandler.Perform(pm)
}

// performWithRetry configures the consumer to start consuming from the newest offset.
func (h retryableHandler) performWithRetry(pm PerformMessage) error {
	backoffFormula := backoff.NewExponentialBackOff()
	backoffFormula.Multiplier = h.retryMultiplier
	backOffWithMaxRetries := backoff.WithMaxRetries(backoffFormula, uint64(h.maxRetry))
	notify := func(err error, nextWait time.Duration) {
		log.Printf("perform failed: %v — retrying in %s", err, nextWait)
	}

	op := func() error {
		err := h.messageHandler.Perform(pm)
		if err != nil {
			metric.IncrementSubqueueMessageErrorCount(h.fromSubqueueID)
		}
		return err
	}
	if err := backoff.RetryNotify(op, backOffWithMaxRetries, notify); err != nil {
		return err
	}

	return nil
}

// withFromSubqueueID configures the consumer to start consuming from the newest offset.
func (h retryableHandler) withFromSubqueueID(subqueueID int) retryableHandler {
	h.fromSubqueueID = subqueueID
	return h
}
