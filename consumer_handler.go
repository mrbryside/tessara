package tessara

import (
	"errors"

	"github.com/IBM/sarama"

	"github.com/mrbryside/tessara/logger"
	"github.com/mrbryside/tessara/metric"
)

// customerHandler is a struct that implements the sarama.ConsumerGroupHandler interface
type consumerHandler struct {
	messageHandler messageHandler
	errorHandler   errorHandler
	consumerConfig consumerConfig
}

// newConsumerGroupHandler creates a new consumer handler
func newConsumerGroupHandler(mh messageHandler, eh errorHandler, cfg consumerConfig) *consumerHandler {
	ch := &consumerHandler{
		messageHandler: mh,
		errorHandler:   eh,
		consumerConfig: cfg,
	}

	return ch
}

// Setup is called when the consumer is initialized
func (ch *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	logger.Debug().
		Any("session", session).
		Msg("consumer handler setup called")
	return nil
}

// Cleanup is called when the consumer is closed
func (ch *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	logger.Debug().
		Any("session", session).
		Msg("consumer handler cleanup called")
	metric.ClearMetrics()
	return nil
}

// ConsumeClaim this is main consume loop will call automatically by sarama when consumer receives a message
// it's run in multiple goroutines by sarama)
func (ch *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// create channel for receive error from comitter it's should be here because consumeClaim is run in multiple goroutine
	commitGiveUpErrorChan := make(chan error)
	mb := newMemoryBuffer(session.Context(), ch.consumerConfig.bufferSize, ch.consumerConfig.waterMarkUpdateInterval, ch.consumerConfig.pushMessageBlockingInterval)
	cm := newCommitter(session.Context(), commitGiveUpErrorChan, ch.errorHandler, mb, session, claim, ch.consumerConfig.commitInterval, ch.consumerConfig.commitGiveUpInterval, ch.consumerConfig.commitGiveUpTime, ch.consumerConfig.pushMessageBlockingInterval)
	rh := newRetryableHandler(ch.messageHandler, ch.consumerConfig.maxRetry, ch.consumerConfig.retryMultiplier)
	sqs := newSubqueues(session.Context(), rh, ch.consumerConfig.bufferSize, ch.consumerConfig.pushMessageBlockingInterval, ch.consumerConfig.subqueueNumber)
	sqq := newSubqueueQualifier(session.Context(), sqs, ch.consumerConfig.subqueueMode, ch.consumerConfig.bufferSize, ch.consumerConfig.pushMessageBlockingInterval)
	ort := newOrchestrator(session.Context(), mb, sqq, cm, ch.consumerConfig.bufferSize, ch.consumerConfig.pushMessageBlockingInterval)

	// consume message from channel and push message to orchestrator
	for {
		select {
		case <-session.Context().Done():
			return nil

		case errFromChan := <-commitGiveUpErrorChan:
			// return error to retry message that exceed commit give up time
			return errors.Join(errFromChan, errors.New("skip processing message due to commit exceed give up time."))

		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			ort.Push(session.Context(), msg)
		}
	}
}
