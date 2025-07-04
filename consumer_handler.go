package tessara

import (
	"errors"
	"log"
	"sync"

	"github.com/IBM/sarama"

	"github.com/mrbryside/tessara/metric"
)

// customerHandler is a struct that implements the sarama.ConsumerGroupHandler interface
type consumerHandler struct {
	messageHandler messageHandler
	errorHandler   errorHandler
	consumerConfig consumerConfig

	// commitGiveUpErrorChan is a channel that receive error from comiiter when exceed commit give up time
	commitGiveUpErrorChan chan error

	closeOnce sync.Once
}

// newConsumerGroupHandler creates a new consumer handler
func newConsumerGroupHandler(mh messageHandler, eh errorHandler, cfg consumerConfig) *consumerHandler {
	ch := &consumerHandler{
		messageHandler:        mh,
		errorHandler:          eh,
		consumerConfig:        cfg,
		commitGiveUpErrorChan: make(chan error),
	}

	return ch
}

// Setup is called when the consumer is initialized
func (ch *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Println("Setup called")
	return nil
}

// Cleanup is called when the consumer is closed
func (ch *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Println("Cleanup called")
	metric.ClearMetrics()
	return nil
}

// ConsumeClaim this is main consume loop will call automatically by sarama when consumer receives a message
func (ch *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// prepare orchestrator
	mb := newMemoryBuffer(session.Context(), ch.consumerConfig.bufferSize, ch.consumerConfig.waterMarkUpdateInterval, ch.consumerConfig.pushMessageBlockingInterval)
	cm := newCommitter(session.Context(), ch.commitGiveUpErrorChan, ch.errorHandler, mb, session, claim, ch.consumerConfig.commitInterval, ch.consumerConfig.commitGiveUpInterval, ch.consumerConfig.commitGiveUpTime)
	rh := newRetryableHandler(ch.messageHandler, ch.consumerConfig.maxRetry, ch.consumerConfig.retryMultiplier)
	sqs := newSubqueues(session.Context(), rh, ch.consumerConfig.bufferSize, ch.consumerConfig.subqueueNumber)
	sqq := newSubqueueQualifier(session.Context(), sqs, ch.consumerConfig.subqueueMode, ch.consumerConfig.bufferSize)
	ort := newOrchestrator(session.Context(), mb, sqq, cm, ch.consumerConfig.bufferSize)

	// consume message from channel and push message to orchestrator
	for {
		select {
		case <-session.Context().Done():
			return nil

		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			ort.Push(session.Context(), msg)

		case errFromChan := <-ch.commitGiveUpErrorChan:
			return errors.Join(errFromChan, errors.New("[consumerHandler.ConsumeClaim]: skip processing message due to commit exceed give up time."))
		}
	}
}
