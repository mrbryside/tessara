package tessara

import (
	"context"
	"time"

	"github.com/IBM/sarama"

	"github.com/mrbryside/tessara/logger"
	"github.com/mrbryside/tessara/metric"
)

// subqueueMessage represents a message that subqueue received from the receiver channel
type subqueueMessage struct {
	consumerMessage *sarama.ConsumerMessage
	messageBuffer   *messageBuffer
}

// subqueue represents a subqueue that receives messages from the receiver channel and push to subqueue handler
type subqueue struct {
	id               int
	receiver         chan subqueueMessage
	retryableHandler retryableHandler

	pushMessageBlockingInterval time.Duration
}

// newSubqueue creates a new subqueue instance
func newSubqueue(ctx context.Context,
	id int,
	rh retryableHandler,
	memoryBufferSize uint64,
	pushMessageBlockingInterval time.Duration,
) *subqueue {
	subqueueChannelBufferSize := memoryBufferSize
	sq := &subqueue{
		id:                          id,
		receiver:                    make(chan subqueueMessage, subqueueChannelBufferSize),
		retryableHandler:            rh.withFromSubqueueID(id),
		pushMessageBlockingInterval: pushMessageBlockingInterval,
	}

	go func() {
		sq.startHandleMessage(ctx)
	}()

	// update metric
	metric.UpdateSubqueueChannelBufferSize(subqueueChannelBufferSize)

	return sq
}

// newSubqueues creates a new subqueue instances
func newSubqueues(ctx context.Context,
	rh retryableHandler,
	memoryBufferSize uint64,
	pushMessageBlockingInterval time.Duration,
	subqueueNumber int,
) []*subqueue {
	var sqs []*subqueue
	for i := range subqueueNumber {
		sqs = append(sqs, newSubqueue(ctx, i+1, rh, memoryBufferSize, pushMessageBlockingInterval))
		// update metric
		metric.InitSubqueueMessageProcessingCount(i + 1)
		metric.InitSubqueueMessageProcessedCount(i + 1)
		metric.InitSubqueueMessageErrorCount(i + 1)
	}
	metric.SetSubqueueCount(subqueueNumber)
	return sqs
}

// Push pushes a subqueue message to the subqueue receiver channel
func (s *subqueue) Push(ctx context.Context, sqMsg subqueueMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case s.receiver <- sqMsg:
			return
		default:
			time.Sleep(s.pushMessageBlockingInterval)
		}
	}
}

// startHandleMessage starts handling messages from the subqueue receiver channel
func (s *subqueue) startHandleMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.receiver:
			if !ok {
				return
			}
			start := time.Now()
			metric.IncrementSubqueueMessageProcessingCount(s.id)

			logger.Debug().
				Any("message", msg.consumerMessage.Value).
				Msg("handling message")

			// perform
			err := s.retryableHandler.Perform(toPerformMessage(msg.consumerMessage))
			if err != nil {
				s.retryableHandler.Fallback(toPerformMessage(msg.consumerMessage), err)
				continue
			}
			msg.messageBuffer.MarkSuccess()
			logger.Debug().
				Any("message", msg.consumerMessage.Value).
				Msg("message proceeded and marked successfully")

			elapse := time.Since(start)
			metric.UpdateSubqueueMessageProcessingTime(elapse)
			metric.IncrementSubqueueMessageProcessedCount(s.id)
			metric.DecrementSubqueueMessageProcessingCount(s.id)
		}
	}
}
