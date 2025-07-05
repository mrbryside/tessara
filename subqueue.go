package tessara

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"

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
}

// newSubqueue creates a new subqueue instance
func newSubqueue(ctx context.Context, id int, rh retryableHandler, memoryBufferSize uint64) *subqueue {
	subqueueChannelBufferSize := memoryBufferSize
	sq := &subqueue{
		id:               id,
		receiver:         make(chan subqueueMessage, subqueueChannelBufferSize),
		retryableHandler: rh.withFromSubqueueID(id),
	}

	go func() {
		sq.startHandleMessage(ctx)
	}()

	// update metric
	metric.UpdateSubqueueChannelBufferSize(subqueueChannelBufferSize)

	return sq
}

// newSubqueues creates a new subqueue instances
func newSubqueues(ctx context.Context, rh retryableHandler, memoryBufferSize uint64, subqueueNumber int) []*subqueue {
	var sqs []*subqueue
	for i := range subqueueNumber {
		sqs = append(sqs, newSubqueue(ctx, i+1, rh, memoryBufferSize))
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
		default:
			time.Sleep(10 * time.Millisecond)
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
			fmt.Println("handle message msg:", string(msg.consumerMessage.Value))
			metric.IncrementSubqueueMessageProcessingCount(s.id)

			// perform
			err := s.retryableHandler.Perform(toPerformMessage(msg.consumerMessage))
			if err != nil {
				s.retryableHandler.Fallback(toPerformMessage(msg.consumerMessage), err)
				continue
			}
			msg.messageBuffer.MarkSuccess()
			fmt.Println("mark success! msg:", string(msg.consumerMessage.Value))

			elapse := time.Since(start)
			metric.UpdateSubqueueMessageProcessingTime(elapse)
			metric.IncrementSubqueueMessageProcessedCount(s.id)
			metric.DecrementSubqueueMessageProcessingCount(s.id)
		}
	}
}

// Close closes the subqueue receiver channel
func (s *subqueue) Close() {
	close(s.receiver)
}
