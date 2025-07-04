package tessara

import (
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/net/context"
)

// orchestrator represents a orchestrator for managing message through subqueue, memory buffer, comitter.
type orchestrator struct {
	receiver          chan *sarama.ConsumerMessage
	memoryBuffer      *memoryBuffer
	subqueueQualifier *subqueueQualifier
	committer         *committer

	pushMessageBlockingInterval time.Duration
}

// newOrchestrator creates a new orchestrator instance.
func newOrchestrator(
	ctx context.Context,
	mb *memoryBuffer,
	sq *subqueueQualifier,
	cm *committer,
	memoryBufferSize uint64,
	pushMessageBlockingInterval time.Duration,
) *orchestrator {
	orchestratorChannelBufferSize := memoryBufferSize
	o := &orchestrator{
		receiver:                    make(chan *sarama.ConsumerMessage, orchestratorChannelBufferSize),
		memoryBuffer:                mb,
		subqueueQualifier:           sq,
		committer:                   cm,
		pushMessageBlockingInterval: pushMessageBlockingInterval,
	}
	go func() {
		o.startReceive(ctx)
	}()

	return o
}

// Push pushes a message to the orchestrator
func (o *orchestrator) Push(ctx context.Context, msg *sarama.ConsumerMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case o.receiver <- msg:
			return
		default:
			time.Sleep(o.pushMessageBlockingInterval)
		}
	}
}

// startReceive starts receiving messages from the receiver channel push in message buffer then qualify to subqueue
func (o *orchestrator) startReceive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-o.receiver:
			if !ok {
				return
			}
			msb := newMessageBuffer(msg.Offset)

			// push to memory buffer, this may block if buffer is full
			// once committer commit some messages this will unblock
			o.memoryBuffer.Push(ctx, msb)

			// push to subqueue qualifier after memory buffer push success
			o.subqueueQualifier.Push(ctx, subqueueMessage{
				consumerMessage: msg,
				messageBuffer:   msb,
			})
		}
	}
}
