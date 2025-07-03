package tessara

import (
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"golang.org/x/net/context"
)

// orchestrator represents a orchestrator for managing message through subqueue, memory buffer, comitter.
type orchestrator struct {
	receiver          chan *sarama.ConsumerMessage
	memoryBuffer      *memoryBuffer
	subqueueQualifier *subqueueQualifier
	committer         *committer

	closeOnce sync.Once
}

// newOrchestrator creates a new orchestrator instance.
func newOrchestrator(
	ctx context.Context,
	mb *memoryBuffer,
	sq *subqueueQualifier,
	cm *committer,
	memoryBufferSize uint64,
) orchestrator {
	orchestratorChannelBufferSize := memoryBufferSize
	o := orchestrator{
		receiver:          make(chan *sarama.ConsumerMessage, orchestratorChannelBufferSize),
		memoryBuffer:      mb,
		subqueueQualifier: sq,
		committer:         cm,
	}
	go func() {
		o.startReceive(ctx)
	}()

	return o
}

// Push pushes a message to the orchestrator
func (o orchestrator) Push(ctx context.Context, msg *sarama.ConsumerMessage) {
	select {
	case <-ctx.Done():
		o.closeOnce.Do(func() {
			close(o.receiver)
			fmt.Println("session context done, stop push message")
		})
		return
	default:
		o.receiver <- msg
	}
}

// startReceive starts receiving messages from the receiver channel push in message buffer then qualify to subqueue
func (o orchestrator) startReceive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(o.receiver)
			fmt.Println("session context done, stop orchreatrator receive message")
			return
		case msg, ok := <-o.receiver:
			if !ok {
				fmt.Println("orchrestrator receiver channel closed")
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
