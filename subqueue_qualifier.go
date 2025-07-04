package tessara

import (
	"context"
)

// qualifier is an interface that defines the Qualify method.
type qualifier interface {
	Qualify(key string, sqs []*subqueue) *subqueue
}

// subqueueQualifier represents a qualifier for a subqueue.
type subqueueQualifier struct {
	receiver  chan subqueueMessage
	qualifier qualifier
	subqueues []*subqueue
}

// newSubqueueQualifier creates a new SubqueueQualifier instance.
func newSubqueueQualifier(ctx context.Context, sqs []*subqueue, qualifierMode string, memoryBufferSize uint64) *subqueueQualifier {
	if len(sqs) == 0 {
		panic("subqueue list is empty")
	}
	subqueueQualifierChannelBufferSize := memoryBufferSize
	sq := &subqueueQualifier{
		receiver:  make(chan subqueueMessage, subqueueQualifierChannelBufferSize),
		qualifier: getQualifier(qualifierMode),
		subqueues: sqs,
	}

	go func() {
		sq.StartQualify(ctx)
	}()

	return sq
}

// StartQualify starts the qualifier process.
func (sq *subqueueQualifier) StartQualify(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case sqMsg, ok := <-sq.receiver:
			if !ok {
				return
			}
			targetSubqueue := sq.qualifier.Qualify(string(sqMsg.consumerMessage.Key), sq.subqueues)
			targetSubqueue.Push(ctx, sqMsg)
		}
	}
}

// Push pushes a message to the orchestrator
func (sq *subqueueQualifier) Push(ctx context.Context, sqMsg subqueueMessage) {
	select {
	case <-ctx.Done():
		return
	default:
		sq.receiver <- sqMsg
	}
}

func getQualifier(qualifierMode string) qualifier {
	switch qualifierMode {
	case "round_robin":
		return newRoundRobinQualifier()
	case "key_distribute":
		return newKeyDistributeQualifier()
	default:
		panic("invalid qualifier mode")
	}
}
