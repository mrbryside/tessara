package tessara

import (
	"context"
	"time"

	"github.com/IBM/sarama"

	"github.com/mrbryside/tessara/mock"
)

type messageBenchMarkHandler struct{}

func (mh messageBenchMarkHandler) Perform(msg PerformMessage) error {
	time.Sleep(10 * time.Millisecond)
	return nil
}
func (mh messageBenchMarkHandler) Fallback(msg PerformMessage, err error) {
}

func prepareConsumer(bufferSize int, subqueue int) *consumerGroupHandler {
	cfg := NewConsumerConfig([]string{"fake broker"}, "fake-topic", "fake-group").
		WithBufferSize(uint64(bufferSize)).
		WithSubqueue(subqueue).
		WithCommitInterval(10 * time.Millisecond).
		WithBlockingInterval(10 * time.Millisecond).
		WithRoundRobinMode()

	return NewConsumer(cfg, messageBenchMarkHandler{}).consumerGroupHandler
}

func mockConsumerGroupSession(ctx context.Context, finishChan chan bool, targetOffset int64) mock.ConsumerGroupSession {
	claims := func() map[string][]int32 {
		return map[string][]int32{
			"fake-topic": {0, 1, 2},
		}
	}

	memberID := func() string {
		return "fake-member-id"
	}

	generationID := func() int32 {
		return 1
	}

	markOffset := func(topic string, partition int32, offset int64, metadata string) {
		if offset == targetOffset {
			finishChan <- true
		}
	}

	resetOffset := func(topic string, partition int32, offset int64, metadata string) {
		// do nothing
	}

	markMessage := func(msg *sarama.ConsumerMessage, metadata string) {
		// do nothing
	}

	commit := func() {
		// do nothing
	}

	context := func() context.Context {
		return ctx
	}

	return mock.NewConsumerGroupSession(claims, memberID, generationID, markOffset, resetOffset, markMessage, commit, context)
}

func mockConsumerGroupClaim(messageChannelBuffer int) mock.ConsumerGroupClaim {
	topicFunc := func() string {
		return "fake-topic"
	}

	partitionFunc := func() int32 {
		return 0
	}

	initialOffsetFunc := func() int64 {
		return 0
	}

	highWaterMarkOffsetFunc := func() int64 {
		return 0
	}

	messageChannel := func() chan *sarama.ConsumerMessage {
		return make(chan *sarama.ConsumerMessage, messageChannelBuffer)
	}

	return mock.NewConsumerGroupClaim(topicFunc, partitionFunc, initialOffsetFunc, highWaterMarkOffsetFunc, messageChannel())
}
