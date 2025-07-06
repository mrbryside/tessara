package mock

import (
	"github.com/IBM/sarama"
)

// ConsumerGroupClaim represents a mocked consumer group claim.
type ConsumerGroupClaim struct {
	TopicFunc               func() string
	PartitionFunc           func() int32
	InitialOffsetFunc       func() int64
	HighWaterMarkOffsetFunc func() int64
	messageChannel          chan *sarama.ConsumerMessage
}

// NewConsumerGroupClaim creates a new mocked consumer group claim.
func NewConsumerGroupClaim(
	topicFunc func() string,
	partitionFunc func() int32,
	initialOffsetFunc func() int64,
	highWaterMarkOffsetFunc func() int64,
	messageChannel chan *sarama.ConsumerMessage,
) ConsumerGroupClaim {
	return ConsumerGroupClaim{
		TopicFunc:               topicFunc,
		PartitionFunc:           partitionFunc,
		InitialOffsetFunc:       initialOffsetFunc,
		HighWaterMarkOffsetFunc: highWaterMarkOffsetFunc,
		messageChannel:          messageChannel,
	}
}

// Topic returns the mocked topic name
func (c ConsumerGroupClaim) Topic() string {
	if c.TopicFunc != nil {
		return c.TopicFunc()
	}
	return ""
}

// Partition returns the mocked partition
func (c ConsumerGroupClaim) Partition() int32 {
	if c.PartitionFunc != nil {
		return c.PartitionFunc()
	}
	return 0
}

// InitialOffset returns the mocked initial offset
func (c ConsumerGroupClaim) InitialOffset() int64 {
	if c.InitialOffsetFunc != nil {
		return c.InitialOffsetFunc()
	}
	return 0
}

// HighWaterMarkOffset returns the mocked high watermark offset
func (c ConsumerGroupClaim) HighWaterMarkOffset() int64 {
	if c.HighWaterMarkOffsetFunc != nil {
		return c.HighWaterMarkOffsetFunc()
	}
	return 0
}

// Messages returns the mocked messages channel
func (c ConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messageChannel
}

// PushMessage pushes a message to the mocked messages channel
func (c ConsumerGroupClaim) PushMessage(msg *sarama.ConsumerMessage) {
	if c.messageChannel != nil {
		c.messageChannel <- msg
	}
}
