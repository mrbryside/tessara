package tessara

import (
	"time"

	"github.com/IBM/sarama"
)

// messageHandler is an interface for handling messages from subqueue
type messageHandler interface {
	Perform(PerformMessage) error
	Fallback(PerformMessage, error)
}

// PerformMessage represents a message to be processed by a SubQueueHandler.
type PerformMessage struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
	Timestamp  time.Time // only set if kafka is version 0.10+
}

// toPerformMessage converts a sarama.ConsumerMessage to a PerformMessage.
func toPerformMessage(cm *sarama.ConsumerMessage) PerformMessage {
	return PerformMessage{
		Key:       cm.Key,
		Value:     cm.Value,
		Topic:     cm.Topic,
		Partition: cm.Partition,
		Offset:    cm.Offset,
		Timestamp: cm.Timestamp,
	}
}
