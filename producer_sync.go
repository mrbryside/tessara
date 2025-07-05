package tessara

import (
	"github.com/IBM/sarama"

	"github.com/mrbryside/tessara/logger"
)

// syncProducer represents a synchronous producer.
type syncProducer struct {
	Producer sarama.SyncProducer
}

// NewSyncProducer creates a new synchronous producer instance.
func NewSyncProducer(config producerConfig) syncProducer {
	producer, err := sarama.NewSyncProducer(config.brokers, config.ToSaramaConfig().Config())
	if err != nil {
		logger.Panic().Err(err).Msg("unable to create sync producer instance")
	}
	return syncProducer{
		Producer: producer,
	}
}

// Produce sends a message to the Kafka cluster synchronously.
func (sp syncProducer) Produce(pm ProducerMessage) (partition int32, offset int64, err error) {
	var headers []sarama.RecordHeader
	for _, header := range pm.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: []byte(header.Value),
		})
	}
	sPm := sarama.ProducerMessage{
		Topic:   pm.Topic,
		Value:   sarama.StringEncoder(pm.Value),
		Key:     sarama.StringEncoder(pm.Key),
		Headers: headers,
	}

	return sp.Producer.SendMessage(&sPm)

}
