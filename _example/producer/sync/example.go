package main

import (
	"time"

	"github.com/mrbryside/tessara"
	"github.com/mrbryside/tessara/logger"
)

func main() {
	// init logger
	logger.Init()

	brokers := []string{"host.docker.internal:9092"}
	topic := "example-topic"

	cfgProducer := tessara.NewProducerConfig(brokers).
		WithSASL("kafkaUser", "kafkaPassword").
		WithRetry(5).
		WithTimeout(3 * time.Second)

	sp := tessara.NewSyncProducer(cfgProducer)
	defer sp.Producer.Close()

	msg := tessara.ProducerMessage{
		Topic: topic,
		Key:   "key",
		Value: []byte("message-1"),
	}
	_, _, err := sp.Produce(msg)
	if err != nil {
		logger.Debug().Err(err).Msg("unable to produce message")
	}

}
