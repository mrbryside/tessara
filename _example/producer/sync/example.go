package main

import (
	"log"
	"time"

	"github.com/mrbryside/tessara"
)

func main() {
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
		log.Println("unable to produce message error:", err.Error())
	}

}
