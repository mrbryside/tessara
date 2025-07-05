package main

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	"github.com/mrbryside/tessara"
	"github.com/mrbryside/tessara/logger"
)

// Handler represents a custom handler for handling perform events and fallback events.
type Handler struct{}

// Perform represents a custom handler for handling perform events.
func (h Handler) Perform(pm tessara.PerformMessage) error {
	randomSleep := time.Duration(rand.Intn(600)) * time.Millisecond
	randError := rand.Intn(100)
	if randError < 10 {
		return errors.New("error from random")
	}
	time.Sleep(randomSleep)
	log.Info().Msg("Message Proceed")
	return nil
}

// Fallback represents a custom handler for handling fallback events.
func (h Handler) Fallback(pm tessara.PerformMessage, err error) {
	logger.Debug().
		Str("topic", string(pm.Topic)).
		Int32("partition", pm.Partition).
		Msg("fallback triggered!")
}

// MyErrorHandler represents a custom error handler for handling commit give up events.
type MyErrorHandler struct{}

// HandleCommitGiveUp using for handle when message exceed commit give up time
// you can use this function to handle the commit give up event
func (mh MyErrorHandler) HandleCommitGiveUp(topic string, partition int32) {
	log.Debug().
		Str("topic", topic).
		Int32("partition", partition).
		Msg("commit give up handle triggered!")
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":2112", nil)
		if err != nil {
			log.Panic().Err(err).Msg("failed to start metrics server")
		}
	}()

	brokers := []string{"host.docker.internal:9092"}
	topic := "example-topic"
	groupID := "example-group"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := tessara.NewConsumerConfig(brokers, topic, groupID).
		WithSASL("kafkaUser", "kafkaPassword").
		WithOffsetInitialOldest().
		WithBufferSize(256).
		WithSubqueue(5).
		WithKeyDistributeMode().
		WithCommitInterval(3*time.Second).
		WithCommitGiveUpInterval(10*time.Second).
		WithCommitGiveUpTime(40*time.Second).
		WithRetry(5, 1.5)

	log.Info().Msg("consumer started")

	tessara.
		NewConsumer(cfg, Handler{}).
		WithErrorHandler(MyErrorHandler{}).
		StartConsume(ctx)

}
