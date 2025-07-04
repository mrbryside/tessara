package tessara

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/mrbryside/tessara/metric"
)

var (
	warningLevelErrors = []string{
		sarama.KError(sarama.ErrRequestTimedOut).Error(),
	}
)

// consumer represents a consumer instance
type consumer struct {
	consumerGroupHandler *consumerHandler
	consumerConfig       consumerConfig
}

// NewConsumer creates a new consumer instance
func NewConsumer(cfg consumerConfig, mh messageHandler) consumer {
	// register metics
	prometheus.MustRegister(metric.MemoryBufferSize)
	prometheus.MustRegister(metric.CurrentWaterMark)
	prometheus.MustRegister(metric.CurrentBuffer)
	prometheus.MustRegister(metric.PushToBufferWatingTime)
	prometheus.MustRegister(metric.SubqueueCount)
	prometheus.MustRegister(metric.SubqueueChannelBufferSize)
	prometheus.MustRegister(metric.SubqueueMessageProcessedCount)
	prometheus.MustRegister(metric.SubqueueMessageProcessingCount)
	prometheus.MustRegister(metric.SubqueueMessageProcessingTime)
	prometheus.MustRegister(metric.SubqueueMessageErrorCount)

	return consumer{
		consumerGroupHandler: newConsumerGroupHandler(mh, newLoggingErrorHandler(), cfg),
		consumerConfig:       cfg,
	}
}

// StartConsume starts the consumer group and will be block until context is cancelled or signal is received
func (c consumer) StartConsume(ctx context.Context) {
	saramaCfg := c.consumerConfig.ToSaramaConfig().Config()
	consumerGroup, err := sarama.NewConsumerGroup(c.consumerConfig.brokers, c.consumerConfig.consumerGroupID, saramaCfg)
	if err != nil {
		log.Panicf("[Consumer.StartConsume]: unable to create sarama consumer group due to %s", err.Error())
	}

	// start consume
	c.consume(ctx, consumerGroup)
	log.Println("consumer started!")

	// handle signals, context cancel
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Println("[Consumer.StartConsume]: terminating via context cancelled")
	case <-signals:
		log.Println("[Consumer.StartConsume]: terminating via signal")
	}
}

// WithErrorHandler sets the error handler for the consumer group
func (c consumer) WithErrorHandler(eh errorHandler) consumer {
	c.consumerGroupHandler.errorHandler = eh
	return c
}

// consume starts consuming messages from the Kafka topic with error watching
func (c consumer) consume(ctx context.Context, cg sarama.ConsumerGroup) {
	go func() {
		startErrorWatch(cg)
	}()
	go func() {
		for {
			err := cg.Consume(ctx, []string{c.consumerConfig.topic}, c.consumerGroupHandler)
			if err != nil {
				log.Panicf("unable to consume: %s", err.Error())
			}
		}
	}()
}

// startErrorWatch starts watching for errors from the consumer group sarama will return to Errors() channel
func startErrorWatch(cg sarama.ConsumerGroup) {
	for err := range cg.Errors() {
		if err == nil {
			continue
		}
		if errors.Is(err, sarama.ErrOffsetOutOfRange) {
			log.Panic("[errorWatch]: error offset out of range, terminating...")
		}
	}
}
