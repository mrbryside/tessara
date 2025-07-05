package tessara

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/mrbryside/tessara/logger"
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

// WithErrorHandler sets the error handler for the consumer group
func (c consumer) WithErrorHandler(eh errorHandler) consumer {
	c.consumerGroupHandler.errorHandler = eh
	return c
}

// StartConsume starts the consumer group and will be block until context is cancelled or signal is received
func (c consumer) StartConsume(ctx context.Context) {
	// init log
	logger.Init()

	// convert config to sarama config
	saramaCfg := c.consumerConfig.ToSaramaConfig().Config()
	consumerGroup, err := sarama.NewConsumerGroup(c.consumerConfig.brokers, c.consumerConfig.consumerGroupID, saramaCfg)
	if err != nil {
		logger.Panic().Err(err).Msg("unable to create sarama consumer group")
	}

	// start consume group this line will return sync wait group
	wg := c.consume(ctx, consumerGroup)
	logger.Debug().Msg("consumer started!")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		logger.Debug().Msg("terminating consumer via context cancelled")
	case <-signals:
		logger.Debug().Msg("terminating consumer consume via signal")
	}

	// waiting for consume done
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		logger.Panic().Err(err).Msg("error closing client")
	}
	logger.Debug().Msg("consumer closed!")
}

// consume starts consuming messages from the Kafka topic with error watching
func (c consumer) consume(ctx context.Context, cg sarama.ConsumerGroup) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		startErrorWatch(cg)
	}()
	go func() {
		defer wg.Done()
		for {
			err := cg.Consume(ctx, []string{c.consumerConfig.topic}, c.consumerGroupHandler)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logger.Panic().Err(err).Msg("unable to consume")
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return wg
}

// startErrorWatch starts watching for errors from the consumer group sarama will return to Errors() channel
func startErrorWatch(cg sarama.ConsumerGroup) {
	for err := range cg.Errors() {
		if err == nil {
			continue
		}
		if errors.Is(err, sarama.ErrOffsetOutOfRange) {
			logger.Panic().Err(err).Msg("error offset out of range")
		}
	}
}
