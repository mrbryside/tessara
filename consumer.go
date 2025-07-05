package tessara

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
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

	var ready = make(chan bool)
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.consume(ctx, consumerGroup, wg, ready)

	// wait until consume ready
	<-ready
	log.Println("consumer started!")

	// handle signals, context cancel
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// handle SIGUSR1 signal
	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	consumptionIsPaused := false
	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("[Consumer.StartConsume]: terminating via context cancelled")
			keepRunning = false
		case <-signals:
			log.Println("[Consumer.StartConsume]: terminating via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(consumerGroup, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	consumerGroup.Close()
	close(ready)
}

// WithErrorHandler sets the error handler for the consumer group
func (c consumer) WithErrorHandler(eh errorHandler) consumer {
	c.consumerGroupHandler.errorHandler = eh
	return c
}

// consume starts consuming messages from the Kafka topic with error watching
func (c consumer) consume(ctx context.Context, cg sarama.ConsumerGroup, wg *sync.WaitGroup, ready chan bool) {
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
				log.Panicf("unable to consume: %s", err.Error())
			}
			if ctx.Err() != nil {
				return
			}
			ready <- true
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

func toggleConsumptionFlow(cg sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		cg.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		cg.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}
