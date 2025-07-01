package metric

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MemoryBufferSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "memory_buffer_size",
			Help: "buffer size of memory buffer",
		},
	)

	CurrentWaterMark = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "current_watermark",
			Help: "current watermark of memory buffer",
		},
	)

	CurrentBuffer = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "current_buffer",
			Help: "current buffer of memory buffer",
		},
	)

	PushToBufferWatingTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "push_to_buffer_waiting_time_seconds",
			Help: "wait time of messages in buffer",
		},
	)

	SubqueueChannelBufferSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "subqueue_channel_buffer_size",
			Help: "buffer size of subqueue channel",
		},
	)

	SubqueueCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "subqueue_count",
			Help: "Current number of subqueue",
		},
	)

	SubqueueMessageProcessingCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "subqueue_message_processing_count",
			Help: "Current number of subqueue message processing",
		},
		[]string{"subqueueId"}, // label to differentiate queues
	)

	SubqueueMessageProcessedCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "subqueue_message_processed_count",
			Help: "Current number of subqueue message processed",
		},
		[]string{"subqueueId"}, // label to differentiate queues
	)

	SubqueueMessageErrorCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "subqueue_message_error_count",
			Help: "Current number of subqueue message error",
		},
		[]string{"subqueueId"}, // label to differentiate queues
	)

	SubqueueMessageProcessingTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "subqueue_message_processing_time_seconds",
			Help: "processing time of messages in subqueue",
		},
	)
)

func ClearMetrics() {
	// do not clear current water mark, currentBuffer because it's will be zero forever if it's not updated
	// and do not clear SubqueueMessageProcessingTime, PushToBufferWatingTime because we need old stat when session reset
	CurrentWaterMark.Set(0)
	CurrentBuffer.Set(0)
	MemoryBufferSize.Set(0)
	PushToBufferWatingTime.Set(0)
	SubqueueMessageProcessingTime.Set(0)
	SubqueueCount.Set(0)
	SubqueueChannelBufferSize.Set(0)
	SubqueueMessageProcessingCount.Reset()
	SubqueueMessageProcessedCount.Reset()
	SubqueueMessageErrorCount.Reset()
}

// UpdateBufferSize updates the size of the memory buffer.
func UpdateBufferSize(bufferSize uint64) {
	go func() {
		MemoryBufferSize.Set(float64(bufferSize))
	}()
}

// IncrementCurrentWatermark increments the current watermark.
func IncrementCurrentWatermark() {
	go func() {
		CurrentWaterMark.Inc()
	}()
}

// UpdateCurrentBuffer updates the current buffer size.
func UpdateCurrentBuffer(buffer uint64) {
	go func() {
		CurrentBuffer.Set(float64(buffer))
	}()
}

// IncrementCurrentBuffer increments the current buffer size.
func IncrementCurrentBuffer() {
	go func() {
		CurrentBuffer.Inc()
	}()
}

// UpdatePushToBufferWatingTime updates the waiting time for pushing to buffer.
func UpdatePushToBufferWatingTime(elapse time.Duration) {
	go func() {
		PushToBufferWatingTime.Set(elapse.Seconds())
	}()
}

func UpdateSubqueueChannelBufferSize(bufferSize uint64) {
	go func() {
		SubqueueChannelBufferSize.Set(float64(bufferSize))
	}()
}

// InitSubqueueMessageProcessingCount initializes the subqueue message processing count.
func InitSubqueueMessageProcessingCount(subqueueId int) {
	go func() {
		SubqueueMessageProcessingCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Set(0)
	}()
}

// IncrementSubqueueMessageProcessingCount increments the subqueue message processing count.
func IncrementSubqueueMessageProcessingCount(subqueueId int) {
	go func() {
		SubqueueMessageProcessingCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Inc()
	}()
}

// DecrementSubqueueMessageProcessingCount decrements the subqueue message processing count.
func DecrementSubqueueMessageProcessingCount(subqueueId int) {
	go func() {
		SubqueueMessageProcessingCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Dec()
	}()
}

// InitSubqueueMessageProcessedCount initializes the subqueue message processed count.
func InitSubqueueMessageProcessedCount(subqueueId int) {
	go func() {
		SubqueueMessageProcessedCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Set(0)
	}()
}

// IncrementSubqueueMessageProcessedCount increments the subqueue message processed count.
func IncrementSubqueueMessageProcessedCount(subqueueId int) {
	go func() {
		SubqueueMessageProcessedCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Inc()
	}()
}

// SetSubqueueCount sets the subqueue count.
func SetSubqueueCount(count int) {
	go func() {
		SubqueueCount.Set(float64(count))
	}()
}

// InitSubqueueMessageErrorCount initializes the subqueue message error count.
func InitSubqueueMessageErrorCount(subqueueId int) {
	go func() {
		SubqueueMessageErrorCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Set(0)
	}()
}

// IncrementSubqueueMessageErrorCount increments the subqueue message error count.
func IncrementSubqueueMessageErrorCount(subqueueId int) {
	go func() {
		SubqueueMessageErrorCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Inc()
	}()
}

// DecrementSubqueueMessageErrorCount decrements the subqueue message error count.
func DecrementSubqueueMessageErrorCount(subqueueId int) {
	go func() {
		SubqueueMessageErrorCount.WithLabelValues(fmt.Sprintf("%d", subqueueId)).Dec()
	}()
}

// UpdateSubqueueMessageProcessingTime sets the subqueue message processing time.
func UpdateSubqueueMessageProcessingTime(elapse time.Duration) {
	go func() {
		SubqueueMessageProcessingTime.Set(elapse.Seconds())
	}()
}
