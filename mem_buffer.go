package tessara

import (
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/mrbryside/tessara/metric"
)

// memoryBuffer is a struct that implements the sarama.ConsumerGroupHandler interface
type memoryBuffer struct {
	waterMarkUpdateInterval     time.Duration
	pushMessageBlockingInterval time.Duration

	messageBuffers  []*messageBuffer
	bufferSize      uint64
	currentBuffer   uint64
	waterMark       uint64
	waterMarkOffset int64
}

// newMemoryBuffer creates a new MemoryBuffer instance with the given buffer size.
func newMemoryBuffer(ctx context.Context, bufferSize uint64, waterMarkUpdateInterval time.Duration, pushMessageBlockingInterval time.Duration) *memoryBuffer {
	mb := &memoryBuffer{
		waterMarkUpdateInterval:     waterMarkUpdateInterval,
		pushMessageBlockingInterval: pushMessageBlockingInterval,
		messageBuffers:              make([]*messageBuffer, bufferSize),
		bufferSize:                  bufferSize,
		currentBuffer:               0,
		waterMark:                   0,
		waterMarkOffset:             -1,
	}
	// set default metric
	metric.UpdateBufferSize(bufferSize)

	go func() {
		mb.waterMarkUpdater(ctx)
	}()
	return mb
}

// waterMarkUpdater updates the water mark of the memory buffer.
func (mb *memoryBuffer) waterMarkUpdater(ctx context.Context) {
	ticker := time.NewTicker(mb.waterMarkUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// need this condition to avoid race condition when Checking IsWaterMarkMsgMarkSuccess it's access array that push by Push function
			if mb.WaterMark() < mb.CurrentBuffer() {
				if mb.IsWaterMarkMsgMarkSuccess() {
					// current water mark is mark success we need to update water mark offset of memory buffer
					// then increase water mark to validate next buffer
					mb.UpdateWaterMarkOffsetByWaterMarkMsgOffset()
					mb.IncrementWaterMark()
					// update metric
					metric.IncrementCurrentWatermark()
				}
			}
		}
	}
}

// Push pushes a new message buffer to the memory buffer.
func (mb *memoryBuffer) Push(ctx context.Context, msgBuffer *messageBuffer) {
	start := time.Now()
bufferCheckLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if mb.IsBufferAvailable() {
				break bufferCheckLoop
			}
			time.Sleep(mb.pushMessageBlockingInterval)
		}
	}

	currentBuffer := mb.CurrentBuffer()
	mb.messageBuffers[currentBuffer%mb.bufferSize] = msgBuffer
	mb.IncrementCurrentBuffer()

	// update current buffer metric
	metric.IncrementCurrentBuffer()
	// update wait time metric
	elapse := time.Since(start)
	metric.UpdatePushToBufferWatingTime(elapse)
}

// WaterMarkOffset returns the offset of the water mark message.
func (mb *memoryBuffer) WaterMarkOffset() int64 {
	return atomic.LoadInt64(&mb.waterMarkOffset)
}

// WaterMark returns the water mark index
func (mb *memoryBuffer) WaterMark() uint64 {
	return atomic.LoadUint64(&mb.waterMark)
}

// WaterMarkMsgOffset returns the offset of the water mark message.
func (mb *memoryBuffer) WaterMarkMsgOffset() int64 {
	return mb.messageBuffers[atomic.LoadUint64(&mb.waterMark)%mb.bufferSize].Offset()
}

// CurrentBuffer returns the current buffer index.
func (mb *memoryBuffer) CurrentBuffer() uint64 {
	return atomic.LoadUint64(&mb.currentBuffer)
}

// IsBufferAvailable returns true if the current buffer index is greater than the water mark index.
func (mb *memoryBuffer) IsBufferAvailable() bool {
	return mb.CurrentBuffer()-mb.WaterMark() < mb.bufferSize
}

// IsNeedToCommit returns true if the current buffer index is greater than the water mark index.
func (mb *memoryBuffer) IsNeedToCommit() bool {
	return mb.CurrentBuffer()-mb.WaterMark() > 0
}

// IsWaterMarkMsgMarkSuccess check current message access index by waterMark % bufferSize and check this message in buffer is mark success or not
func (mb *memoryBuffer) IsWaterMarkMsgMarkSuccess() bool {
	messageBuffer := mb.messageBuffers[atomic.LoadUint64(&mb.waterMark)%mb.bufferSize]
	if messageBuffer == nil {
		return false
	}
	return messageBuffer.IsMarkSuccess()
}

// UpdateWaterMarkOffsetByWaterMarkMsgOffset updates the water mark offset by the water mark message offset.
func (mb *memoryBuffer) UpdateWaterMarkOffsetByWaterMarkMsgOffset() {
	atomic.StoreInt64(&mb.waterMarkOffset, mb.WaterMarkMsgOffset())
}

// IncrementWaterMark increments the water mark index.
func (mb *memoryBuffer) IncrementWaterMark() {
	atomic.AddUint64(&mb.waterMark, 1)
}

// IncrementCurrentBuffer increments the current buffer index.
func (mb *memoryBuffer) IncrementCurrentBuffer() {
	atomic.AddUint64(&mb.currentBuffer, 1)
}
