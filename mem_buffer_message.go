package tessara

import "sync/atomic"

// messageBuffer represents a buffer for storing message offset and mark status.
type messageBuffer struct {
	offset        int64
	isMarkSuccess int32
}

// newMessageBuffer creates a new message buffer with the given offset.
func newMessageBuffer(offset int64) *messageBuffer {
	return &messageBuffer{
		offset:        offset,
		isMarkSuccess: 0,
	}
}

// Offset returns the current offset of the message buffer.
func (mb *messageBuffer) Offset() int64 {
	return atomic.LoadInt64(&mb.offset)
}

// MarkSuccess marks the message buffer as successful.
func (mb *messageBuffer) MarkSuccess() {
	atomic.StoreInt32(&mb.isMarkSuccess, 1)
}

// IsMarkSuccess returns true if the message buffer is marked as successful.
func (mb *messageBuffer) IsMarkSuccess() bool {
	return atomic.LoadInt32(&mb.isMarkSuccess) == 1
}
