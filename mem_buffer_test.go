package tessara

// import (
// 	"context"
// 	"testing"
// 	"time"

// 	"github.com/stretchr/testify/assert"
// )

// func TestMarkSuccessViaPointer(t *testing.T) {
// 	mb := newMemoryBuffer(context.Background(), 10, 100*time.Millisecond, 100*time.Millisecond)

// 	msgBuffer := &messageBuffer{
// 		offset:        1,
// 		isMarkSuccess: 0,
// 	}
// 	mb.Push(context.Background(), msgBuffer)
// 	msgBuffer.MarkSuccess()

// 	assert.Equal(t, mb.messageBuffers[0].IsMarkSuccess(), true)
// }

// func TestPushMessageMoreThanBufferSize(t *testing.T) {
// 	mb := newMemoryBuffer(context.Background(), 2, 100*time.Millisecond, 100*time.Millisecond)

// 	msgBuffer := &messageBuffer{
// 		offset:        1,
// 		isMarkSuccess: 0,
// 	}
// 	msgBuffer2 := &messageBuffer{
// 		offset:        2,
// 		isMarkSuccess: 0,
// 	}
// 	msgBuffer3 := &messageBuffer{
// 		offset:        3,
// 		isMarkSuccess: 0,
// 	}
// 	msgBuffer4 := &messageBuffer{
// 		offset:        4,
// 		isMarkSuccess: 0,
// 	}
// 	mb.Push(context.Background(), msgBuffer)
// 	mb.Push(context.Background(), msgBuffer2)
// 	msgBuffer.MarkSuccess()
// 	msgBuffer2.MarkSuccess()
// 	mb.Push(context.Background(), msgBuffer3)
// 	mb.Push(context.Background(), msgBuffer4)
// 	msgBuffer3.MarkSuccess()
// 	msgBuffer4.MarkSuccess()

// 	time.Sleep(1 * time.Second)

// 	assert.Equal(t, mb.messageBuffers[0], msgBuffer3)
// 	assert.Equal(t, mb.messageBuffers[1], msgBuffer4)
// 	assert.Equal(t, mb.messageBuffers[0].IsMarkSuccess(), true)
// 	assert.Equal(t, mb.messageBuffers[1].IsMarkSuccess(), true)
// 	assert.Equal(t, mb.WaterMark(), uint64(4))
// 	assert.Equal(t, mb.WaterMarkOffset(), int64(4))
// }

// func TestPushMessageMoreThanBufferSizeThenMarkSuccessAfterPushAgain(t *testing.T) {
// 	mb := newMemoryBuffer(context.Background(), 2, 100*time.Millisecond, 100*time.Millisecond)

// 	msgBuffer := &messageBuffer{
// 		offset:        1,
// 		isMarkSuccess: 0,
// 	}
// 	msgBuffer2 := &messageBuffer{
// 		offset:        2,
// 		isMarkSuccess: 0,
// 	}
// 	msgBuffer3 := &messageBuffer{
// 		offset:        3,
// 		isMarkSuccess: 0,
// 	}
// 	mb.Push(context.Background(), msgBuffer)
// 	mb.Push(context.Background(), msgBuffer2)
// 	go func() {
// 		time.Sleep(1 * time.Second)
// 		msgBuffer.MarkSuccess()
// 	}()
// 	mb.Push(context.Background(), msgBuffer3)
// 	assert.Equal(t, mb.WaterMark(), uint64(1))

// 	msgBuffer3.MarkSuccess()
// 	time.Sleep(2 * time.Second)
// 	assert.Equal(t, mb.WaterMark(), uint64(1)) // this will not increase the watermark because the message buffer offset 2 not finish yet even though msgBuffer3 is marked as success

// 	msgBuffer2.MarkSuccess()
// 	time.Sleep(1 * time.Second)
// 	assert.Equal(t, mb.WaterMark(), uint64(3))
// }
