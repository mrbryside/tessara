package tessara

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/IBM/sarama"
)

func BenchmarkRoundRobin(b *testing.B) {
	b.Run("buffer-size-256/subqueue-size-32/message-size-1000/cpu", func(b *testing.B) {
		os.Setenv("TESSARA_ENABLE_LOG", "false")
		os.Setenv("TESSARA_REGISTER_METRICS", "false")
		messageSize := 1000
		bufferSize := 256
		subqueue := 32

		finishChan := make(chan bool)
		ctx, cancel := context.WithCancel(context.Background())
		mcs := mockConsumerGroupSession(ctx, finishChan, int64(messageSize))
		mcc := mockConsumerGroupClaim(messageSize)
		cgh := prepareConsumer(bufferSize, subqueue)

		wg := sync.WaitGroup{}
		wg.Add(messageSize)
		go func() {
			for i := range messageSize {
				mcc.PushMessage(&sarama.ConsumerMessage{
					Topic:     "fake-topic",
					Partition: 0,
					Offset:    int64(i),
					Key:       []byte("fake-key"),
					Value:     []byte("fake-value"),
				})
				wg.Done()
			}
		}()
		wg.Wait()

		b.ResetTimer()
		go func() {
			<-finishChan
			cancel()
		}()
		cgh.ConsumeClaim(mcs, mcc)
		b.StopTimer()
	})
}
