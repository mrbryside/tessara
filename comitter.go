package tessara

import (
	"errors"
	"time"

	"golang.org/x/net/context"

	"github.com/IBM/sarama"
	"github.com/mrbryside/tessara/logger"
)

// committer is a struct that implements the sarama.ConsumerGroupHandler interface
type committer struct {
	// error handler
	errorHandler errorHandler

	// memory buffer
	memoryBuffer *memoryBuffer

	// sarama consumer group information
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim

	// comitter
	commitInterval              time.Duration
	commitGiveupInterval        time.Duration
	commitGiveUpTime            time.Duration
	commitGiveUpErrorChan       chan error
	latestCommittedOffset       int64
	lastestCommittedAt          time.Time
	pushMessageBlockingInterval time.Duration
}

// newCommitter creates a new Committer instance
func newCommitter(
	ctx context.Context,
	commitGiveUpErrorChan chan error,
	errorHandler errorHandler,
	mb *memoryBuffer,
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	commitInterval time.Duration,
	commitGiveupInterval time.Duration,
	commitGiveUpTime time.Duration,
	pushMessageBlockingInterval time.Duration,
) *committer {
	c := &committer{
		commitGiveUpErrorChan:       commitGiveUpErrorChan,
		errorHandler:                errorHandler,
		session:                     session,
		claim:                       claim,
		memoryBuffer:                mb,
		commitInterval:              commitInterval,
		commitGiveupInterval:        commitGiveupInterval,
		commitGiveUpTime:            commitGiveUpTime,
		latestCommittedOffset:       -1,
		lastestCommittedAt:          time.Now(),
		pushMessageBlockingInterval: pushMessageBlockingInterval,
	}

	go func() {
		c.startCommitIntervalAndCommitGiveUpInterval(ctx)
	}()

	return c
}

// startCommitIntervalAndCommitGiveUpInterval starts the commit interval to periodically commit offsets to Kafka also check give up time then handle it
func (c *committer) startCommitIntervalAndCommitGiveUpInterval(ctx context.Context) {
	tickerCommitInterval := time.NewTicker(c.commitInterval)
	defer tickerCommitInterval.Stop()

	tickerCommitGiveUpInterval := time.NewTicker(c.commitGiveupInterval)
	defer tickerCommitGiveUpInterval.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tickerCommitInterval.C:
			waterMarkOffset := c.memoryBuffer.WaterMarkOffset()
			waterMarkOffsetForCommit := waterMarkOffset + 1 // mark offset in kafka needs to be incremented by 1
			if isWaterMarkOffsetNotDefault(waterMarkOffset) && isWaterMarkOffsetMoreThanLatestComittedOffset(waterMarkOffset, c.latestCommittedOffset) {
				c.session.MarkOffset(c.claim.Topic(), c.claim.Partition(), waterMarkOffsetForCommit, "")
				c.latestCommittedOffset = waterMarkOffset
				c.lastestCommittedAt = time.Now()
				logger.Debug().
					Str("topic", c.claim.Topic()).
					Int32("partition", c.claim.Partition()).
					Int64("offset", waterMarkOffsetForCommit).
					Msg("offset committed")
			}

		case <-tickerCommitGiveUpInterval.C:
			if isCommitExceedGiveUpTime(c.lastestCommittedAt, c.commitGiveUpTime) && c.memoryBuffer.IsNeedToCommit() {
				c.errorHandler.HandleCommitGiveUp(c.claim.Topic(), c.claim.Partition())
				c.pushErrorToGiveUpErrorChannel(ctx)
			}
		}
	}
}

// pushErrorToGiveUpErrorChannel pushes error to give up error channel
func (c *committer) pushErrorToGiveUpErrorChannel(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c.commitGiveUpErrorChan <- errors.New("error commit give up"):
			return
		default:
			time.Sleep(c.pushMessageBlockingInterval)
		}
	}

}

func isCommitExceedGiveUpTime(lastestCommitedAt time.Time, commitGiveUpTime time.Duration) bool {
	return time.Now().After(lastestCommitedAt.Add(commitGiveUpTime))
}

// isWaterMarkOffsetMoreThanLatestComittedOffset checks if the water mark offset is more than the latest committed offset
func isWaterMarkOffsetMoreThanLatestComittedOffset(waterMarkOffset, latestComittedOffset int64) bool {
	return waterMarkOffset > latestComittedOffset
}

// isWaterMarkOffsetNotDefault checks if the water mark offset is not default
func isWaterMarkOffsetNotDefault(waterMarkOffset int64) bool {
	return waterMarkOffset != -1
}
