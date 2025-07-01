package tessara

import "time"

// ------ Consumer ------
// sasl configures SASL authentication for the consumer.
type sasl struct {
	Username string
	Password string
}

// offsetInitialNewest configures the consumer to start consuming from the newest offset.
type offsetInitialNewest struct{}

// offsetInitialOldest configures the consumer to start consuming from the oldest offset.
type offsetInitialOldest struct{}

// ------ Producer -------
// producerRetry configures the producer to retry sending messages.
type producerRetry struct {
	Max int
}

// producerTimeout configures the producer to timeout sending messages.
type producerTimeout struct {
	Duration time.Duration
}
