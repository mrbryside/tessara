package tessara

import "time"

// consumerConfig represents the configuration for a consumer and sarama config that will be transformed into a sarama config.
type consumerConfig struct {
	// kafka configuration
	topic           string
	consumerGroupID string
	brokers         []string

	// memory buffer config
	bufferSize                  uint64
	waterMarkUpdateInterval     time.Duration
	pushMessageBlockingInterval time.Duration

	// message handler config
	maxRetry        int
	retryMultiplier float64

	// subqueue config
	subqueueNumber int
	subqueueMode   string

	// comitter config
	commitInterval       time.Duration
	commitGiveUpInterval time.Duration
	commitGiveUpTime     time.Duration

	// sarama config
	saramaConfig []any
}

// NewConsumerConfig creates a new consumer configuration.
func NewConsumerConfig(brokers []string, topic string, consumerGroupID string) consumerConfig {
	c := consumerConfig{
		topic:           topic,
		consumerGroupID: consumerGroupID,
		brokers:         brokers,
	}

	// default config
	c.bufferSize = 256
	c.subqueueNumber = 1
	c.subqueueMode = "key_distribute"
	c.maxRetry = 0
	c.retryMultiplier = 1.5 // this may no need right now because user using WithRetry to set that give both maxRetry and retryMultiplier
	c.commitInterval = 3 * time.Second
	c.commitGiveUpInterval = 10 * time.Second
	c.commitGiveUpTime = 120 * time.Second
	c.waterMarkUpdateInterval = 100 * time.Millisecond
	c.pushMessageBlockingInterval = 100 * time.Millisecond

	return c
}

/*
consumer configuration
*/

// WithBufferSize sets the buffer size for the consumer.
func (c consumerConfig) WithBufferSize(bufferSize uint64) consumerConfig {
	if bufferSize <= 0 {
		panic("buffer size must be greater than 0")
	}
	c.bufferSize = bufferSize
	return c
}

// WithRoundRobinMode sets the round robin mode for the consumer.
func (c consumerConfig) WithRoundRobinMode() consumerConfig {
	c.subqueueMode = "round_robin"
	return c
}

// WithKeyDistributeMode sets the key distribute mode for the consumer.
func (c consumerConfig) WithKeyDistributeMode() consumerConfig {
	c.subqueueMode = "key_distribute"
	return c
}

// WithCommitGiveUpInterval sets the commit give up interval for the consumer.
func (c consumerConfig) WithCommitGiveUpInterval(commitGiveUpInterval time.Duration) consumerConfig {
	if commitGiveUpInterval <= 0 {
		panic("commit give up interval must be greater than 0")
	}
	c.commitGiveUpInterval = commitGiveUpInterval
	return c
}

// WithCommitGiveUpTime sets the commit give up time for the consumer.
func (c consumerConfig) WithCommitGiveUpTime(commitGiveUpTime time.Duration) consumerConfig {
	if commitGiveUpTime <= 0 {
		panic("commit give up interval must be greater than 0")
	}
	c.commitGiveUpTime = commitGiveUpTime
	return c
}

// WithCommitInterval sets the commit interval for the consumer.
func (c consumerConfig) WithCommitInterval(commitInterval time.Duration) consumerConfig {
	if commitInterval <= 0 {
		panic("commit interval must be greater than 0")
	}
	c.commitInterval = commitInterval
	return c
}

// WithRetry sets the retry configuration for the consumer.
func (c consumerConfig) WithRetry(maxRetry int, retryMultiplier float64) consumerConfig {
	if maxRetry < 0 {
		panic("max retry must be greater than or equal to 0")
	}
	if retryMultiplier <= 0 {
		panic("retry multiplier must be greater than 0")
	}
	c.maxRetry = maxRetry
	c.retryMultiplier = retryMultiplier

	return c
}

// WithSubqueue sets the subqueue number for the consumer.
func (c consumerConfig) WithSubqueue(subqueueNumber int) consumerConfig {
	if subqueueNumber <= 0 {
		panic("subqueue number must be greater than 0")
	}
	c.subqueueNumber = subqueueNumber
	return c
}

//------------

/*
sarama config functions, config below will transform to sarama configuration to put into sarama.Config when creating a new consumer group.
*/

// WithSASL sets the SASL configuration for the consumer.
func (c consumerConfig) WithSASL(username, password string) consumerConfig {
	if username == "" || password == "" {
		panic("username and password must not be empty")
	}
	c.saramaConfig = append(c.saramaConfig, sasl{
		Username: username,
		Password: password,
	})
	return c
}

// WithOffsetInitialNewest sets the offset initial to newest for the consumer.
func (c consumerConfig) WithOffsetInitialNewest() consumerConfig {
	c.saramaConfig = append(c.saramaConfig, offsetInitialNewest{})
	return c
}

// WithOffsetInitialOldest sets the offset initial to oldest for the consumer.
func (c consumerConfig) WithOffsetInitialOldest() consumerConfig {
	c.saramaConfig = append(c.saramaConfig, offsetInitialOldest{})
	return c
}

//------------
