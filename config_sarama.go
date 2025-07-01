package tessara

import (
	"crypto/sha512"
	"time"

	"github.com/IBM/sarama"
	"github.com/mrbryside/tessara/sacmclient"
	"github.com/twmb/murmur3"
)

// saramaConfig wraps the sarama.Config struct for easier configuration.
type saramaConfig struct {
	saramaConfig sarama.Config
}

// newSaramaConfig creates a new SaramaConfig with default settings.
func newSaramaConfig() saramaConfig {
	config := *sarama.NewConfig()
	config = setDefaultSetting(config)

	return saramaConfig{
		saramaConfig: config,
	}
}

// setDefaultSetting applies default settings to the sarama.Config.
func setDefaultSetting(c sarama.Config) sarama.Config {
	c.Version = sarama.V2_1_0_0
	// default consumer config
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.Return.Errors = true
	c.Consumer.Retry.Backoff = 1 * time.Second
	c.Metadata.Retry.Max = 10
	// c.Metadata.Retry.BackoffFunc = func(retries, maxRetries int) time.Duration {}
	// saramaConfig.Consumer.Group.ResetInvalidOffsets = true | false

	// default producer config
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.Retry.Max = 3
	c.Producer.Timeout = 3 * time.Second
	c.Producer.Partitioner = sarama.NewCustomHashPartitioner(murmur3.New32)
	return c
}

// WithSASL512 enables SASL/SCRAM-SHA-512 authentication.
func (s saramaConfig) WithSASL512(username string, password string) saramaConfig {
	s.saramaConfig.Net.SASL.Enable = true
	s.saramaConfig.Net.SASL.Handshake = true
	s.saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &sacmclient.XDGSCRAMClient{HashGeneratorFcn: sha512.New} }
	s.saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	s.saramaConfig.Net.SASL.User = username
	s.saramaConfig.Net.SASL.Password = password
	return s
}

// WithOffsetInitialNewest sets the consumer to start from the newest offset.
func (s saramaConfig) WithOffsetInitialNewest() saramaConfig {
	s.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	return s
}

// WithOffsetInitialOldest sets the consumer to start from the oldest offset.
func (s saramaConfig) WithOffsetInitialOldest() saramaConfig {
	s.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	return s
}

// WithProducerRetry configures the producer to retry sending messages.
func (s saramaConfig) WithProducerRetry(max int) saramaConfig {
	s.saramaConfig.Producer.Retry.Max = max
	return s
}

// WithProducerTimeout configures the producer to timeout sending messages.
func (s saramaConfig) WithProducerTimeout(timeout time.Duration) saramaConfig {
	s.saramaConfig.Producer.Timeout = timeout
	return s
}

// Config returns a pointer to the underlying sarama.Config.
func (s saramaConfig) Config() *sarama.Config {
	return &s.saramaConfig
}
