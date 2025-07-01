package tessara

import "time"

// producerConfig represents the configuration for a producer.
type producerConfig struct {
	brokers []string

	saramaConfig []any
}

// NewProducerConfig creates a new producer configuration.
func NewProducerConfig(brokers []string) producerConfig {
	return producerConfig{
		brokers: brokers,
	}
}

/*
sarama config functions, config below will transform to sarama configuration to put into sarama.Config when creating a new consumer group.
*/

// WithSASL sets the SASL configuration for the consumer.
func (pc producerConfig) WithSASL(username, password string) producerConfig {
	if username == "" || password == "" {
		panic("username and password must not be empty")
	}
	pc.saramaConfig = append(pc.saramaConfig, sasl{
		Username: username,
		Password: password,
	})
	return pc
}

// WithRetry configures the producer to retry sending messages.
func (pc producerConfig) WithRetry(max int) producerConfig {
	pc.saramaConfig = append(pc.saramaConfig, producerRetry{
		Max: max,
	})
	return pc
}

// WithTimeout configures the producer to timeout sending messages.
func (pc producerConfig) WithTimeout(timeout time.Duration) producerConfig {
	pc.saramaConfig = append(pc.saramaConfig, producerTimeout{
		Duration: timeout,
	})
	return pc
}

//------------
