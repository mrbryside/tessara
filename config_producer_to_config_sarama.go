package tessara

// ToSaramaConfig converts the consumer configuration to a Sarama configuration.
func (c producerConfig) ToSaramaConfig() saramaConfig {
	saramaCfg := newSaramaConfig()
	for _, cc := range c.saramaConfig {
		switch configType := cc.(type) {
		case sasl:
			saramaCfg = saramaCfg.WithSASL512(configType.Username, configType.Password)
		case producerRetry:
			saramaCfg = saramaCfg.WithProducerRetry(configType.Max)
		case producerTimeout:
			saramaCfg = saramaCfg.WithProducerTimeout(configType.Duration)
		default:
			// do nothing
		}
	}

	return saramaCfg
}
