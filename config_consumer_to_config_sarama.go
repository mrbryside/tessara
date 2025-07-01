package tessara

// ToSaramaConfig converts the consumer configuration to a Sarama configuration.
func (c consumerConfig) ToSaramaConfig() saramaConfig {
	saramaCfg := newSaramaConfig()
	for _, cc := range c.saramaConfig {
		switch configType := cc.(type) {
		case sasl:
			saramaCfg = saramaCfg.WithSASL512(configType.Username, configType.Password)
		case offsetInitialNewest:
			saramaCfg = saramaCfg.WithOffsetInitialNewest()
		case offsetInitialOldest:
			saramaCfg = saramaCfg.WithOffsetInitialOldest()
		default:
			// do nothing
		}
	}
	// set channel buffer size sarama to equal to consumer buffer size
	saramaCfg.saramaConfig.ChannelBufferSize = int(c.bufferSize)

	return saramaCfg
}
