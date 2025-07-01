package tessara

// ProducerMessage represents a message to be produced by a producer.
type ProducerMessage struct {
	Topic     string
	Partition *int32
	Key       string
	Headers   []Header
	Value     []byte

	MetricLabelKeyType string
}

// Header represents a header to be included in a message.
type Header struct {
	Key   string
	Value []byte
}
