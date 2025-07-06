package mock

import (
	"context"

	"github.com/IBM/sarama"
)

// ConsumerGroupSession is a mock implementation of sarama.ConsumerGroupSession
type ConsumerGroupSession struct {
	ClaimsFunc       func() map[string][]int32
	MemberIDFunc     func() string
	GenerationIDFunc func() int32
	MarkOffsetFunc   func(topic string, partition int32, offset int64, metadata string)
	ResetOffsetFunc  func(topic string, partition int32, offset int64, metadata string)
	MarkMessageFunc  func(msg *sarama.ConsumerMessage, metadata string)
	CommitFunc       func()
	ContextFunc      func() context.Context
}

// NewConsumerGroupSession creates a new mock ConsumerGroupSession
func NewConsumerGroupSession(
	claimsFunc func() map[string][]int32,
	memberIDFunc func() string,
	generationIDFunc func() int32,
	markOffsetFunc func(topic string, partition int32, offset int64, metadata string),
	resetOffsetFunc func(topic string, partition int32, offset int64, metadata string),
	markMessageFunc func(msg *sarama.ConsumerMessage, metadata string),
	commitFunc func(),
	contextFunc func() context.Context,
) ConsumerGroupSession {
	return ConsumerGroupSession{
		ClaimsFunc:       claimsFunc,
		MemberIDFunc:     memberIDFunc,
		GenerationIDFunc: generationIDFunc,
		MarkOffsetFunc:   markOffsetFunc,
		ResetOffsetFunc:  resetOffsetFunc,
		MarkMessageFunc:  markMessageFunc,
		CommitFunc:       commitFunc,
		ContextFunc:      contextFunc,
	}
}

// Claims returns the mocked claims
func (m ConsumerGroupSession) Claims() map[string][]int32 {
	if m.ClaimsFunc != nil {
		return m.ClaimsFunc()
	}
	return nil
}

// MemberID returns the mocked member ID
func (m ConsumerGroupSession) MemberID() string {
	if m.MemberIDFunc != nil {
		return m.MemberIDFunc()
	}
	return ""
}

// GenerationID returns the mocked generation ID
func (m ConsumerGroupSession) GenerationID() int32 {
	if m.GenerationIDFunc != nil {
		return m.GenerationIDFunc()
	}
	return 0
}

// MarkOffset marks the offset with the mocked function
func (m ConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if m.MarkOffsetFunc != nil {
		m.MarkOffsetFunc(topic, partition, offset, metadata)
	}
}

// ResetOffset resets the offset with the mocked function
func (m ConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	if m.ResetOffsetFunc != nil {
		m.ResetOffsetFunc(topic, partition, offset, metadata)
	}
}

// MarkMessage marks the message with the mocked function
func (m ConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	if m.MarkMessageFunc != nil {
		m.MarkMessageFunc(msg, metadata)
	}
}

func (m ConsumerGroupSession) Commit() {
	if m.CommitFunc != nil {
		m.CommitFunc()
	}
}

// Context returns the mocked context
func (m ConsumerGroupSession) Context() context.Context {
	if m.ContextFunc != nil {
		return m.ContextFunc()
	}
	return nil
}
