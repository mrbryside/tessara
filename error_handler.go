package tessara

import (
	"log"
)

// errorHandler is an interface for handling errors
type errorHandler interface {
	HandleCommitGiveUp(topic string, partition int32)
}

// loggingErrorHandler logs errors handler
type loggingErrorHandler struct{}

// newLoggingErrorHandler creates a new instance of LoggingErrorHandler
func newLoggingErrorHandler() loggingErrorHandler {
	return loggingErrorHandler{}
}

// HandleCommitGiveUp logs the commit give up event
func (lh loggingErrorHandler) HandleCommitGiveUp(topic string, partition int32) {
	log.Println("commit give up for ")
}
