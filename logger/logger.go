package logger

import (
	"os"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

// Init for creating a logger instance
func Init() {
	// Check the environment variable
	enableLog := os.Getenv("TESSARA_ENABLE_LOG")

	// Configure a local logger instance
	if enableLog == "true" {
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		logger = zerolog.New(nil) // Disables logging for this instance
	}
}

// Debug for creating a debug event
func Debug() *zerolog.Event {
	return logger.Debug()
}

// Panic for creating a panic event
func Panic() *zerolog.Event {
	return logger.Panic()
}
