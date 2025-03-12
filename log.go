package main

import (
	"os"

	btclogv1 "github.com/btcsuite/btclog"
	"github.com/btcsuite/btclog/v2"
)

var (
	// backend is the logging backend used to create all loggers.
	backend = btclog.NewDefaultHandler(os.Stderr)

	// logger is logger for the main package of the lndinit tool.
	logger = btclog.NewSLogger(backend).WithPrefix("LNDINIT")
)

// Initialize sets up all loggers with their default levels.
func Initialize(debugLevel btclogv1.Level) {
	// Set default log level for all loggers
	level := btclogv1.LevelInfo
	if debugLevel.String() != "" {
		var err error
		backend.SetLevel(debugLevel)
		if err != nil {
			logger.Errorf("Invalid debug level %s, "+
				"using info", debugLevel)
			level = btclog.LevelInfo
		}
	}

	logger.SetLevel(level)
}

// NewSubLogger creates a new sub logger with the given prefix.
func NewSubLogger(prefix string) btclog.Logger {
	return logger.SubSystem(prefix)
}
