package main

import (
	"os"

	"github.com/btcsuite/btclog/v2"
)

var (
	// backend is the logging backend used to create all loggers.
	backend = btclog.NewDefaultHandler(os.Stderr)

	// logger is logger for the main package of the lndinit tool.
	logger = btclog.NewSLogger(backend).WithPrefix("LNDINIT")
)

// NewSubLogger creates a new sub logger with the given prefix.
func NewSubLogger(prefix string) btclog.Logger {
	return logger.SubSystem(prefix)
}
