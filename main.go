package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/btcsuite/btclog/v2"
	"github.com/jessevdk/go-flags"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc"
)

const (
	ExitCodeSuccess      int = 0
	ExitCodeTargetExists int = 128
	ExitCodeInputMissing int = 129
	ExitCodeFailure      int = 255

	outputFormatRaw  = "raw"
	outputFormatJSON = "json"

	storageFile = "file"
	storageK8s  = "k8s"

	errTargetExists = "target exists error"
	errInputMissing = "input missing error"

	defaultRPCPort   = "10009"
	defaultRPCServer = "localhost:" + defaultRPCPort
)

type globalOptions struct {
	ErrorOnExisting bool   `long:"error-on-existing" short:"e" description:"Exit with code EXIT_CODE_TARGET_EXISTS (128) instead of 0 if the result of an action is already present"`
	DebugLevel      string `long:"debuglevel" short:"d" description:"Set the log level (Off, Critical, Error, Warn, Info, Debug, Trace)"`
}

func main() {
	globalOpts := &globalOptions{}

	// Set up a very minimal logging to stderr if verbose mode is on. To get
	// just the global options, we do a pre-parsing without any commands
	// registered yet. We ignore any errors as that'll be handled later.
	_, _ = flags.NewParser(globalOpts, flags.IgnoreUnknown).Parse()

	logger.Info("Version %s commit=%s, debuglevel=debug", Version(), Commit)

	if globalOpts.DebugLevel != "" {
		level, ok := btclog.LevelFromString(globalOpts.DebugLevel)
		if !ok {
			logger.Errorf("Invalid debug level %s, "+
				"using info", globalOpts.DebugLevel)
			level = btclog.LevelInfo
		}

		logger.SetLevel(level)
	}

	parser := flags.NewParser(
		globalOpts, flags.HelpFlag|flags.PassDoubleDash,
	)
	if err := registerCommands(parser); err != nil {
		logger.Errorf("Command parser error: %v", err)
		os.Exit(ExitCodeFailure)
	}

	if _, err := parser.Parse(); err != nil {
		flagErr, isFlagErr := err.(*flags.Error)
		switch {
		case isFlagErr:
			if flagErr.Type != flags.ErrHelp {
				// Print error if not due to help request.
				logger.Errorf("Config error: %v", err)
				os.Exit(ExitCodeFailure)
			} else {
				// Help was requested, print without any log
				// prefix and exit normally.
				_, _ = fmt.Fprintln(os.Stderr, flagErr.Message)
				os.Exit(ExitCodeSuccess)
			}

		// Ugh, can't use errors.Is() here because the flag parser does
		// not wrap the returned errors properly.
		case strings.Contains(err.Error(), errTargetExists):
			// Respect the user's choice of verbose/non-verbose
			// logging here. The default is quietly aborting if the
			// target already exists.
			if globalOpts.ErrorOnExisting {
				logger.Errorf("Failing on state error: %v", err)
				os.Exit(ExitCodeTargetExists)
			}

			logger.Errorf("Ignoring non-fatal error: %v", err)
			os.Exit(ExitCodeSuccess)

		// Ugh, can't use errors.Is() here because the flag parser does
		// not wrap the returned errors properly.
		case strings.Contains(err.Error(), errInputMissing):
			logger.Errorf("Input error: %v", err)
			os.Exit(ExitCodeInputMissing)

		default:
			logger.Errorf("Runtime error: %v", err)
			os.Exit(ExitCodeFailure)
		}
	}

	os.Exit(ExitCodeSuccess)
}

type subCommand interface {
	Register(parser *flags.Parser) error
}

func registerCommands(parser *flags.Parser) error {
	commands := []subCommand{
		newGenPasswordCommand(),
		newGenSeedCommand(),
		newInitWalletCommand(),
		newLoadSecretCommand(),
		newStoreSecretCommand(),
		newStoreConfigmapCommand(),
		newWaitReadyCommand(),
	}

	for _, command := range commands {
		if err := command.Register(parser); err != nil {
			return err
		}
	}

	return nil
}

func asJSON(resp interface{}) (string, error) {
	b, err := json.Marshal(resp)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func readFile(fileName string) (string, error) {
	fileName = lncfg.CleanAndExpandPath(fileName)
	if !lnrpc.FileExists(fileName) {
		return "", fmt.Errorf("input file %s missing: %v", fileName,
			errInputMissing)
	}

	byteContent, err := os.ReadFile(fileName)
	if err != nil {
		return "", fmt.Errorf("error reading file %s: %v", fileName,
			err)
	}

	return string(byteContent), nil
}

func stripNewline(str string) string {
	return strings.TrimRight(strings.TrimRight(str, "\r\n"), "\n")
}
