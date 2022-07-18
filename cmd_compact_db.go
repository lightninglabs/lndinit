package main

import "github.com/jessevdk/go-flags"

type compactDBCommand struct {
	Output string `long:"output" short:"o" description:"Output format" choice:"raw" choice:"json"`
}

func newCompactDBCommand() *compactDBCommand {
	return &compactDBCommand{
		Output: outputFormatRaw,
	}
}

func (x *compactDBCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"compact-db",
		"Compacts a bbolt based channel.db database file",
		"Runs the compaction process on a bbolt based channel "+
			"database file (channel.db) without the need to start "+
			"lnd; in fact, lnd needs to be shut down in order to "+
			"release the write-lock on the channel.db file that "+
			"should be compacted",
		x,
	)
	return err
}

func (x *compactDBCommand) Execute(_ []string) error {
	return nil
}
