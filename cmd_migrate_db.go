package main

import "github.com/jessevdk/go-flags"

type migrateDBCommand struct {
	Output string `long:"output" short:"o" description:"Output format" choice:"raw" choice:"json"`
}

func newMigrateDBCommand() *migrateDBCommand {
	return &migrateDBCommand{
		Output: outputFormatRaw,
	}
}

func (x *migrateDBCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"migrate-db",
		"Applies all schema migrations on a bbolt based channel.db "+
			"database file",
		"Runs all pending schema migrations on a bbolt based channel "+
			"database file (channel.db) without the need to start "+
			"lnd; in fact, lnd needs to be shut down in order to "+
			"release the write-lock on the channel.db file that "+
			"should be compacted; NOTE: Running this command will "+
			"make it impossible to downgrade lnd to a version "+
			"before '"+LNDVersion+"'!",
		x,
	)
	return err
}

func (x *migrateDBCommand) Execute(_ []string) error {
	return nil
}
