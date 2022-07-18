package main

import "github.com/jessevdk/go-flags"

type testSeedCommand struct {
	Output string `long:"output" short:"o" description:"Output format" choice:"raw" choice:"json"`
}

func newTestSeedCommand() *testSeedCommand {
	return &testSeedCommand{
		Output: outputFormatRaw,
	}
}

func (x *testSeedCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"test-seed",
		"Test if a seed is correct",
		"Tests whether a seed (and its optional passphrase) is "+
			"correct by deriving the node's public identity key",
		x,
	)
	return err
}

func (x *testSeedCommand) Execute(_ []string) error {
	return nil
}
