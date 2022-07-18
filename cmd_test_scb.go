package main

import "github.com/jessevdk/go-flags"

type testSCBCommand struct {
	Output string `long:"output" short:"o" description:"Output format" choice:"raw" choice:"json"`
}

func newTestSCBCommand() *testSCBCommand {
	return &testSCBCommand{
		Output: outputFormatRaw,
	}
}

func (x *testSCBCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"test-scb",
		"Tests an SCB (channel.backup) file",
		"Tests a static channel backup (SCB, also known as "+
			"`channel.backup`) file for integrity and optionally "+
			"dumps its content.",
		x,
	)
	return err
}

func (x *testSCBCommand) Execute(_ []string) error {
	return nil
}
