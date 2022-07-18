package main

import "github.com/jessevdk/go-flags"

type testWalletCommand struct {
	Output string `long:"output" short:"o" description:"Output format" choice:"raw" choice:"json"`
}

func newTestWalletCommand() *testWalletCommand {
	return &testWalletCommand{
		Output: outputFormatRaw,
	}
}

func (x *testWalletCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"test-wallet",
		"Test a wallet password and show its node identity public key",
		"Tests whether a wallet file (wallet.db) can be correctly "+
			"decrypted with the given password; then prints the "+
			"node's identity public key the wallet was created "+
			"for, if decryption was successful",
		x,
	)
	return err
}

func (x *testWalletCommand) Execute(_ []string) error {
	return nil
}
