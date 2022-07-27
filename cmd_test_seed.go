package main

import "github.com/jessevdk/go-flags"

type testSeedCommand struct {
	Network      string                `long:"network" description:"The Bitcoin network to interpret the seed for, required for deriving the correct node identity key" choice:"mainnet" choice:"testnet" choice:"testnet3" choice:"regtest" choice:"simnet"`
	SecretSource string                `long:"secret-source" description:"Where to read the seed and its optional passphrase from" choice:"terminal" choice:"file" choice:"k8s" choice:"env"`
	File         *seedSecretSourceFile `group:"Flags for reading the secrets from files (use when --secret-source=file)" namespace:"file"`
	K8s          *seedSecretSourceK8s  `group:"Flags for reading the secrets from Kubernetes (use when --secret-source=k8s)" namespace:"k8s"`
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
