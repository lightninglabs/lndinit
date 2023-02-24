package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testSeedWithNewline     = []byte("seed phrase with newline\n")
	testPasswordWithNewline = []byte("p4ssw0rd\r\n\n\r\r\n")
)

// TestReadInput makes sure input files are always trimmed so we don't have any
// newline characters left over.
func TestReadInput(t *testing.T) {
	cmd := newInitWalletCommand()

	cmd.File.Seed = writeToTempFile(t, testSeedWithNewline)
	cmd.File.WalletPassword = writeToTempFile(t, testPasswordWithNewline)

	seed, seedPassphrase, walletPassword, err := cmd.readInput(true)
	require.NoError(t, err)
	require.Equal(t, "seed phrase with newline", seed)
	require.Equal(t, "", seedPassphrase)
	require.Equal(t, "p4ssw0rd", walletPassword)
}

func writeToTempFile(t *testing.T, data []byte) string {
	tempFileName, err := os.CreateTemp("", "*.txt")
	require.NoError(t, err)

	err = os.WriteFile(tempFileName.Name(), data, 0600)
	require.NoError(t, err)

	return tempFileName.Name()
}
