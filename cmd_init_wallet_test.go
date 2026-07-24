package main

import (
	"fmt"
	"os"
	"testing"
	"time"

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

// TestParseWatchOnlyBirthday makes sure the birthday of a watch-only wallet can
// be given in any of the supported notations and that nonsensical values are
// rejected instead of silently turning into a full chain rescan.
func TestParseWatchOnlyBirthday(t *testing.T) {
	t.Parallel()

	// A couple of reference points, expressed both as a timestamp and as
	// the string we expect the parser to accept.
	const (
		// mainnetGenesis is the timestamp of the mainnet genesis block.
		mainnetGenesis = 1231006505

		// regtestGenesis is the timestamp of the regtest genesis block.
		regtestGenesis = 1296688602
	)

	now := time.Now()

	testCases := []struct {
		name        string
		birthday    string
		network     string
		expected    uint64
		expectedErr string
	}{{
		name:     "empty means unknown",
		birthday: "",
		network:  "mainnet",
		expected: 0,
	}, {
		name:     "unix timestamp in seconds",
		birthday: "1719792000",
		network:  "mainnet",
		expected: 1719792000,
	}, {
		name:     "rfc3339 timestamp",
		birthday: "2024-07-01T00:00:00Z",
		network:  "mainnet",
		expected: 1719792000,
	}, {
		name:     "rfc3339 timestamp with offset",
		birthday: "2024-07-01T02:00:00+02:00",
		network:  "mainnet",
		expected: 1719792000,
	}, {
		name:     "calendar date is midnight utc",
		birthday: "2024-07-01",
		network:  "mainnet",
		expected: 1719792000,
	}, {
		name:     "the genesis block itself",
		birthday: fmt.Sprintf("%d", mainnetGenesis),
		network:  "mainnet",
		expected: mainnetGenesis,
	}, {
		name:     "now is fine",
		birthday: fmt.Sprintf("%d", now.Unix()),
		network:  "mainnet",
		expected: uint64(now.Unix()),
	}, {
		name:     "slight clock skew is tolerated",
		birthday: fmt.Sprintf("%d", now.Add(time.Hour).Unix()),
		network:  "mainnet",
		expected: uint64(now.Add(time.Hour).Unix()),
	}, {
		name:        "zero is before genesis",
		birthday:    "0",
		network:     "mainnet",
		expectedErr: "before the mainnet genesis block time",
	}, {
		name:        "negative timestamp",
		birthday:    "-1719792000",
		network:     "mainnet",
		expectedErr: "before the mainnet genesis block time",
	}, {
		name:        "date before genesis",
		birthday:    "2008-10-31",
		network:     "mainnet",
		expectedErr: "before the mainnet genesis block time",
	}, {
		name:        "milliseconds instead of seconds",
		birthday:    "1719792000000",
		network:     "mainnet",
		expectedErr: "in the future",
	}, {
		name:        "far future date",
		birthday:    now.AddDate(1, 0, 0).Format(time.RFC3339),
		network:     "mainnet",
		expectedErr: "in the future",
	}, {
		name:        "not a timestamp or date",
		birthday:    "yesterday",
		network:     "mainnet",
		expectedErr: "is neither a Unix timestamp",
	}, {
		name:        "date with slashes",
		birthday:    "2024/07/01",
		network:     "mainnet",
		expectedErr: "is neither a Unix timestamp",
	}, {
		name:        "unknown network",
		birthday:    "2024-07-01",
		network:     "fakenet",
		expectedErr: "unknown network",
	}, {
		// The genesis block of each network has its own timestamp, so
		// the lower bound has to follow the network.
		name:     "regtest genesis",
		birthday: fmt.Sprintf("%d", regtestGenesis),
		network:  "regtest",
		expected: regtestGenesis,
	}, {
		name:        "mainnet date before regtest genesis",
		birthday:    "2010-07-01",
		network:     "regtest",
		expectedErr: "before the regtest genesis block time",
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			birthday, err := parseWatchOnlyBirthday(
				tc.birthday, tc.network,
			)

			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				require.Zero(t, birthday)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, birthday)
		})
	}
}

// TestWatchOnlyBirthdayRequiresWatchOnly makes sure we don't silently ignore a
// birthday that was given for a wallet that isn't initialized as a watch-only
// one through RPC, which is the only combination that can act on it.
func TestWatchOnlyBirthdayRequiresWatchOnly(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		initType  string
		watchOnly bool
	}{{
		name:      "rpc without watch-only",
		initType:  typeRpc,
		watchOnly: false,
	}, {
		name:      "file init",
		initType:  typeFile,
		watchOnly: false,
	}, {
		// The file based init doesn't look at any of the RPC flags, so
		// the watch-only flag being set alongside it doesn't make the
		// birthday reachable either.
		name:      "file init with the watch-only flag set",
		initType:  typeFile,
		watchOnly: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cmd := newInitWalletCommand()
			cmd.InitType = tc.initType
			cmd.InitRpc.WatchOnly = tc.watchOnly
			cmd.InitRpc.WatchOnlyBirthday = "2024-07-01"

			err := cmd.Execute(nil)
			require.ErrorContains(
				t, err, "can only be used in combination with",
			)
		})
	}
}

func writeToTempFile(t *testing.T, data []byte) string {
	tempFileName, err := os.CreateTemp("", "*.txt")
	require.NoError(t, err)

	err = os.WriteFile(tempFileName.Name(), data, 0600)
	require.NoError(t, err)

	return tempFileName.Name()
}
