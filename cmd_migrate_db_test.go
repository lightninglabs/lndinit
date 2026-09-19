package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// testDataPath is the source directory containing test data.
const testDataPath = "testdata/data"

// setupTestData creates a new temp directory and copies test data into it.
// It returns the path to the new temp directory.
func setupTestData(t *testing.T) string {
	// Create unique temp dir for this test.
	tempDir := t.TempDir()
	err := copyTestDataDir(testDataPath, tempDir)

	require.NoError(t, err, "failed to copy test data")

	return tempDir
}

// TestValidateBulkResetFlags verifies that destructive bulk reset
// authorization is only accepted for the bulk migration path.
func TestValidateBulkResetFlags(t *testing.T) {
	tests := []struct {
		name          string
		bulkWrites    bool
		forceNew      bool
		expectedError string
	}{
		{
			name:          "reset requires bulk writes",
			expectedError: "--reset-bulk-target requires --bulk-writes",
		},
		{
			name:       "reset conflicts with force new",
			bulkWrites: true,
			forceNew:   true,
			expectedError: "--reset-bulk-target cannot be combined " +
				"with --force-new-migration",
		},
		{
			name:       "bulk reset accepted",
			bulkWrites: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			cmd := newMigrateDBCommand()
			cmd.BulkWrites = test.bulkWrites
			cmd.ResetBulkTarget = true
			cmd.ForceNewMigration = test.forceNew

			err := cmd.validateDBBackends()
			if test.expectedError == "" {
				require.NoError(t, err)

				return
			}

			require.EqualError(t, err, test.expectedError)
		})
	}
}

// TestRedactDsn makes sure that no part of a database connection string that
// could hold a password survives into a log line, in either of the notations
// postgres accepts.
func TestRedactDsn(t *testing.T) {
	t.Parallel()

	const password = "sup3rs3cr3t"

	tests := []struct {
		name     string
		dsn      string
		expected string
	}{{
		name:     "empty dsn",
		dsn:      "",
		expected: "",
	}, {
		name:     "url with password",
		dsn:      "postgres://alice:" + password + "@localhost:5432/lnd",
		expected: "postgres://alice@localhost:5432/lnd",
	}, {
		name:     "url without password",
		dsn:      "postgres://alice@localhost:5432/lnd",
		expected: "postgres://alice@localhost:5432/lnd",
	}, {
		name:     "url without credentials",
		dsn:      "postgres://localhost:5432/lnd",
		expected: "postgres://localhost:5432/lnd",
	}, {
		// The allow-listed query parameters are kept, since knowing
		// them helps when debugging a connection problem.
		name: "url with allowed query parameters",
		dsn: "postgresql://alice:" + password + "@localhost:5432/" +
			"lnd?sslmode=disable",
		expected: "postgresql://alice@localhost:5432/lnd?" +
			"sslmode=disable",
	}, {
		// A password can also be smuggled in as a query parameter,
		// which is why everything that isn't allow-listed is dropped.
		name: "url with password query parameter",
		dsn: "postgres://alice@localhost:5432/lnd?password=" +
			password + "&sslmode=require",
		expected: "postgres://alice@localhost:5432/lnd?" +
			"sslmode=require",
	}, {
		name: "keyword value dsn",
		dsn: "host=localhost port=5432 user=alice password=" +
			password + " dbname=lnd sslmode=disable",
		expected: "host=localhost port=5432 user=alice dbname=lnd " +
			"sslmode=disable",
	}, {
		// A quoted value containing spaces is split into several
		// fields by the tokenization, but because only allow-listed
		// keys are kept, the fragments are dropped instead of logged.
		name: "keyword value dsn with quoted password",
		dsn: "host=localhost password='" + password + " with " +
			"spaces' dbname=lnd",
		expected: "host=localhost dbname=lnd",
	}, {
		name:     "keyword value dsn with only secrets",
		dsn:      "password=" + password,
		expected: redactedDsn,
	}, {
		name:     "unparsable url",
		dsn:      "postgres://alice:" + password + "@loc alhost/lnd",
		expected: redactedDsn,
	}}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			redacted := redactDsn(test.dsn)
			require.Equal(t, test.expected, redacted)
			require.NotContains(t, redacted, password)
		})
	}
}
