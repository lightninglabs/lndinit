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
