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
