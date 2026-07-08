package main

import (
	"testing"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/sqlite"
	"github.com/lightningnetwork/lnd/lncfg"
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

// TestMigrateDBNoBboltDBExists verifies that running migrate-db against a data
// directory that contains no bbolt source DBs is a successful no-op by default
// and only fails when --error-if-no-bbolt-db-exists is set.
func TestMigrateDBNoBboltDBExists(t *testing.T) {
	t.Parallel()

	makeCmd := func(dataDir string, errIfMissing bool) *migrateDBCommand {
		return &migrateDBCommand{
			Source: &SourceDB{
				Backend: lncfg.BoltBackend,
				Bolt: &Bolt{
					DBTimeout: kvdb.DefaultDBTimeout,
					DataDir:   dataDir,
					TowerDir:  dataDir,
				},
			},
			Dest: &DestDB{
				Backend: lncfg.SqliteBackend,
				Sqlite: &Sqlite{
					DataDir:  dataDir,
					TowerDir: dataDir,
					Config:   &sqlite.Config{},
				},
			},
			Network:          "regtest",
			ChunkSize:        1024,
			ErrorIfNoBboltDB: errIfMissing,
		}
	}

	t.Run("default skips missing DBs", func(t *testing.T) {
		t.Parallel()

		// An empty temp dir contains no bbolt DBs at all. With the
		// default flag value the migration should succeed without
		// touching the destination.
		err := makeCmd(t.TempDir(), false).Execute(nil)
		require.NoError(t, err)
	})

	t.Run("flag forces error when missing", func(t *testing.T) {
		t.Parallel()

		err := makeCmd(t.TempDir(), true).Execute(nil)
		require.Error(t, err)
		require.Contains(
			t, err.Error(), "failed to open source db",
		)
	})
}
