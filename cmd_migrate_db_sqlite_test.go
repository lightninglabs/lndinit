//go:build kvdb_sqlite

package main

import (
	"testing"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/sqlite"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/stretchr/testify/require"
)

// TestMigrateDBSqlite tests the migration of a database from Bolt to SQLite.
func TestMigrateDBSqlite(t *testing.T) {
	t.Parallel()

	// Create temp dir for test databases.
	tempDir := setupTestData(t)

	// Copy entire test directory structure.
	err := copyTestDataDir("testdata/data", tempDir)
	require.NoError(t, err, "failed to copy test data")

	// Set up source DB config (bolt).
	sourceDB := &SourceDB{
		Backend: lncfg.BoltBackend,
		Bolt: &Bolt{
			DBTimeout: kvdb.DefaultDBTimeout,
			DataDir:   tempDir,
			TowerDir:  tempDir,
		},
	}

	// Set up destination DB config (sqlite).
	destDB := &DestDB{
		Backend: lncfg.SqliteBackend,
		Sqlite: &Sqlite{
			DataDir:  tempDir,
			TowerDir: tempDir,
			Config:   &sqlite.Config{},
		},
	}

	// Create and run migration command.
	cmd := &migrateDBCommand{
		Source:  sourceDB,
		Dest:    destDB,
		Network: "regtest",
		// Select a small chunk size to test the chunking.
		ChunkSize: 1024,
	}

	err = cmd.Execute(nil)

	require.NoError(t, err, "migration failed")
}
