//go:build kvdb_sqlite

package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/lightninglabs/lndinit/migratekvdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/sqlite"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// openSqliteTarget opens a fresh sqlite kvdb backend in dir for the given prefix.
func openSqliteTarget(t *testing.T, dir, prefix string) kvdb.Backend {
	t.Helper()
	sqlbase.Init(0)

	target, err := kvdb.Open(
		kvdb.SqliteBackendName, context.Background(),
		&sqlite.Config{Timeout: time.Minute}, dir, "test.sqlite", prefix,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	return target
}

// TestMigrateEdgeCasesSqlite migrates an edge-case bolt DB into sqlite via the
// normal (per-key) migrate path and verifies with the streaming ForAll path.
// This is the regression test for the SQLite empty-value bug: sqlite decodes an
// empty stored value as nil, and the ForAll verify must not mistake that for a
// bucket/type mismatch.
func TestMigrateEdgeCasesSqlite(t *testing.T) {
	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "source.db")

	counts := buildEdgeCaseBoltSource(t, srcPath)
	require.Greater(t, counts.leaves, 0)

	src, err := kvdb.Open(
		kvdb.BoltBackendName, srcPath, true, time.Minute, true,
	)
	require.NoError(t, err)
	defer src.Close()

	target := openSqliteTarget(t, tempDir, "channeldb")

	metaDB, err := bbolt.Open(filepath.Join(tempDir, "meta.db"), 0600, nil)
	require.NoError(t, err)
	defer metaDB.Close()

	logger := btclog.NewSLogger(btclog.NewDefaultHandler(discardWriter{}))
	migrator, err := migratekvdb.New(migratekvdb.Config{
		Logger:       logger,
		MetaDB:       metaDB,
		DBPrefixName: "channeldb",
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Migrate (per-key path), then verify via ForAll.
	require.NoError(t, migrator.Migrate(ctx, src, target))
	require.NoError(t, migrator.VerifyMigration(ctx, src, target, false),
		"ForAll verify must accept sqlite empty-value leaves")

	// Independent byte-for-byte cross-check.
	compareBackends(t, src, target)
}
