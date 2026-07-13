//go:build kvdb_postgres

package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/lightninglabs/lndinit/migratekvdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

const pgEdgeTable = "channeldb_kv"

// pgEdgeEnv bundles a fresh postgres target for one (sub)test.
type pgEdgeEnv struct {
	src    kvdb.Backend
	target kvdb.Backend
	raw    *sql.DB
	meta   *bbolt.DB
}

// newPGEdgeEnv builds an edge-case bolt source and a fresh postgres target DB.
func newPGEdgeEnv(t *testing.T, setup *PostgresTestSetup) (*pgEdgeEnv, edgeCaseCounts) {
	t.Helper()
	setup.createTestDatabase(t)
	dsn := setup.getDsn(setup.dbName)

	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "source.db")
	counts := buildEdgeCaseBoltSource(t, srcPath)

	src, err := kvdb.Open(
		kvdb.BoltBackendName, srcPath, true, time.Minute, true,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })

	target, err := kvdb.Open(
		kvdb.PostgresBackendName, context.Background(),
		&postgres.Config{
			Dsn: dsn, Timeout: time.Minute, MaxConnections: 10,
		},
		"channeldb",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	raw, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	meta, err := bbolt.Open(filepath.Join(tempDir, "meta.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close() })

	return &pgEdgeEnv{src: src, target: target, raw: raw, meta: meta}, counts
}

func newTestMigrator(t *testing.T, meta *bbolt.DB) *migratekvdb.Migrator {
	t.Helper()
	m, err := migratekvdb.New(migratekvdb.Config{
		Logger:       btclog.NewSLogger(btclog.NewDefaultHandler(discardWriter{})),
		MetaDB:       meta,
		DBPrefixName: "channeldb",
	})
	require.NoError(t, err)
	return m
}

// TestBulkMigratePostgres covers the batched bulk write path end-to-end on edge
// cases, plus guards, batch clamping, negative verification, and crash recovery.
func TestBulkMigratePostgres(t *testing.T) {
	setup := setupEmbeddedPostgres(t)
	defer func() { require.NoError(t, setup.cleanup()) }()

	ctx := context.Background()

	// --- happy path: bulk migrate + ForAll verify + independent compare ---
	t.Run("happy_path", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)
		m := newTestMigrator(t, env.meta)

		require.NoError(t, m.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))
		require.NoError(t, m.VerifyMigration(ctx, env.src, env.target, false))
		compareBackends(t, env.src, env.target)
	})

	// --- batched verify: correctness, with a tiny batch size to exercise
	//     batch boundaries and multiple BFS levels ---
	t.Run("batched_verify", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)
		m := newTestMigrator(t, env.meta)

		require.NoError(t, m.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))
		require.NoError(t, m.VerifyBatchedSQL(ctx, env.src, env.raw, pgEdgeTable, 2))
		compareBackends(t, env.src, env.target)
	})

	// --- batched verify must also detect target corruption ---
	t.Run("batched_verify_catches_mutation", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)
		m := newTestMigrator(t, env.meta)
		require.NoError(t, m.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))

		_, err := env.raw.ExecContext(ctx, "UPDATE "+pgEdgeTable+
			" SET value = '\\xdeadbeef' WHERE id = (SELECT id FROM "+
			pgEdgeTable+" WHERE value IS NOT NULL ORDER BY id LIMIT 1)")
		require.NoError(t, err)

		require.Error(t, m.VerifyBatchedSQL(ctx, env.src, env.raw, pgEdgeTable, 2),
			"batched verify should detect a mutated value")
	})

	// --- batched verify must detect an extra top-level bucket in the target
	//     (a check the sqlite-combining compareDBs path deliberately omits) ---
	t.Run("batched_verify_catches_extra_root", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)
		m := newTestMigrator(t, env.meta)
		require.NoError(t, m.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))

		_, err := env.raw.ExecContext(ctx, "INSERT INTO "+pgEdgeTable+
			" (parent_id, key) VALUES (NULL, '\\xfefefefe')")
		require.NoError(t, err)

		require.Error(t, m.VerifyBatchedSQL(ctx, env.src, env.raw, pgEdgeTable, 2),
			"batched verify should detect an extra top-level bucket")
	})

	// --- batch size larger than the param limit must clamp, not fail ---
	t.Run("batch_clamp", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)
		m := newTestMigrator(t, env.meta)

		// 100000 rows * 3 params would blow past 65535; must be clamped.
		require.NoError(t, m.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 100000))
		require.NoError(t, m.VerifyMigration(ctx, env.src, env.target, false))
	})

	// --- refuse to bulk-migrate into a non-empty target ---
	t.Run("refuse_non_empty", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)
		m1 := newTestMigrator(t, env.meta)
		require.NoError(t, m1.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))

		// A brand-new migrator (fresh meta) pointed at the now-populated
		// target must refuse rather than double-write.
		meta2, err := bbolt.Open(filepath.Join(t.TempDir(), "m2.db"), 0600, nil)
		require.NoError(t, err)
		defer meta2.Close()
		m2 := newTestMigrator(t, meta2)

		err = m2.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not empty")
	})

	// --- crash recovery: an interrupted attempt leaves the in-progress
	//     marker; a re-run must truncate and complete successfully ---
	t.Run("recovery_after_interrupt", func(t *testing.T) {
		env, _ := newPGEdgeEnv(t, setup)

		// First attempt: cancel the context so it fails AFTER writing the
		// in-progress marker but before/at the data transaction.
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		m1 := newTestMigrator(t, env.meta)
		err := m1.MigrateBulkSQL(cancelledCtx, env.src, env.raw, pgEdgeTable, 0)
		require.Error(t, err, "cancelled attempt should fail")

		// Second attempt with a fresh migrator on the SAME meta DB: it
		// should detect the marker, truncate, and complete.
		m2 := newTestMigrator(t, env.meta)
		require.NoError(t, m2.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))
		require.NoError(t, m2.VerifyMigration(ctx, env.src, env.target, false))
		compareBackends(t, env.src, env.target)
	})

	// --- verification must CATCH target corruption (proves it isn't a no-op) ---
	corruptCases := map[string]string{
		"missing_row": "DELETE FROM " + pgEdgeTable +
			" WHERE id = (SELECT id FROM " + pgEdgeTable +
			" WHERE value IS NOT NULL ORDER BY id LIMIT 1)",
		"mutated_value": "UPDATE " + pgEdgeTable +
			" SET value = '\\xdeadbeef' WHERE id = (SELECT id FROM " +
			pgEdgeTable + " WHERE value IS NOT NULL ORDER BY id LIMIT 1)",
		"extra_row": "INSERT INTO " + pgEdgeTable +
			" (parent_id, key, value) SELECT parent_id, '\\xffffffff', " +
			"'\\x00' FROM " + pgEdgeTable +
			" WHERE value IS NOT NULL ORDER BY id LIMIT 1",
	}
	for name, stmt := range corruptCases {
		t.Run("verify_catches_"+name, func(t *testing.T) {
			env, _ := newPGEdgeEnv(t, setup)
			m := newTestMigrator(t, env.meta)
			require.NoError(t, m.MigrateBulkSQL(ctx, env.src, env.raw, pgEdgeTable, 0))

			_, err := env.raw.ExecContext(ctx, stmt)
			require.NoError(t, err, "corruption statement failed")

			// Verification is fresh (bulk only wrote migration state),
			// so this exercises the full compare and must fail.
			require.Error(t, m.VerifyMigration(ctx, env.src, env.target, false),
				"verification should detect %s", name)
		})
	}
}
