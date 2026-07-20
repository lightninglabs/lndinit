//go:build kvdb_postgres

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/lightninglabs/lndinit/migratekvdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

const (
	testMaxConnections = 100
	testDsnTemplate    = "postgres://postgres:postgres@127.0.0.1:9877/%s?sslmode=disable"
)

// PostgresTestSetup holds the test configuration for Postgres.
type PostgresTestSetup struct {
	tempDir  string
	dbName   string
	postgres *embeddedpostgres.EmbeddedPostgres
	stopFunc func() error
}

// setupEmbeddedPostgres initializes and starts the embedded postgres instance.
func setupEmbeddedPostgres(t *testing.T) *PostgresTestSetup {
	sqlbase.Init(testMaxConnections)

	setup := &PostgresTestSetup{}

	// Initialize embedded postgres
	setup.postgres = embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(9877).
			StartParameters(map[string]string{
				"max_connections": fmt.Sprintf("%d", testMaxConnections),
			}),
	)

	// Start postgres
	err := setup.postgres.Start()
	require.NoError(t, err, "failed to start postgres")

	setup.stopFunc = setup.postgres.Stop

	return setup
}

// createTestDatabase creates a new random test database.
func (p *PostgresTestSetup) createTestDatabase(t *testing.T) {
	// Generate random database name
	randBytes := make([]byte, 8)
	_, err := rand.Read(randBytes)
	require.NoError(t, err)
	p.dbName = "test_" + hex.EncodeToString(randBytes)

	// Create the database
	dbConn, err := sql.Open("pgx", p.getDsn("postgres"))
	require.NoError(t, err)
	defer dbConn.Close()

	_, err = dbConn.ExecContext(
		context.Background(),
		"CREATE DATABASE "+p.dbName,
	)
	require.NoError(t, err)
}

// setupTestDir creates and sets up the temporary test directory.
func (p *PostgresTestSetup) setupTestDir(t *testing.T) {
	p.tempDir = setupTestData(t)
}

// getDsn returns the DSN for the specified database.
func (p *PostgresTestSetup) getDsn(dbName string) string {
	return fmt.Sprintf(testDsnTemplate, dbName)
}

// cleanup performs necessary cleanup.
func (p *PostgresTestSetup) cleanup() error {
	if p.stopFunc != nil {
		return p.stopFunc()
	}
	return nil
}

// getDBConfigs returns the source and destination DB configs.
func (p *PostgresTestSetup) getDBConfigs() (*SourceDB, *DestDB) {
	sourceDB := &SourceDB{
		Backend: lncfg.BoltBackend,
		Bolt: &Bolt{
			DBTimeout: kvdb.DefaultDBTimeout,
			DataDir:   p.tempDir,
			TowerDir:  p.tempDir,
		},
	}

	destDB := &DestDB{
		Backend: lncfg.PostgresBackend,
		Postgres: &postgres.Config{
			Dsn: p.getDsn(p.dbName),
		},
	}

	return sourceDB, destDB
}

// TestMigrateDBPostgres tests the migration of a database from Bolt to
// Postgres.
func TestMigrateDBPostgres(t *testing.T) {
	// Setup postgres.
	setup := setupEmbeddedPostgres(t)
	defer func() {
		require.NoError(t, setup.cleanup())
	}()

	// Setup test environment.
	setup.setupTestDir(t)
	setup.createTestDatabase(t)

	sourceDB, destDB := setup.getDBConfigs()

	// Create and run migration command.
	cmd := &migrateDBCommand{
		Source:    sourceDB,
		Dest:      destDB,
		Network:   "regtest",
		ChunkSize: 1024,
	}

	err := cmd.Execute(nil)
	require.NoError(t, err, "failed to execute migration")
}

// TestMigrateDBPostgresBulk tests the fresh-only bulk migration path from Bolt
// to Postgres through the migrate-db command wiring.
func TestMigrateDBPostgresBulk(t *testing.T) {
	// Setup postgres.
	setup := setupEmbeddedPostgres(t)
	defer func() {
		require.NoError(t, setup.cleanup())
	}()

	// Setup test environment.
	setup.setupTestDir(t)
	setup.createTestDatabase(t)

	sourceDB, destDB := setup.getDBConfigs()

	// Create and run migration command.
	cmd := &migrateDBCommand{
		Source:     sourceDB,
		Dest:       destDB,
		Network:    "regtest",
		ChunkSize:  1024,
		BulkWrites: true,
	}

	err := cmd.Execute(nil)
	require.NoError(t, err, "failed to execute bulk migration")
}

// TestPostgresBulkMigrationParity verifies that the standard and bulk paths
// produce identical Postgres KV databases from the same Bolt source.
func TestPostgresBulkMigrationParity(t *testing.T) {
	setup := setupEmbeddedPostgres(t)
	defer func() {
		require.NoError(t, setup.cleanup())
	}()

	setup.createTestDatabase(t)
	standardDBName := setup.dbName
	setup.createTestDatabase(t)
	bulkDBName := setup.dbName

	ctx := t.Context()
	sourceDB := newParitySourceDB(t)
	standardTarget := openParityTarget(
		t, ctx, setup.getDsn(standardDBName), false,
	)
	bulkTarget := openParityTarget(
		t, ctx, setup.getDsn(bulkDBName), true,
	)

	standardMigrator := newParityMigrator(t)
	require.NoError(t, standardMigrator.Migrate(
		ctx, sourceDB, standardTarget,
	))
	require.NoError(t, standardMigrator.VerifyMigration(
		ctx, sourceDB, standardTarget, false,
	))

	bulkMigrator := newParityMigrator(t)
	require.NoError(t, bulkTarget.bulk.migrate(
		ctx, bulkMigrator, sourceDB, false,
	))
	require.NoError(t, bulkTarget.bulk.verify(
		ctx, bulkMigrator, sourceDB,
	))

	require.NoError(t, compareKVDBs(sourceDB, standardTarget))
	require.NoError(t, compareKVDBs(sourceDB, bulkTarget))
	require.NoError(t, compareKVDBs(standardTarget, bulkTarget))
}

// newParitySourceDB creates a Bolt source containing the edge cases whose
// representation must match between the standard and bulk migration paths.
func newParitySourceDB(t *testing.T) kvdb.Backend {
	t.Helper()

	args := []interface{}{
		filepath.Join(t.TempDir(), "source.db"), true, time.Minute, false,
	}
	db, err := kvdb.Create(kvdb.BoltBackendName, args...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	err = db.Update(func(tx kvdb.RwTx) error {
		root, err := tx.CreateTopLevelBucket([]byte("root"))
		if err != nil {
			return err
		}
		if err := root.SetSequence(11); err != nil {
			return err
		}
		if err := root.Put([]byte("empty"), []byte{}); err != nil {
			return err
		}
		if err := root.Put(
			[]byte("binary"), []byte{0x00, 0xff, 0x01},
		); err != nil {
			return err
		}

		nested, err := root.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := nested.SetSequence(math.MaxUint64); err != nil {
			return err
		}
		if err := nested.Put(
			[]byte{0x00, 0xff}, []byte("nested-value"),
		); err != nil {
			return err
		}

		emptyBucket, err := root.CreateBucket([]byte("empty-bucket"))
		if err != nil {
			return err
		}

		return emptyBucket.SetSequence(5)
	}, func() {})
	require.NoError(t, err)

	return db
}

// openParityTarget opens an independent Postgres target for a parity test.
func openParityTarget(t *testing.T, ctx context.Context, dsn string,
	bulkWrites bool) *destinationBackend {

	t.Helper()

	target, err := openDestDb(ctx, &DestDB{
		Backend: lncfg.PostgresBackend,
		Postgres: &postgres.Config{
			Dsn: dsn,
		},
	}, "parity", "regtest", bulkWrites)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, target.Close())
	})

	return target
}

// newParityMigrator creates a migrator with isolated metadata state.
func newParityMigrator(t *testing.T) *migratekvdb.Migrator {
	t.Helper()

	metaDB, err := bbolt.Open(
		filepath.Join(t.TempDir(), "meta.db"), 0600, nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metaDB.Close())
	})

	migrator, err := migratekvdb.New(migratekvdb.Config{
		ChunkSize:    8,
		MetaDB:       metaDB,
		DBPrefixName: "parity",
	})
	require.NoError(t, err)

	return migrator
}

// compareKVDBs compares two KV databases symmetrically, including bucket
// types, bucket sequences, keys, values, and extra entries on either side.
func compareKVDBs(leftDB, rightDB kvdb.Backend) error {
	return leftDB.View(func(leftTx kvdb.RTx) error {
		return rightDB.View(func(rightTx kvdb.RTx) error {
			err := leftTx.ForEachBucket(func(name []byte) error {
				left := leftTx.ReadBucket(name)
				right := rightTx.ReadBucket(name)
				if right == nil {
					return fmt.Errorf("right database is missing root %x", name)
				}

				return compareKVBuckets(name, left, right)
			})
			if err != nil {
				return err
			}

			return rightTx.ForEachBucket(func(name []byte) error {
				if leftTx.ReadBucket(name) == nil {
					return fmt.Errorf("right database has extra root %x", name)
				}

				return nil
			})
		}, func() {})
	}, func() {})
}

// compareKVBuckets recursively compares two corresponding KV buckets.
func compareKVBuckets(path []byte, left, right kvdb.RBucket) error {
	if left.Sequence() != right.Sequence() {
		return fmt.Errorf("sequence mismatch at %x: left=%d right=%d",
			path, left.Sequence(), right.Sequence())
	}

	leftCursor := left.ReadCursor()
	rightCursor := right.ReadCursor()
	leftKey, leftValue := leftCursor.First()
	rightKey, rightValue := rightCursor.First()

	for leftKey != nil || rightKey != nil {
		if leftKey == nil {
			return fmt.Errorf("right bucket has extra key at %x: %x",
				path, rightKey)
		}
		if rightKey == nil {
			return fmt.Errorf("right bucket is missing key at %x: %x",
				path, leftKey)
		}
		if !bytes.Equal(leftKey, rightKey) {
			return fmt.Errorf("key mismatch at %x: left=%x right=%x",
				path, leftKey, rightKey)
		}

		leftNested := left.NestedReadBucket(leftKey)
		rightNested := right.NestedReadBucket(rightKey)
		switch {
		case leftNested != nil && rightNested != nil:
			nestedPath := append(append([]byte{}, path...), leftKey...)
			if err := compareKVBuckets(
				nestedPath, leftNested, rightNested,
			); err != nil {
				return err
			}

		case leftNested != nil || rightNested != nil:
			return fmt.Errorf("bucket/value type mismatch at %x key %x",
				path, leftKey)

		case !bytes.Equal(leftValue, rightValue):
			return fmt.Errorf("value mismatch at %x key %x", path, leftKey)
		}

		leftKey, leftValue = leftCursor.Next()
		rightKey, rightValue = rightCursor.Next()
	}

	return nil
}
