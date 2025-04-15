//go:build kvdb_postgres

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/stretchr/testify/require"
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
	t.Parallel()

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
