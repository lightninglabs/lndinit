package migratekvdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/stretchr/testify/require"
)

// TestMigration tests the migration of a test database including the
// verification of the migration.
func TestMigration(t *testing.T) {
	// Create temporary directory for test databases.
	tempDir, err := os.MkdirTemp("", "boltdb_migration_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create source and target database paths.
	sourceDBPath := filepath.Join(tempDir, "source.db")
	targetDBPath := filepath.Join(tempDir, "target.db")

	// Create and populate source database.
	sourceDB, err := createTestDatabase(sourceDBPath)
	require.NoError(t, err)
	defer sourceDB.Close()

	// Cleanup the test database files.
	defer os.Remove(sourceDBPath)
	defer os.Remove(targetDBPath)

	const (
		noFreelistSync = true
		timeout        = time.Minute
	)

	args := []interface{}{
		targetDBPath, noFreelistSync, timeout,
	}
	backend := kvdb.BoltBackendName

	// Create empty target database.
	targetDB, err := walletdb.Create(backend, args...)
	require.NoError(t, err)
	defer targetDB.Close()

	consoleLogHandler := btclog.NewDefaultHandler(
		os.Stdout,
	)
	consoleLogger := btclog.NewSLogger(consoleLogHandler)
	consoleLogger.SetLevel(btclog.LevelDebug)

	// Configure and run migration.
	cfg := Config{
		// Chunksize in bytes.
		ChunkSize: 2,
		Logger:    consoleLogger,
	}

	migrator, err := New(cfg)
	require.NoError(t, err)

	err = migrator.Migrate(context.Background(), sourceDB, targetDB)
	require.NoError(t, err)

	err = migrator.VerifyMigration(context.Background(), targetDB)
	require.NoError(t, err)

	// Verify migration by comparing values in the source and target
	// databases as a sanity check that the previous hash verification has
	// no errors.
	err = verifyDatabases(t, sourceDB, targetDB)
	require.NoError(t, err)
}

// createTestDatabase creates a test database with some test data.
func createTestDatabase(dbPath string) (walletdb.DB, error) {
	const (
		noFreelistSync = true
		timeout        = time.Minute
	)
	fmt.Println("creating test database")

	args := []interface{}{
		dbPath, noFreelistSync, timeout,
	}
	backend := kvdb.BoltBackendName
	db, err := walletdb.Create(backend, args...)
	if err != nil {
		return nil, err
	}

	// Create test data structure.
	err = db.Update(func(tx walletdb.ReadWriteTx) error {
		fmt.Println("Creating test data structure...")
		// Create root bucket "accounts".
		accounts, err := tx.CreateTopLevelBucket([]byte("accounts"))
		if err != nil {
			fmt.Print("bucket creation failed.")
		}

		// Create nested buckets and add some key-value pairs.
		for i := 1; i <= 3; i++ {
			userBucket, err := accounts.CreateBucketIfNotExists(
				[]byte("user" + strconv.Itoa(i)),
			)
			if err != nil {
				return err
			}

			err = userBucket.Put([]byte("name"), []byte("Alice"))
			if err != nil {
				return err
			}

			err = userBucket.Put(
				[]byte("email"),
				[]byte("alice@example.com"),
			)
			if err != nil {
				return err
			}

			// Create a nested bucket for transactions.
			txBucket, err := userBucket.CreateBucketIfNotExists(
				[]byte("transactions"),
			)
			if err != nil {
				return err
			}

			err = txBucket.Put([]byte("tx1"), []byte("100 BTC"))
			if err != nil {
				return err
			}
		}

		return nil
	}, func() {})

	return db, err
}

// verifyDatabases verifies the migration by comparing the values in the
// source and target databases. This checks every value to make sure we do not
// have an error in our resume logic. So it walks the entire database without
// any chunking, so we have a redundant check.
func verifyDatabases(t *testing.T, sourceDB, targetDB walletdb.DB) error {
	return sourceDB.View(func(sourceTx walletdb.ReadTx) error {
		return targetDB.View(func(targetTx walletdb.ReadTx) error {
			// Helper function to compare buckets recursively.
			var compareBuckets func(source, target walletdb.ReadBucket) error
			compareBuckets = func(source, target walletdb.ReadBucket) error {
				// Compare all key-value pairs.
				return source.ForEach(func(k, v []byte) error {
					if v == nil {
						// This is a nested bucket.
						sourceBucket := source.NestedReadBucket(k)
						targetBucket := target.NestedReadBucket(k)
						require.NotNil(t, targetBucket)
						return compareBuckets(sourceBucket, targetBucket)
					}

					// This is a key-value pair.
					targetValue := target.Get(k)
					require.Equal(t, v, targetValue)
					return nil
				})
			}

			// Compare root buckets.
			return sourceTx.ForEachBucket(func(name []byte) error {
				sourceBucket := sourceTx.ReadBucket(name)
				targetBucket := targetTx.ReadBucket(name)
				require.NotNil(t, targetBucket)
				return compareBuckets(sourceBucket, targetBucket)
			})
		}, func() {})
	}, func() {})
}
