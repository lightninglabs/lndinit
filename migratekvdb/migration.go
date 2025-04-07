package migratekvdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/kvdb"
	"go.etcd.io/bbolt"
)

// Database bucket and key constants.
const (
	// migrationMetaBucket is the top-level bucket that stores the migration
	// metadata.
	migrationMetaBucket = "migration_meta"

	verificationMetaBucket = "verification_meta"

	// migrationStateKey is the key of the current state of the migration.
	migrationStateKey = "migration_state"

	// verificationStateKey is the key of the verification state of the
	// migration.
	verificationStateKey = "verification_state"
)

// Size constants.
const (
	// DefaultChunkSize is 20MB (leaving room for overhead).
	DefaultChunkSize = 20 * 1024 * 1024

	// MaxChunkSize is 500MB to prevent excessive memory usage.
	MaxChunkSize = 500 * 1024 * 1024
)

// Config holds the configuration for the migrator.
type Config struct {
	// ChunkSize is the number of items (key-value pairs or buckets) to
	// process in a single transaction in bytes.
	ChunkSize uint64

	// Logger is the logger to use for logging.
	Logger btclog.Logger

	// MetaDB is the database that stores the migration/verification state.
	// We store it separately to not clutter the source or destination
	// databases.
	MetaDB *bbolt.DB

	// DBPrefixName is the prefix of the database name.
	DBPrefixName string
}

// validateConfig ensures the configuration is valid and sets defaults.
func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Validate and set defaults for chunk size.
	switch {
	case cfg.ChunkSize == 0:
		cfg.ChunkSize = DefaultChunkSize

		cfg.Logger.Infof("Chunk size set to default: %d bytes",
			cfg.ChunkSize)

	case cfg.ChunkSize > MaxChunkSize:
		return fmt.Errorf("chunk size too large: %d bytes, maximum "+
			"is %d bytes", cfg.ChunkSize, MaxChunkSize)
	}

	// Ensure logger is set.
	if cfg.Logger == nil {
		cfg.Logger = btclog.Disabled
	}

	return nil
}

// Migrator handles the chunked migration of bolt databases. It supports:
//   - Resumable migrations through state tracking
//   - Chunk-based processing to handle large databases
//   - Verification of migrated data (also resumable)
//   - Progress logging
type Migrator struct {
	// cfg is the configuration for the migrator.
	cfg Config

	// migration is the migration state which is used to track and also
	// resume the migration.
	migration *MigrationState

	// verification is the verification state which is used to track and
	// also resume the verification.
	verification *VerificationState

	// Rate tracking fields to track the progress of the migration and
	// verification.
	startTimeMigration     time.Time
	startTimeVerification  time.Time
	lastLogTime            time.Time
	logInterval            time.Duration
	lastMigratedKeyCount   int64
	lastVerifiedKeyCount   int64
	migratedKeysSinceStart int64
	verifiedKeysSinceStart int64
}

// New creates a new Migrator with the given configuration.
func New(cfg Config) (*Migrator, error) {
	// Validate and set defaults for the config.
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Migrator{
		cfg:                cfg,
		startTimeMigration: time.Now(),
		lastLogTime:        time.Now(),
		logInterval:        5 * time.Second,
		migration:          newMigrationState(cfg.MetaDB),
		verification:       newVerificationState(cfg.MetaDB),
	}, nil
}

// checkBucketMigrated checks if a bucket have already been processed
// in our depth-first traversal based on the current path we're processing.
// In also returns the next bucket to fast-forward so that we can immediately
// resume the migration from the same point we left off.
//
// NOTE: This works because we can be sure that the nested bucket paths are
// lexicographically ordered and are always walked through in the same order.
func checkBucketMigrated(currentPath BucketPath,
	checkPath BucketPath) (bool, []byte) {
	// Find the minimum length of the paths.
	minLen := min(len(currentPath.RawPath), len(checkPath.RawPath))

	// Compare each bucket's raw bytes.
	for i := range minLen {
		// Compare the raw bytes of each bucket
		comp := bytes.Compare(
			checkPath.RawPath[i], currentPath.RawPath[i],
		)
		if comp != 0 {
			// If buckets differ, path is migrated if it's
			// lexicographically smaller.
			return comp < 0, nil
		}
	}

	// If we get here, paths were equal up to minLen.
	// We want to return the next bucket after the current path
	// so we can fast-forward to the same point we left off.
	if len(currentPath.RawPath) > minLen {
		// Return the next bucket after the current path at minLen
		// level.
		return false, currentPath.RawPath[minLen]
	}

	// If current path is shorter or equal we are not done yet.
	return false, nil
}

// initMigrationState reads and initializes the migration state.
func (m *Migrator) initMigrationState(_ context.Context) error {
	err := m.migration.read()
	if err != nil && !errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound) {
		return fmt.Errorf("failed to read migration "+
			"state: %w", err)
	}

	// In case the migration is alredy complete we return an error.
	if m.migration.persistedState.Finished {
		m.cfg.Logger.Infof("Migration already finished at time: %v",
			m.migration.persistedState.FinishedTime.Format(time.RFC3339))

		return errMigrationComplete
	}

	// We do not have a bucket path yet set so this is a fresh migration.
	//
	// NOTE: There is a chance that the user deletes the meta db and starts
	// a new migration. There is currently no check that the destination db
	// is empty so it will just overwrite the existing data. However that
	// is not a problem because the verification will still
	if !m.migration.persistedState.CurrentBucketPath.HasPath() {
		m.migration.persistedState = newPersistedState()

		m.cfg.Logger.Infof("No previous migration state found, " +
			"starting fresh")

		return nil
	}

	// otherwise we have a state to resume from.
	// Otherwise we are ready to resume the migration.
	m.migration.resuming = true
	m.cfg.Logger.Infof("Resuming migration from "+
		"path %v, total processed keys: %d",
		m.migration.persistedState.CurrentBucketPath,
		m.migration.persistedState.ProcessedKeys)

	return nil
}

// logProgress logs the progress of the migration.
func (m *Migrator) logMigrationProgress(path BucketPath) {
	if time.Since(m.lastLogTime) < m.logInterval {
		return
	}

	totalKeys := m.migration.persistedState.ProcessedKeys
	totalBuckets := m.migration.persistedState.ProcessedBuckets
	elapsed := time.Since(m.startTimeMigration)
	intervalElapsed := time.Since(m.lastLogTime)

	intervalKeys := m.migratedKeysSinceStart - m.lastMigratedKeyCount
	currentRate := float64(intervalKeys) / intervalElapsed.Seconds()

	m.cfg.Logger.Infof("Migration progress at path %v: "+
		"processed %d keys total, %d buckets total "+
		"(current rate %.2f keys/sec), elapsed: %v",
		path, totalKeys, totalBuckets, currentRate,
		elapsed.Round(time.Second))

	m.lastLogTime = time.Now()
	m.lastMigratedKeyCount = m.migratedKeysSinceStart
}

// logProgress logs the progress of the migration.
func (m *Migrator) logVerificationProgress(path BucketPath) {
	if time.Since(m.lastLogTime) < m.logInterval {
		return
	}

	totalMigratedKeys := m.migration.persistedState.ProcessedKeys

	totalKeys := m.verification.persistedState.ProcessedKeys
	totalBuckets := m.verification.persistedState.ProcessedBuckets
	elapsed := time.Since(m.startTimeVerification)
	intervalElapsed := time.Since(m.lastLogTime)

	intervalKeys := m.verifiedKeysSinceStart - m.lastVerifiedKeyCount
	currentRate := float64(intervalKeys) / intervalElapsed.Seconds()

	// Calculate percentage of total keys from migration.
	percentComplete := float64(totalKeys) / float64(totalMigratedKeys) * 100

	m.cfg.Logger.Infof("Verification progress at path %v: "+
		"processed %d keys total, %d buckets total "+
		"(current rate %.2f keys/sec, %.1f%% complete), elapsed: %v",
		path, totalKeys, totalBuckets, currentRate,
		percentComplete, elapsed.Round(time.Second))

	m.lastLogTime = time.Now()
	m.lastVerifiedKeyCount = m.verifiedKeysSinceStart
}

// Migrate performs the migration of the source database to the target database.
func (m *Migrator) Migrate(ctx context.Context, sourceDB,
	targetDB kvdb.Backend) error {

	m.cfg.Logger.Infof("Migrating database with prefix `%s`",
		m.cfg.DBPrefixName)

	// Initialize or resume migration state.
	if err := m.initMigrationState(ctx); err != nil {
		if err == errMigrationComplete {
			m.cfg.Logger.Infof("Migration already completed")

			return nil
		}
		return err
	}

	if !m.migration.resuming {
		// Before proceeding with the migration we check that
		// the destination db is empty.
		var topLevelBuckets [][]byte
		err := kvdb.View(targetDB, func(tx kvdb.RTx) error {
			return tx.ForEachBucket(func(bucket []byte) error {
				bucketCopy := make([]byte, len(bucket))
				copy(bucketCopy, bucket)
				topLevelBuckets = append(
					topLevelBuckets, bucketCopy,
				)

				return nil
			})
		}, func() {})
		if err != nil {
			return err
		}

		if len(topLevelBuckets) > 0 {
			return fmt.Errorf("Target database for prefix `%s` "+
				"is not empty, refusing to start a new "+
				"migration - delete the target database "+
				"manually first", m.cfg.DBPrefixName)
		}
	}

	var moreChunks bool
	// Process chunks until complete.
	for {
		moreChunks = false
		err := sourceDB.View(func(sourceTx kvdb.RTx) error {
			return targetDB.Update(func(targetTx kvdb.RwTx) error {
				// Process the chunk.
				err := m.processChunk(ctx, sourceTx, targetTx)

				// We need to exclude this error because it is
				// expected and we want to continue processing
				// the next chunk.
				if err != nil && err == errChunkSizeExceeded {
					moreChunks = true
					return nil
				}
				return err
			}, func() {})
		}, func() {})

		if err != nil {
			return err
		}

		if moreChunks {
			// Save state and hash for the chunk. We already
			// updated the migration where we checked for the
			// chunk size limit.
			err = m.migration.write()
			if err != nil {
				return fmt.Errorf("failed to save "+
					"migration state: %w", err)
			}

			m.cfg.Logger.InfoS(
				ctx, "Committed chunk successfully:",
				"bucket", m.migration.persistedState.CurrentBucketPath,
				"processed_keys", m.migration.persistedState.ProcessedKeys,
				"processed_buckets", m.migration.persistedState.ProcessedBuckets,
				"current_chunk_size(B)", m.migration.currentChunkBytes,
			)

			m.cfg.Logger.Debug("Migration progress saved, " +
				"continuing with next chunk")

			m.migration.newChunk()

			continue
		}

		// Migration is complete, finalize everything in this
		// transaction.
		m.cfg.Logger.Infof("Migration complete, processed in "+
			"total %d keys",
			m.migration.persistedState.ProcessedKeys)

		// Update final state. This also makes sure the migration state
		// is marked as finished.
		m.migration.setFinalState()

		// Write the migration state.
		err = m.migration.write()
		if err != nil {
			return fmt.Errorf("failed to write migration "+
				"state: %w", err)
		}

		if m.migration.resuming {
			return fmt.Errorf("no keys were migrated, " +
				"the migration state path was not " +
				"found")
		}

		elapsed := time.Since(m.migration.persistedState.StartTime)

		m.cfg.Logger.Infof("Migration for db with prefix `%s` "+
			"completed in %v (since inital start)",
			m.cfg.DBPrefixName, elapsed)

		return nil
	}

}

// processChunk processes a single chunk of the migration by walking through
// the nested bucket structure and migrating the key-value pairs up to the
// specified chunk size.
func (m *Migrator) processChunk(ctx context.Context,
	sourceTx kvdb.RTx, targetTx kvdb.RwTx) error {

	// We start the iteration by looping through the root buckets.
	err := sourceTx.ForEachBucket(func(name []byte) error {
		sourceRootBucket := sourceTx.ReadBucket(name)
		if sourceRootBucket == nil {
			return fmt.Errorf("source root bucket not found for "+
				"key %x", name)
		}

		// We create the root bucket if it does not exist.
		targetRootBucket, err := targetTx.CreateTopLevelBucket(
			name,
		)
		if err != nil {
			return fmt.Errorf("failed to create target root "+
				"bucket: %w", err)
		}

		if !m.migration.resuming {
			m.cfg.Logger.Infof("Migrating root bucket: %v",
				loggableKeyName(name))

			// Increase the number of processed buckets.
			m.migration.persistedState.ProcessedBuckets++
		}

		// Start with the root bucket name as the initial path.
		initialPath := NewBucketPath([][]byte{name})
		err = m.migrateBucket(
			ctx, sourceRootBucket, targetRootBucket, initialPath,
		)

		return err
	})
	if err != nil {
		return err
	}

	return nil
}

// migrateBucket migrates a bucket from the source database to the target
// database.
func (m *Migrator) migrateBucket(ctx context.Context,
	sourceB kvdb.RBucket, targetB kvdb.RwBucket,
	path BucketPath) error {

	// Helper variables to shorten the names.
	currentPath := m.migration.persistedState.CurrentBucketPath
	lastUnprocessedKey := m.migration.persistedState.LastUnprocessedKey

	var alreadyMigrated bool
	var fastForwardBucket []byte

	// We skip the bucket if we already migrated it or fast-forward to the
	// next bucket if we can.
	switch m.migration.resuming {
	case true:
		alreadyMigrated, fastForwardBucket = checkBucketMigrated(
			currentPath, path,
		)

		if alreadyMigrated {
			m.cfg.Logger.Debugf("Skipping already migrated "+
				"bucket: %v", path)

			return nil
		}

	case false:
		// Only set sequence if source bucket has a non-zero sequence
		// otherwise we keep for the sql case the sequence is a NULL
		// value.

		sourceSeq := sourceB.Sequence()
		if sourceSeq != 0 {
			if err := targetB.SetSequence(sourceSeq); err != nil {
				return fmt.Errorf("error copying sequence "+
					"number %d: %v", sourceSeq, err)
			}

			m.cfg.Logger.Debugf("Successfully copied sequence "+
				"number for bucket %v: %d", path, sourceSeq)
		}
	}

	// We iterate over the source bucket.
	cursor := sourceB.ReadCursor()

	// We default to the first key in the bucket.
	k, v := cursor.First()

	// In case the fast-forward bucket is not nil we seek to it, but only
	// if we are resuming.
	if m.migration.resuming && fastForwardBucket != nil {
		m.cfg.Logger.Debugf("Resuming migration, fast-forwarding to "+
			"bucket: %s", loggableKeyName(fastForwardBucket))

		k, v = cursor.Seek(fastForwardBucket)
	}

	isResumptionPoint := currentPath.Equal(path)
	if isResumptionPoint {
		m.cfg.Logger.Debugf("Found migration resumption point "+
			"at bucket %v with last key: %s",
			path, loggableKeyName(lastUnprocessedKey))

		m.migration.resuming = false

		// In case we have a last unprocessed key we seek to it.
		if lastUnprocessedKey != nil {
			k, v = cursor.Seek(lastUnprocessedKey)
			if k == nil {
				return fmt.Errorf("failed to seek to last "+
					"unprocessed key: %x",
					lastUnprocessedKey)
			}
		}
	}

	// Now we position the cursor to the best known state now we iterate
	// over the bucket.
	for ; k != nil; k, v = cursor.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// We only log when not resuming to not spam with bucket paths
		// which are already migrated.
		if !m.migration.resuming {
			m.logMigrationProgress(path)
		}

		// We might slightly exceed the chunk size limit here but it
		// also allows us to use very small chunk sizes otherwise we
		// would need a minimum chunk size of the maximum key-value
		// size.
		if m.migration.currentChunkBytes >= m.cfg.ChunkSize {
			m.cfg.Logger.DebugS(ctx, "Chunk size exceeded, "+
				"committing current chunk:",
				"current_bucket", path,
				"last_key", loggableKeyName(lastUnprocessedKey),
				"chunk_size (Bytes)",
				m.migration.currentChunkBytes,
			)

			// We reached the chunk size limit, so we update the
			// current bucket path and return the error so the
			// caller can handle the transaction commit/rollback.
			m.migration.persistedState.CurrentBucketPath = path
			m.migration.persistedState.LastUnprocessedKey = k

			return errChunkSizeExceeded
		}

		// We to not count the size of the key-value pair if we are
		// resuming.
		if !m.migration.resuming {
			size := uint64(len(k) + len(v))

			m.migration.currentChunkBytes += size
		}

		// In case the value is nil this is a nested bucket so we go
		// into recursion here.
		if v == nil {
			sourceNestedBucket := sourceB.NestedReadBucket(k)
			if sourceNestedBucket == nil {
				return fmt.Errorf("source nested bucket not "+
					"found for key %x", k)
			}
			targetNestedBucket := targetB.NestedReadWriteBucket(k)

			// If the target bucket does not exist we create it.
			// Because we do the migration in chunks we might have
			// already created this bucket in a previous chunk.
			if targetNestedBucket == nil {
				// This is a safety check and should never
				// happen. If we are resuming it means we
				// already created this bucket in a previous
				// chunk.
				if m.migration.resuming {
					return fmt.Errorf("target nested "+
						"bucket not found for key %x",
						k)
				}

				var err error
				targetNestedBucket, err = targetB.CreateBucket(k)
				if err != nil {
					return fmt.Errorf("failed to create "+
						"bucket %x: %w", k, err)
				}

				m.cfg.Logger.Debugf("Created new nested "+
					"bucket at path %v: %s", path,
					loggableKeyName(k))
			}

			// We append the bucket to the path.
			newPath := path.AppendBucket(k)

			if !m.migration.resuming {
				// Increase the number of processed buckets.
				m.migration.persistedState.ProcessedBuckets++
			}

			// Recursively migrate the nested bucket.
			err := m.migrateBucket(
				ctx, sourceNestedBucket, targetNestedBucket,
				newPath,
			)
			if err != nil {
				return err
			}

			// We continue processing the other keys in the same
			// bucket.
			continue
		}

		// When resuming we skip the key-value pair.
		if m.migration.resuming {
			m.cfg.Logger.DebugS(ctx, "Resuming migration, "+
				"skipping key-value pair:",
				"bucket", path,
				"key", loggableKeyName(k),
				"value", loggableKeyName(v))

			continue
		}

		m.cfg.Logger.DebugS(ctx, "Migrating key-value pair:",
			"bucket", path,
			"key", loggableKeyName(k),
			"value", loggableKeyName(v))

		// We copy the key-value pair to the target bucket but we are
		// not committing the transaction here. This will accumulate.
		if err := targetB.Put(k, v); err != nil {
			return fmt.Errorf("failed to migrate key %x: %w", k,
				err)
		}

		// We process another key-value pair.
		m.migration.persistedState.ProcessedKeys++

		// Needed in case of a restart to have the correct rate.
		m.migratedKeysSinceStart++
	}

	m.cfg.Logger.Debugf("Migration of bucket %v completed", path)

	return nil
}

// VerifyMigration verifies the migration by comparing the source and target
// databases.
func (m *Migrator) VerifyMigration(ctx context.Context,
	sourceDB, targetDB kvdb.Backend, force bool) error {

	m.startTimeVerification = time.Now()

	// Initialize the verification state so we resume in the right way.
	err := m.initVerificationState(ctx, force)
	if err != nil {
		if err == errVerificationComplete {
			m.cfg.Logger.Infof("Verification already completed for "+
				"db with prefix `%s`", m.cfg.DBPrefixName)

			return nil
		}
		return err
	}

	err = m.compareDBs(ctx, sourceDB, targetDB)
	if err != nil {
		return err
	}

	// This should never happen we should always have a fnished migration
	// when executing the verification.
	if !m.migration.persistedState.Finished {
		return fmt.Errorf("migration not finished, " +
			"please run the migration again")
	}

	if m.migration.persistedState.ProcessedKeys !=
		m.verification.persistedState.ProcessedKeys {

		return fmt.Errorf("processed keys mismatch: %d != %d",
			m.migration.persistedState.ProcessedKeys,
			m.verification.persistedState.ProcessedKeys)
	}

	if m.migration.persistedState.ProcessedBuckets !=
		m.verification.persistedState.ProcessedBuckets {

		return fmt.Errorf("processed buckets mismatch: %d != %d",
			m.migration.persistedState.ProcessedBuckets,
			m.verification.persistedState.ProcessedBuckets)
	}

	if m.verification.resuming {

		return fmt.Errorf("no keys were verified, " +
			"the verification state path was not " +
			"found")
	}

	m.logFinalStats()

	return nil
}

// compareDBs compares the source and target databases value by value.
func (m *Migrator) compareDBs(ctx context.Context, srcDB,
	targetDB kvdb.Backend) error {

	// It is curcial that the source database is read here first because
	// for the sqlite case databases were combined so the targetDB might
	// have more buckets (top-level-buckets) than the sourceDB.
	return srcDB.View(func(srcTx kvdb.RTx) error {
		return targetDB.View(func(targetTx kvdb.RTx) error {
			// First compare root buckets.
			err := srcTx.ForEachBucket(func(name []byte) error {
				srcBucket := srcTx.ReadBucket(name)

				// In case we force a verification of a
				// tombstoned db we need to skip the tombstone
				// top-level bucket.
				if bytes.Equal(name, channeldb.TombstoneKey) {
					m.cfg.Logger.Info("Tombstone key " +
						"root bucket found, skipping")

					return nil
				}

				// Check if target has this root bucket
				targetBucket := targetTx.ReadBucket(name)
				if targetBucket == nil {
					return fmt.Errorf("root bucket missing "+
						"in target: %x", name)
				}

				// Walk and compare this bucket's hierarchy.
				newPath := NewBucketPath([][]byte{name})

				err := m.walkAndCompare(ctx, srcBucket,
					targetBucket, newPath)
				if err != nil {
					return err
				}

				// We checkpoint the state here in case we
				// shutdown/crash in the middle of the
				// verification.
				if !m.verification.resuming {
					m.verification.persistedState.
						CurrentBucketPath = newPath

					m.verification.persistedState.
						ProcessedBuckets++

					m.verification.persistedState.
						LastUnprocessedKey = nil

					// We persist the state here.
					err := m.verification.write()
					if err != nil {
						return err
					}
				}

				return nil
			})
			if err != nil {
				return err
			}

			m.verification.setFinalState()
			err = m.verification.write()
			if err != nil {
				return err
			}

			return nil
		}, func() {
		})
	}, func() {})
}

// walkAndCompare walks source and target buckets in parallel,
// comparing their contents.
func (m *Migrator) walkAndCompare(ctx context.Context, srcBucket,
	targetBucket kvdb.RBucket, bucketPath BucketPath) error {

	currentPath := m.verification.persistedState.CurrentBucketPath
	var nextBucket []byte

	switch m.verification.resuming {
	case false:
		m.cfg.Logger.Debugf("Walking and comparing bucket: %v",
			bucketPath)

		// Check the sequence number of the source and target buckets.
		if srcBucket.Sequence() != targetBucket.Sequence() {
			return fmt.Errorf("sequence number mismatch at "+
				"path %v: source=%d target=%d", bucketPath,
				srcBucket.Sequence(), targetBucket.Sequence())
		}

	case true:
		var alreadyMigrated bool
		alreadyMigrated, nextBucket = checkBucketMigrated(
			currentPath, bucketPath,
		)
		if alreadyMigrated {
			return nil
		}
	}

	srcCursor := srcBucket.ReadCursor()
	targetCursor := targetBucket.ReadCursor()

	sk, sv := srcCursor.First()
	tk, tv := targetCursor.First()

	if m.verification.resuming && nextBucket != nil {
		m.cfg.Logger.Debugf("Fast-forwarding verification to "+
			"bucket: %s", loggableKeyName(nextBucket))

		sk, sv = srcCursor.Seek(nextBucket)
		tk, tv = targetCursor.Seek(nextBucket)
	}

	// This resume is different from the migration resume because we are
	// only resuming in the verification at the beginning because read
	// transaction do not need to be closed regularly. We only read the
	// data here from both databases and compare. We still keep the resume
	// in case we shutdown/crash in the middle of the verification.
	if m.verification.resuming {
		currentPath := m.verification.persistedState.CurrentBucketPath
		isResumptionPoint := currentPath.Equal(bucketPath)
		lastKey := m.verification.persistedState.LastUnprocessedKey
		if isResumptionPoint {
			m.cfg.Logger.Infof("Found verification "+
				"resumption point "+
				"at bucket %v with last unprocessed key: %v",
				bucketPath, loggableKeyName(lastKey))

			m.verification.resuming = false

			if lastKey != nil {
				// Position the cursor to the last unprocessed
				// key.
				sk, sv = srcCursor.Seek(lastKey)
				tk, tv = targetCursor.Seek(lastKey)
			}
		}
	}

	for ; sk != nil; sk, sv = srcCursor.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !m.verification.resuming {
			m.logVerificationProgress(bucketPath)
		}

		// We only increment the chunk size if we are not resuming.
		if !m.verification.resuming {
			size := len(sk) + len(sv)
			m.verification.currentChunkBytes += uint64(size)
		}

		if m.verification.currentChunkBytes > m.cfg.ChunkSize {
			m.cfg.Logger.Debugf("Chunk size exceeded, "+
				"checkpointing at bucket %v with last "+
				"unprocessed key: %s", bucketPath,
				loggableKeyName(sk))

			m.verification.currentChunkBytes = 0
			m.verification.persistedState.CurrentBucketPath = bucketPath
			m.verification.persistedState.LastUnprocessedKey = sk

			err := m.verification.write()
			if err != nil {
				return err
			}

			// We do not chuck but we still checkpoint to resume
			// in case of of a shutdown/crash.
		}

		// We also check it in case we are resuming. Check if target
		// has fewer entries than source.
		if tk == nil {
			return fmt.Errorf("target bucket has fewer entries "+
				"at path %v, missing key %v", bucketPath,
				loggableKeyName(sk))
		}

		// Compare keys of both databases.
		if !bytes.Equal(sk, tk) {
			return fmt.Errorf("key mismatch at path %v: "+
				"source=%v target=%v", bucketPath,
				loggableKeyName(sk), loggableKeyName(tk))
		}

		// Handle nested buckets.
		if sv == nil {
			newPath := bucketPath.AppendBucket(sk)

			// Both must be buckets
			if tv != nil {
				return fmt.Errorf("type mismatch at path %v: "+
					"source is nested bucket, target is "+
					"single value", bucketPath)
			}

			srcNestedBucket := srcBucket.NestedReadBucket(sk)
			targetNestedBucket := targetBucket.NestedReadBucket(tk)
			if srcNestedBucket == nil {
				return fmt.Errorf("source nested bucket "+
					"access failed at path %v", bucketPath)
			}
			if targetNestedBucket == nil {
				return fmt.Errorf("target nested bucket "+
					"access failed at path %v", bucketPath)
			}

			// Recurse into nested buckets.
			err := m.walkAndCompare(
				ctx, srcNestedBucket, targetNestedBucket,
				newPath)
			if err != nil {
				return err
			}

			tk, tv = targetCursor.Next()

			// We only increase the counter if we are not resuming.
			if !m.verification.resuming {
				m.verification.persistedState.ProcessedBuckets++
			}

			continue
		}

		if m.verification.resuming {
			m.cfg.Logger.Debugf("Skipping key %x at path %v", sk,
				bucketPath)

			continue
		}

		// Handle key-value pairs.
		if !bytes.Equal(sv, tv) {
			return fmt.Errorf("value mismatch at path %v key %s, "+
				"source=%s target=%s", bucketPath,
				loggableKeyName(sk), loggableKeyName(sv),
				loggableKeyName(tv))
		}

		m.verification.persistedState.ProcessedKeys++
		m.verifiedKeysSinceStart++

		// Move the target cursor to the next entry.
		tk, tv = targetCursor.Next()
	}

	// Check if target has more entries than source
	if tk, _ := targetCursor.Next(); tk != nil {
		return fmt.Errorf("target bucket has extra entries "+
			"at path %v, extra key %x", bucketPath, tk)
	}

	return nil
}

// initVerificationState reads and initializes the verification state.
func (m *Migrator) initVerificationState(ctx context.Context, force bool) error {
	// Read the existing states.
	err := m.migration.read()
	if err != nil {
		return fmt.Errorf("failed to read migration "+
			"state: %w", err)
	}
	err = m.verification.read()
	if err != nil && !errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound) {

		return fmt.Errorf("failed to read migration "+
			"state: %w", err)
	}

	// We need to handle different cases when starting the verification
	// process.
	switch {
	// In case we already finished the verification and we are not forcing
	case m.verification.persistedState.Finished && !force:
		m.cfg.Logger.Infof("Verification already finished at time: %v",
			m.verification.persistedState.FinishedTime.Format(
				time.RFC3339),
		)

		return errVerificationComplete

	// In case we already finished the verification and we are forcing
	// a new one we need to start fresh.
	case m.verification.persistedState.Finished && force:
		m.verification.persistedState = newPersistedState()

		m.cfg.Logger.Infof("Forcing new verification, deleting " +
			"verification state and starting fresh")

		return nil

	// In case we do not have a current path set we need to start fresh,
	// we do not have a previous verification state which means we haven't
	// commited a state yet.
	case !m.verification.persistedState.CurrentBucketPath.HasPath():
		m.verification.persistedState = newPersistedState()

		m.cfg.Logger.Infof("No previous verification state found, " +
			"starting fresh")

		return nil

	// Otherwise we have a previous verification state so we will resume.
	default:
		m.verification.resuming = true
		m.cfg.Logger.InfoS(ctx, "Resuming verification",
			"bucket", m.verification.persistedState.CurrentBucketPath,
			"processed_keys", m.verification.persistedState.ProcessedKeys,
		)

		return nil
	}
}

// logFinalStats logs the complete statistics of the migration and
// verification process.
//
// TODO(ziggie): Make sure the right times are logged in case of a
// restart.
func (m *Migrator) logFinalStats() {
	migrationTime := time.Since(m.migration.persistedState.StartTime) -
		time.Since(m.verification.persistedState.StartTime)

	verificationTime := time.Since(m.verification.persistedState.StartTime)
	totalTime := time.Since(m.migration.persistedState.StartTime)

	migrationRate := float64(m.migration.persistedState.ProcessedKeys) /
		migrationTime.Seconds()

	verificationRate := float64(m.verification.persistedState.ProcessedKeys) /
		verificationTime.Seconds()

	m.cfg.Logger.Infof("*** FINAL MIGRATION/VERIFICATION STATISTICS for "+
		"DB with prefix `%s` ***", m.cfg.DBPrefixName)
	m.cfg.Logger.Infof("Total time: %v", totalTime.Round(time.Second))
	m.cfg.Logger.Infof("  ├── Migration time: %v (%.2f keys/sec)",
		migrationTime.Round(time.Second), migrationRate)
	m.cfg.Logger.Infof("  └── Verification time: %v (%.2f keys/sec)",
		verificationTime.Round(time.Second), verificationRate)
	m.cfg.Logger.Infof("Processed items:")
	m.cfg.Logger.Infof("  ├── Keys: %d",
		m.migration.persistedState.ProcessedKeys)
	m.cfg.Logger.Infof("  └── Buckets: %d",
		m.migration.persistedState.ProcessedBuckets)
}
