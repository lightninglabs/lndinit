package migratekvdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/btcsuite/btcwallet/walletdb"
)

// Database bucket and key constants.
const (
	// migrationMetaBucket is the top-level bucket that stores the migration
	// metadata.
	migrationMetaBucket = "migration_meta"

	// migrationStateKey is the key of the current state of the migration.
	migrationStateKey = "migration_state"

	// verificationStateKey is the key of the verification state of the
	// migration.
	verificationStateKey = "verification_state"

	// chunkHashKey is the nested bucket in the migration_meta bucket that
	// stores the chunk hashes of the source database which are later used
	// for verification of the migrated data.
	chunkHashKey = "source_chunk_hashes"

	// migrationCompleteKey is the key of the migration complete state.
	migrationCompleteKey = "migration_complete"

	// verificationCompleteKey is the key of the verification complete
	// state.
	verificationCompleteKey = "verification_complete"
)

// Size constants.
const (
	// DefaultChunkSize is 20MB (leaving room for overhead).
	DefaultChunkSize = 20 * 1024 * 1024

	// MaxChunkSize is 500MB to prevent excessive memory usage.
	MaxChunkSize = 500 * 1024 * 1024
)

// StateType indicates whether we're handling migration or verification state.
type stateType uint8

const (
	stateMigration stateType = iota
	stateVerification
)

// Config holds the configuration for the migrator.
type Config struct {
	// ChunkSize is the number of items (key-value pairs or buckets) to
	// process in a single transaction in bytes.
	ChunkSize uint64

	// Logger is the logger to use for logging.
	Logger btclog.Logger

	// ForceNewMigration is a flag to force a new migration even if a
	// migration state is found. This will delete all existing migration
	// data in the target database.
	ForceNewMigration bool
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

// ChunkHash represents the verification hash of a chunk of the source database.
type ChunkHash struct {
	// Hash is the accumulated hash of the data which is included in one
	// chunk.
	Hash uint64 `json:"hash"`

	// LastKey is the last key processed in the chunk so we can resume
	// the migration from the same point.
	LastKey []byte `json:"last_key"`

	// Path is the last processed bucket path in this chunk.
	Path BucketPath `json:"path"`
}

// Equal compares this ChunkHash with another ChunkHash for equality.
func (c *ChunkHash) Equal(other *ChunkHash) bool {
	// Check for nil
	if c == nil || other == nil {
		return c == other
	}

	if c.Hash != other.Hash {
		return false
	}

	if !bytes.Equal(c.LastKey, other.LastKey) {
		return false
	}

	return c.Path.Equal(other.Path)
}

// String returns a string representation of the ChunkHash.
func (c *ChunkHash) String() string {
	if c == nil {
		return "ChunkHash<nil>"
	}
	return fmt.Sprintf("ChunkHash{hash=%d, lastKey=%x, path=%v}",
		c.Hash, c.LastKey, c.Path)
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
}

// New creates a new Migrator with the given configuration.
func New(cfg Config) (*Migrator, error) {
	// Validate and set defaults for the config.
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Migrator{
		cfg: cfg,
		migration: &MigrationState{
			hash:     fnv.New64a(),
			resuming: false,
		},
		verification: &VerificationState{
			hash:     fnv.New64a(),
			resuming: false,
		},
	}, nil
}

// markMigrationComplete marks the migration as complete in the migration_meta
// bucket.
func (m *Migrator) markMigrationComplete(tx walletdb.ReadWriteTx) error {
	metaBucket, err := tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	timestamp := time.Now().UTC().Format(time.RFC3339)
	err = metaBucket.Put(
		[]byte(migrationCompleteKey),
		[]byte(fmt.Sprintf("completed at %s", timestamp)),
	)
	if err != nil {
		return fmt.Errorf("failed to mark migration complete: %w", err)
	}

	m.cfg.Logger.Infof("Migration marked as complete at %s", timestamp)
	return nil
}

// markVerificationComplete marks the verification as complete in the
// migration_meta bucket.
func (m *Migrator) markVerificationComplete(tx walletdb.ReadWriteTx) error {
	metaBucket, err := tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	timestamp := time.Now().UTC().Format(time.RFC3339)
	err = metaBucket.Put(
		[]byte(verificationCompleteKey),
		[]byte(fmt.Sprintf("completed at %s", timestamp)),
	)
	if err != nil {
		return fmt.Errorf("failed to mark verification complete: %w", err)
	}

	m.cfg.Logger.Infof("Verification marked as complete at %s", timestamp)
	return nil
}

// isMigrationComplete checks if the migration has been marked as complete.
func (m *Migrator) isMigrationComplete(tx walletdb.ReadTx) (bool, error) {
	bucket := tx.ReadBucket([]byte(migrationMetaBucket))
	if bucket == nil {
		return false, nil
	}

	marker := bucket.Get([]byte(migrationCompleteKey))
	return marker != nil, nil
}

// isVerificationComplete checks if the verification has been marked as
// complete.
func (m *Migrator) isVerificationComplete(tx walletdb.ReadTx) (bool, error) {
	bucket := tx.ReadBucket([]byte(migrationMetaBucket))
	if bucket == nil {
		return false, nil
	}

	marker := bucket.Get([]byte(verificationCompleteKey))
	return marker != nil, nil
}

// recordChunkHash records the chunk hash in the target database. When
// migrating the source database we hash each piece of data (bucket,
// key-value data) which is then later used to verify our target database.
// We record this data in the migration meta bucket.
func (m *Migrator) recordChunkHash(ctx context.Context,
	targetTx walletdb.ReadWriteTx) error {

	metaBucket, err := targetTx.CreateTopLevelBucket(
		[]byte(migrationMetaBucket),
	)
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	// Read or create the nested bucket for the chunk hashes.
	chunkBucket := metaBucket.NestedReadWriteBucket([]byte(chunkHashKey))
	if chunkBucket == nil {
		chunkBucket, err = metaBucket.CreateBucketIfNotExists(
			[]byte(chunkHashKey),
		)
		if err != nil {
			return fmt.Errorf("failed to create chunk "+
				"bucket: %v", err)
		}
	}

	// We do not only save the hash of the chunk but also make sure it has
	// the same path and key so we can then more easily debug mismatches.
	record := ChunkHash{
		Hash:    m.migration.hash.Sum64(),
		LastKey: m.migration.persistedState.LastProcessedKey,
		Path:    m.migration.persistedState.CurrentBucketPath,
	}

	// We json encode the record so we can save it in the target DB.
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Create a unique key for the chunk hash.
	key := m.migration.persistedState.CurrentBucketPath.CreateChunkKey(
		m.migration.persistedState.LastProcessedKey,
	)

	m.cfg.Logger.DebugS(ctx, "Recording chunk hash",
		"bucket", m.migration.persistedState.CurrentBucketPath,
		"last_key", hex.EncodeToString(m.migration.persistedState.LastProcessedKey),
		"hash", m.migration.hash.Sum64(),
		"chunk hash key", hex.EncodeToString(key),
	)

	err = chunkBucket.Put(key, encoded)

	return err
}

// getChunkHash tries to get the chunk hash from a specific bucket path. This
// is part of the verification process. We try to lookup the chunk record we
// created during migration and basically use a key lookup to find the
// corresponding chunk record from the migration. So if the lookup matches it
// is very likely that also the data is correct because the same the key
// consistes of the bucket path and the last processed key.
func (m *Migrator) getChunkHash(targetTx walletdb.ReadTx) (ChunkHash, error) {
	metaBucket := targetTx.ReadBucket([]byte(migrationMetaBucket))
	if metaBucket == nil {
		return ChunkHash{}, errNoMetaBucket
	}

	chunkBucket := metaBucket.NestedReadBucket([]byte(chunkHashKey))
	if chunkBucket == nil {
		return ChunkHash{}, fmt.Errorf("failed to lookup chunk " +
			"verification hash: chunk bucket not found")
	}

	lookupKey := m.verification.persistedState.CurrentBucketPath.CreateChunkKey(
		m.verification.persistedState.LastProcessedKey,
	)

	m.cfg.Logger.Debugf("Lookup key: %x", lookupKey)

	chunkHashBytes := chunkBucket.Get(lookupKey)
	if chunkHashBytes == nil {
		return ChunkHash{}, errNoChunkHash
	}

	var chunkHash ChunkHash
	if err := json.Unmarshal(chunkHashBytes, &chunkHash); err != nil {
		return ChunkHash{}, fmt.Errorf("failed to unmarshal chunk "+
			"hash: %w", err)
	}

	return chunkHash, nil
}

// isBucketMigrated checks if a bucket would have already been processed
// in our depth-first traversal based on the current path we're processing.
//
// NOTE: This works because we can be sure that the nested bucket paths are
// lexicographically ordered and are always walked through in the same order.
func (m *Migrator) isBucketMigrated(currentPath, checkPath BucketPath) bool {
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
			return comp < 0
		}
	}

	// This point can only ever be reached if the checked path is equal but
	// the current path is longer. Which means the checked path is still
	// not migrated because we are doing a depth-first traversal.
	return false
}

// logChunkProgress logs the progress of a chunk.
func (m *Migrator) logChunkProgress(ctx context.Context, path string,
	lastKey []byte, currentChunkBytes uint64, hash uint64) {

	m.cfg.Logger.InfoS(ctx, "Chunk size exceeded, committing:",
		"current_bucket", path,
		"last_key", loggableKeyName(lastKey),
		"chunk_size_limit (Bytes)", m.cfg.ChunkSize,
		"chunk_size_actual (Bytes)", currentChunkBytes,
		"hash", hash,
	)
}

// initMigrationState reads and initializes the migration state.
func (m *Migrator) initMigrationState(ctx context.Context, targetDB walletdb.DB) error {
	return walletdb.Update(targetDB, func(tx walletdb.ReadWriteTx) error {
		// Try to read existing state.
		err := m.migration.read(tx)
		switch {
		case err != nil && !errors.Is(err, errNoMetaBucket) &&
			!errors.Is(err, errNoStateFound):

			return fmt.Errorf("failed to read migration "+
				"state: %w", err)

		case m.migration.persistedState == nil:
			// State does not exist, start fresh.
			m.cfg.Logger.Warn("Migration state not found, " +
				"starting fresh")
			m.migration.persistedState = newPersistedState(
				m.cfg.ChunkSize,
			)
			m.migration.resuming = false

			return nil

		case m.cfg.ForceNewMigration:
			// Force new migration.
			m.cfg.Logger.Info("Starting fresh migration")
			m.migration.persistedState = newPersistedState(
				m.cfg.ChunkSize,
			)
			m.migration.resuming = false

			// Drop the meta bucket and start fresh.
			err = tx.DeleteTopLevelBucket(
				[]byte(migrationMetaBucket),
			)
			if err != nil {
				return fmt.Errorf("failed to delete meta "+
					"bucket: %w", err)
			}
			return nil

		default:
			return m.validateExistingMigrationState(ctx, tx)
		}
	})
}

// validateExistingMigrationState validates an existing migration state.
func (m *Migrator) validateExistingMigrationState(ctx context.Context,
	tx walletdb.ReadWriteTx) error {

	// Check if migration is already completed.
	complete, err := m.isMigrationComplete(tx)
	if err != nil {
		return fmt.Errorf("failed to check if migration is "+
			"complete: %w", err)
	}
	if complete {
		m.cfg.Logger.Info("Migration already complete, set the force " +
			"new migration flag to start fresh")

		return errMigrationComplete
	}

	if !m.migration.persistedState.CurrentBucketPath.HasPath() {
		return fmt.Errorf("migration state already exists " +
			"but no bucket path found")
	}

	// Check chunk size consistency.
	if m.migration.persistedState.ChunkSize != m.cfg.ChunkSize {
		m.cfg.Logger.Error("Chunk size mismatch, previous migration "+
			"chunk size was %d, but current chunk size is %d",
			m.migration.persistedState.ChunkSize, m.cfg.ChunkSize)

		return errChunkSizeMismatch
	}

	// Otherwise we are ready to resume the migration.
	m.migration.resuming = true
	m.cfg.Logger.InfoS(ctx, "Resuming migration",
		"bucket", m.migration.persistedState.CurrentBucketPath,
		"processed_keys", m.migration.persistedState.ProcessedKeys,
	)

	return nil
}

// Migrate performs the migration of the source database to the target database.
func (m *Migrator) Migrate(ctx context.Context, sourceDB,
	targetDB walletdb.DB) error {

	m.cfg.Logger.Infof("Migrating database ...")

	// Initialize or resume migration state.
	if err := m.initMigrationState(ctx, targetDB); err != nil {
		if err == errMigrationComplete {
			return nil
		}
		return err
	}

	// Process chunks until complete.
	for {
		// Each chunk is processed in its own transaction pair.
		needsMoreChunks, err := m.processNextChunk(
			ctx, sourceDB, targetDB,
		)
		if err != nil {
			return err
		}

		// Migration is complete.
		if !needsMoreChunks {
			return nil
		}

		// We start a new chunk.
		m.migration.newChunk()
	}
}

// processNextChunk processes the next chunk of the migration.
func (m *Migrator) processNextChunk(ctx context.Context, sourceDB,
	targetDB walletdb.DB) (bool, error) {

	var needsMoreChunks bool

	err := walletdb.View(sourceDB, func(sourceTx walletdb.ReadTx) error {
		return walletdb.Update(targetDB, func(targetTx walletdb.ReadWriteTx) error {
			// Process the chunk.
			err := m.processChunk(ctx, sourceTx, targetTx)
			if err != nil {
				if err != errChunkSizeExceeded {
					return err
				}
			}

			if err == errChunkSizeExceeded {
				// Save state and hash for the chunk.
				err = m.migration.write(targetTx)
				if err != nil {
					return fmt.Errorf("failed to save "+
						"migration state: %w", err)
				}

				err = m.recordChunkHash(ctx, targetTx)
				if err != nil {
					return fmt.Errorf("failed to record "+
						"chunk hash: %w", err)
				}

				m.cfg.Logger.InfoS(
					ctx, "Committed chunk successfully:",
					"bucket", m.migration.persistedState.CurrentBucketPath,
					"processed_keys", m.migration.persistedState.ProcessedKeys,
					"current_chunk_size(B)", m.migration.currentChunkBytes,
					"source_hash", m.migration.hash.Sum64(),
				)

				m.cfg.Logger.Infof("Migration progress saved, " +
					"continuing with next chunk")

				needsMoreChunks = true

				return nil
			}

			// Migration is complete, finalize everything in this
			// transaction.
			m.cfg.Logger.Infof("Migration complete, processed in "+
				"total %d keys",
				m.migration.persistedState.ProcessedKeys)

			// Mark migration as complete.
			err = m.markMigrationComplete(targetTx)
			if err != nil {
				return err
			}

			// Update final state. This also makes sure the last
			// chunk has a unique key.
			m.migration.markComplete()

			// // Record the final chunk hash.
			err = m.recordChunkHash(ctx, targetTx)
			if err != nil {
				return fmt.Errorf("failed to record "+
					"the finalchunk hash: %w", err)
			}

			// Write the migration state.
			err = m.migration.write(targetTx)
			if err != nil {
				return fmt.Errorf("failed to write migration "+
					"state: %w", err)
			}

			if m.migration.resuming {
				return fmt.Errorf("no keys were migrated, " +
					"the migration state path was not " +
					"found")
			}

			return nil
		})
	})

	return needsMoreChunks, err
}

// processChunk processes a single chunk of the migration by walking through
// the nested bucket structure and migrating the key-value pairs up to the
// specified chunk size.
func (m *Migrator) processChunk(ctx context.Context,
	sourceTx walletdb.ReadTx, targetTx walletdb.ReadWriteTx) error {

	// We start the iteration by looping through the root buckets.
	err := sourceTx.ForEachBucket(func(rootBucket []byte) error {
		sourceRootBucket := sourceTx.ReadBucket(rootBucket)

		// We create the root bucket if it does not exist.
		targetRootBucket, err := targetTx.CreateTopLevelBucket(
			rootBucket,
		)
		if err != nil {
			return fmt.Errorf("failed to create target root "+
				"bucket: %w", err)
		}

		if !m.migration.resuming {
			m.cfg.Logger.Infof("Migrating root bucket: %v",
				loggableKeyName(rootBucket))

			// We also add the key of the root bucket to the source
			// hash.
			m.migration.hash.Write(rootBucket)
		}

		// Start with the root bucket name as the initial path.
		initialPath := NewBucketPath([][]byte{rootBucket})
		err = m.migrateBucket(
			ctx, sourceRootBucket, targetRootBucket, initialPath,
		)
		if err == errChunkSizeExceeded {
			// We return the error so the caller can handle the
			// transaction commit/rollback.
			return errChunkSizeExceeded
		}
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// migrateBucket migrates a bucket from the source database to the target
// database.
func (m *Migrator) migrateBucket(ctx context.Context,
	sourceB walletdb.ReadBucket, targetB walletdb.ReadWriteBucket,
	path BucketPath) error {

	// Skip already migrated buckets in case we are resuming from a failed
	// migration or resuming from a new chunk.
	if m.isBucketMigrated(m.migration.persistedState.CurrentBucketPath, path) {
		m.cfg.Logger.Debugf("Skipping already migrated bucket: %v", path)

		return nil
	}

	// Copying the sequence number over as well.
	if !m.migration.resuming {
		if err := targetB.SetSequence(sourceB.Sequence()); err != nil {
			return fmt.Errorf("error copying sequence number")
		}

		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, sourceB.Sequence())
		m.migration.hash.Write(seqBytes)
	}

	// We now iterate over the source bucket.
	cursor := sourceB.ReadCursor()

	// We also navigate to the last processed key if we are resuming in
	// this bucket or start from the beginning.
	k, v, err := m.positionCursor(cursor, path, stateMigration)
	if err != nil {
		return err
	}

	// We process through the bucket.
	for ; k != nil; k, v = cursor.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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
				var err error
				targetNestedBucket, err = targetB.CreateBucket(k)
				if err != nil {
					return fmt.Errorf("failed to create "+
						"bucket %x: %w", k, err)
				}

				m.cfg.Logger.DebugS(ctx, "created nested bucket",
					"key", loggableKeyName(k))
			}

			// We append the bucket to the path.
			newPath := path.AppendBucket(k)

			// We also include the nested bucket in the source hash
			// for later verification.
			if !m.migration.resuming {
				m.cfg.Logger.Debugf("Migrating a new nested "+
					"bucket: %s", newPath)

				// We add the hash of the nested bucket to the
				// source hash.
				m.migration.hash.Write(k)
			}

			// Recursively migrate the nested bucket.
			if err := m.migrateBucket(
				ctx, sourceNestedBucket, targetNestedBucket,
				newPath,
			); err != nil {
				if err == errChunkSizeExceeded {
					// Propagate chunk size exceeded up the
					// stack.
					return err
				}
				return fmt.Errorf("failed to migrate bucket "+
					"%s: %w", path, err)
			}

			// We continue processing the other keys in the same
			// bucket.
			continue
		}

		// In case we are resuming from a new chunk or we recover from
		// a failed migration we skip the key processing. This will be
		// set to false once we either found the bucket or started with
		// a new migration.
		if m.migration.resuming {
			m.cfg.Logger.TraceS(ctx, "skipping key-value pair:",
				"bucket", path,
				"key", loggableKeyName(k),
				"value", loggableKeyName(v))

			continue
		}

		m.cfg.Logger.TraceS(ctx, "migrating key-value pair:",
			"bucket", path,
			"key", loggableKeyName(k),
			"value", loggableKeyName(v))

		// We copy the key-value pair to the target bucket but we are
		// not committing the transaction here. This will accumulate.
		if err := targetB.Put(k, v); err != nil {
			return fmt.Errorf("failed to migrate key %x: %w", k,
				err)
		}

		// We record the stats of the migration and also log them when
		// writing the chunk to disk.
		m.migration.persistedState.ProcessedKeys++

		// We write the key value pairs to the source hash.
		_, err = m.migration.hash.Write(k)
		if err != nil {
			return fmt.Errorf("failed to write key to "+
				"hash: %w", err)
		}
		_, err = m.migration.hash.Write(v)
		if err != nil {
			return fmt.Errorf("failed to write value to "+
				"hash: %w", err)
		}

		m.cfg.Logger.DebugS(ctx, "new preliminary hash (migration)",
			"hash", m.migration.hash.Sum64())

		// If chunk size is reached, commit and signal pause.
		entrySize := uint64(len(k) + len(v))
		m.migration.currentChunkBytes += entrySize

		if m.migration.currentChunkBytes >= m.cfg.ChunkSize {
			m.logChunkProgress(
				ctx, path.StringPath,
				k, m.migration.currentChunkBytes,
				m.migration.hash.Sum64(),
			)

			// We reached the chunk size limit, so we update the
			// current bucket path and return the error so the
			// caller can handle the transaction commit/rollback.
			m.migration.persistedState.CurrentBucketPath = path
			m.migration.persistedState.LastProcessedKey = k
			m.migration.persistedState.LastChunkHash = m.migration.hash.Sum64()

			return errChunkSizeExceeded
		}
	}

	m.cfg.Logger.Debugf("Migration of bucket %s processing completed", path)

	return nil
}

// checkResumptionPoint checks if we're at the resumption point for either
// migration or verification state.
func (m *Migrator) checkResumptionPoint(path BucketPath,
	stateType stateType) bool {

	var currentPath BucketPath
	var lastKey []byte

	switch stateType {
	case stateMigration:
		currentPath = m.migration.persistedState.CurrentBucketPath
		lastKey = m.migration.persistedState.LastProcessedKey

		isResumptionPoint := currentPath.Equal(path)
		if isResumptionPoint {
			m.cfg.Logger.Infof("Found migration resumption point "+
				"at bucket %v with last key: %s",
				path, loggableKeyName(lastKey))

			m.migration.resuming = false
			return true
		}

	case stateVerification:
		currentPath = m.verification.persistedState.CurrentBucketPath
		lastKey = m.verification.persistedState.LastProcessedKey
		isResumptionPoint := currentPath.Equal(path)
		if isResumptionPoint {
			m.cfg.Logger.Infof("Found verification resumption "+
				"point at bucket %v with last key: %s",
				path, loggableKeyName(lastKey))

			m.verification.resuming = false
			return true
		}

	default:
		m.cfg.Logger.Errorf("Unknown state type: %d", stateType)
	}

	return false
}

// positionCursor positions the cursor either at the resumption point or at
// the start of the bucket.
func (m *Migrator) positionCursor(cursor walletdb.ReadCursor,
	path BucketPath, stateType stateType) (k, v []byte, err error) {

	isResumptionPoint := m.checkResumptionPoint(path, stateType)

	var lastProcessedKey []byte
	switch stateType {
	case stateMigration:
		lastProcessedKey = m.migration.persistedState.LastProcessedKey

	case stateVerification:
		lastProcessedKey = m.verification.persistedState.LastProcessedKey
	}
	// If we're at resumption point and have a last key, seek to it.
	if isResumptionPoint &&
		lastProcessedKey != nil {

		k, _ = cursor.Seek(lastProcessedKey)
		if k == nil {
			return nil, nil, fmt.Errorf("failed to find last "+
				"processed key %x in bucket %s - database "+
				"may be corrupted", lastProcessedKey, path)
		}
		k, v = cursor.Next()
	} else {
		k, v = cursor.First()
	}

	return k, v, nil
}

// VerifyMigration verifies that the source and target databases match exactly.
// During the migration verification checksums are calculated while migrating
// through the source database. In this verification process we loop through the
// target database and create the checksums independently to compare them with
// the source checksums.
func (m *Migrator) VerifyMigration(ctx context.Context,
	targetDB walletdb.DB) error {

	m.cfg.Logger.Info("Verifying migration...")

	// Initialize verification state.
	err := walletdb.View(targetDB, func(tx walletdb.ReadTx) error {
		if err := m.migration.read(tx); err != nil {
			return fmt.Errorf("failed to read migration "+
				"state: %w", err)
		}

		return m.initVerificationState(tx)
	})
	if err != nil {
		if errors.Is(err, errVerificationComplete) {
			return nil
		}
		return err
	}

	for {
		m.cfg.Logger.Infof("Processing chunk for verification...")

		moreChunks, err := m.processVerificationChunk(ctx, targetDB)
		if err != nil {
			return err
		}
		if !moreChunks {
			return nil
		}

		// We start a new chunk for the next verification step.
		m.verification.newChunk()
	}
}

// initVerificationState initializes or resumes the verification state.
func (m *Migrator) initVerificationState(tx walletdb.ReadTx) error {
	// First check if migration is complete.
	complete, err := m.isMigrationComplete(tx)
	if err != nil {
		return fmt.Errorf("failed to check if migration is "+
			"complete: %w", err)
	}
	if !complete {
		return errMigrationIncomplete
	}

	// Read verification state.
	err = m.verification.read(tx)
	switch {
	case err != nil && !errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound):
		return fmt.Errorf("failed to read verification state: %w", err)

	case m.verification.persistedState == nil:
		// No state found, start fresh.
		m.cfg.Logger.Info("No verification state found, starting fresh")
		m.verification.persistedState = newPersistedState(m.cfg.ChunkSize)
		m.verification.resuming = false
		return nil

	case !m.verification.persistedState.CurrentBucketPath.HasPath():
		return fmt.Errorf("verification state already exists " +
			"but no bucket path found")

	default:
		// Check if verification is already complete.
		complete, err = m.isVerificationComplete(tx)
		if err != nil {
			return fmt.Errorf("failed to check if verification is "+
				"complete: %w", err)
		}
		if complete {
			m.cfg.Logger.Info("Verification already complete")
			return errVerificationComplete
		}

		// Resume the verification.
		verificationState := m.verification.persistedState
		m.verification.resuming = true
		m.cfg.Logger.InfoS(context.Background(),
			"Resuming verification",
			"bucket", verificationState.CurrentBucketPath,
			"processed_keys", verificationState.ProcessedKeys,
		)

		return nil
	}
}

// verifyChunkHash verifies that the current chunk's hash matches the migration
// hash. When migrating we save the chunk of the source database and its
// information in the destination database via a specific key which is composed
// of the last path of the chunk and its final processed key. When verifying
// we must use the same chunk size which ensures we are comparing the same data.
func (m *Migrator) verifyChunkHash(tx walletdb.ReadTx) error {
	m.cfg.Logger.Debugf("Looking up chunk hash for %v, last processed "+
		"key: %x",
		m.verification.persistedState.CurrentBucketPath,
		m.verification.persistedState.LastProcessedKey)

	migrationChunkHash, err := m.getChunkHash(tx)
	if err != nil {
		return fmt.Errorf("failed to get chunk hash: %w", err)
	}

	verificationChunkHash := &ChunkHash{
		Hash:    m.verification.persistedState.LastChunkHash,
		LastKey: m.verification.persistedState.LastProcessedKey,
		Path:    m.verification.persistedState.CurrentBucketPath,
	}

	if !migrationChunkHash.Equal(verificationChunkHash) {
		return fmt.Errorf("chunk hash mismatch, expected %v, got %v",
			migrationChunkHash, verificationChunkHash)
	}

	m.cfg.Logger.Infof("Chunk hash match, verification complete at "+
		"path: %s", m.verification.persistedState.CurrentBucketPath)

	return nil
}

// processVerificationChunk processes a single chunk and returns whether there
// are more chunks to process.
func (m *Migrator) processVerificationChunk(ctx context.Context,
	targetDB walletdb.DB) (bool, error) {

	var moreChunks bool
	err := walletdb.Update(targetDB, func(tx walletdb.ReadWriteTx) error {
		// We compute the hash for the current chunk.
		err := m.computeHashForChunk(ctx, tx)
		if err != nil {
			if err != errChunkSizeExceeded {
				return err
			}
		}

		// We now verify the chunk hash we computed.
		if err == errChunkSizeExceeded {
			if err := m.verifyChunkHash(tx); err != nil {
				return err
			}

			// Persist the verification state.
			if err := m.verification.write(tx); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			m.cfg.Logger.Debugf("Verification state persisted: %v",
				m.verification.persistedState)

			m.cfg.Logger.Infof("Verification of chunk %s "+
				"complete",
				m.verification.persistedState.CurrentBucketPath)

			moreChunks = true

			return nil
		}

		// Mark verification as complete.
		err = m.markVerificationComplete(tx)
		if err != nil {
			return fmt.Errorf("failed to mark verification "+
				"complete: %w", err)
		}

		// We finished the verification process successfully and
		// we log the final state here.
		m.verification.markComplete()
		if err := m.verification.write(tx); err != nil {
			return fmt.Errorf("failed to save migration "+
				"state: %w", err)
		}

		// We reached the final chunk.
		if err := m.verifyChunkHash(tx); err != nil {
			return err
		}

		if m.verification.resuming {
			return fmt.Errorf("verification failed, " +
				"the verification state path was not " +
				"found")
		}

		// Verify the number of processed keys.
		if m.verification.persistedState.ProcessedKeys !=
			m.migration.persistedState.ProcessedKeys {

			return fmt.Errorf("verification failed, total "+
				"processed keys mismatch, expected %d, got %d",
				m.migration.persistedState.ProcessedKeys,
				m.verification.persistedState.ProcessedKeys)
		}

		m.cfg.Logger.Infof("Verification complete, processed "+
			"in total %d keys (same as migration)",
			m.verification.persistedState.ProcessedKeys)

		return nil
	})

	return moreChunks, err
}

// computeHashForChunk computes the verification hash for the current chunk.
func (m *Migrator) computeHashForChunk(ctx context.Context,
	targetTx walletdb.ReadWriteTx) error {

	// We start the iteration by looping through the root buckets.
	err := targetTx.ForEachBucket(func(rootBucket []byte) error {
		targetRootBucket := targetTx.ReadWriteBucket(rootBucket)
		if targetRootBucket == nil {
			return fmt.Errorf("failed to get target root bucket: %w",
				errNoBucket)
		}

		// We skip the migration meta bucket because it is not part of
		// the verification process.
		if bytes.Equal(rootBucket, []byte(migrationMetaBucket)) {
			return nil
		}

		if !m.verification.resuming {
			m.cfg.Logger.Infof("Verifying root bucket: %s",
				loggableKeyName(rootBucket))

			// We also add the key of the root bucket for the
			// verification process.
			m.verification.hash.Write(rootBucket)
		}

		// Start with the root bucket name as the initial path.
		initialPath := NewBucketPath([][]byte{rootBucket})
		err := m.hashBucketContent(
			ctx, targetRootBucket, initialPath,
		)
		if err == errChunkSizeExceeded {
			// We return the error so the caller can handle the
			// transaction commit/rollback.
			return errChunkSizeExceeded
		}
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

// hashBucketContent loops through the bucket and recreates the hash for the
// bucket content.
func (m *Migrator) hashBucketContent(ctx context.Context,
	targetB walletdb.ReadWriteBucket, path BucketPath) error {

	// Skip already verified buckets in case we are resuming from a failed
	// verification or resuming when creating a new verification chunk.
	if m.isBucketMigrated(m.verification.persistedState.CurrentBucketPath, path) {
		m.cfg.Logger.Debugf("Skipping already verified bucket: %v", path)

		return nil
	}

	// The sequence number is also part of the verification process.
	if !m.verification.resuming {
		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, targetB.Sequence())
		m.verification.hash.Write(seqBytes)
	}

	// We now iterate over the source bucket.
	cursor := targetB.ReadCursor()

	// We also navigate to the last processed key if we are resuming in
	// this bucket or start from the beginning.
	k, v, err := m.positionCursor(cursor, path, stateVerification)
	if err != nil {
		return err
	}

	// We process through the bucket.
	for ; k != nil; k, v = cursor.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		isNestedBucket := v == nil

		// This is needed because sqlite does treat an empty byte array
		// as nil and there might be keys which have an empty byte array
		// as value.
		var targetNestedBucket walletdb.ReadWriteBucket
		if isNestedBucket {
			targetNestedBucket = targetB.NestedReadWriteBucket(k)
			if targetNestedBucket == nil {
				m.cfg.Logger.Debugf("target nested bucket not "+
					"found for key, treating as key-value "+
					"pair %x", k)

				isNestedBucket = false
			}
		}

		// In case the value is nil this is a nested bucket so we go
		// into recursion here.
		switch {
		case isNestedBucket:
			// We use the hex encoded key as the path to make sure
			// we don't have any special characters in the path.
			newPath := path.AppendBucket(k)

			if !m.verification.resuming {
				m.cfg.Logger.Debugf("Verifying a new nested "+
					"bucket: %v", newPath)

				// We add the hash of the nested bucket to the
				// verification hash as well.
				m.verification.hash.Write(k)
			}

			// Recursively migrate the nested bucket.
			if err := m.hashBucketContent(
				ctx, targetNestedBucket, newPath,
			); err != nil {
				if err == errChunkSizeExceeded {
					// Propagate chunk size exceeded up the
					// stack.
					return err
				}
				return fmt.Errorf("failed to verify bucket "+
					"%s: %w", path, err)
			}

		default:
			// In case we are resuming from a new chunk or we
			// recover from a failed migration we skip the key
			// processing. This will be set to false once we
			// either found the bucket or started with a new
			// migration.
			if m.verification.resuming {
				continue
			}

			// We record the stats of the migration and also log
			// them when writing the chunk to disk.
			m.verification.persistedState.ProcessedKeys++

			// We write the key value pairs to the source hash.
			m.verification.hash.Write(k)
			m.verification.hash.Write(v)

			m.cfg.Logger.TraceS(ctx, "verifying key-value pair:",
				"bucket", path,
				"key", loggableKeyName(k),
				"value", loggableKeyName(v))

			m.cfg.Logger.DebugS(ctx, "new preliminary hash "+
				"(verification)", "hash",
				m.verification.hash.Sum64())

			// If chunk size is reached, commit and signal pause.
			entrySize := uint64(len(k) + len(v))
			m.verification.currentChunkBytes += entrySize

			if m.verification.currentChunkBytes >= m.cfg.ChunkSize {
				m.logChunkProgress(
					ctx, path.StringPath, k,
					m.verification.currentChunkBytes,
					m.verification.hash.Sum64(),
				)

				// We reached the chunk size limit, so we
				// update the current bucket path and return
				// the error so the caller can handle the
				// transaction commit/rollback.
				m.verification.persistedState.CurrentBucketPath = path
				m.verification.persistedState.LastProcessedKey = k
				m.verification.persistedState.LastChunkHash = m.verification.hash.Sum64()

				return errChunkSizeExceeded
			}
		}

	}

	m.cfg.Logger.Debugf("Verification of bucket %s processing "+
		"completed", path)

	return nil
}
