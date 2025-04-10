package migratekvdb

import (
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// persistedState tracks the migration progress for resumability of the process.
type persistedState struct {
	// Currently processing bucket
	CurrentBucketPath BucketPath `json:"current_bucket_path"`

	// Last key processed in current bucket
	LastUnprocessedKey []byte `json:"last_unprocessed_key"`

	// Total processed keys
	ProcessedKeys int64 `json:"processed_keys"`

	// Number of buckets processed
	ProcessedBuckets int64 `json:"processed_buckets"`

	// Timestamp of the migration
	StartTime time.Time `json:"start_time"`
}

// String returns a string representation of the persisted state.
func (s *persistedState) String() string {
	return fmt.Sprintf("Path: %s, LastKey: %x, ProcessedKeys: %d, "+
		"ProcessedBuckets: %d, Time: %s",
		s.CurrentBucketPath,
		s.LastUnprocessedKey,
		s.ProcessedKeys,
		s.ProcessedBuckets,
		s.StartTime.Format(time.RFC3339),
	)
}

// newPersistedState creates a new persisted state with the required chunk size.
// The chunk size needs to be persisted because the verification depends on the
// same chunk size.
func newPersistedState() *persistedState {
	return &persistedState{
		CurrentBucketPath:  NewBucketPath([][]byte{}),
		LastUnprocessedKey: nil,
		StartTime:          time.Now(),
	}
}

// MigrationState holds migration-specific state for resumability of the
// process.
type MigrationState struct {
	persistedState    *persistedState
	currentChunkBytes uint64
	resuming          bool
}

// read reads the migration state from the database.
func (m *MigrationState) read(metaDB *bbolt.DB) error {
	return metaDB.View(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket([]byte(migrationMetaBucket))
		if metaBucket == nil {
			return errNoMetaBucket
		}

		stateBytes := metaBucket.Get([]byte(migrationStateKey))
		if stateBytes == nil {
			return errNoStateFound
		}

		var state persistedState
		if err := json.Unmarshal(stateBytes, &state); err != nil {
			return fmt.Errorf("failed to unmarshal state: %w", err)
		}

		m.persistedState = &state

		return nil
	})
}

func (m *MigrationState) write(metaDB *bbolt.DB) error {
	return metaDB.Update(func(tx *bbolt.Tx) error {
		metaBucket, err := tx.CreateBucketIfNotExists(
			[]byte(migrationMetaBucket),
		)
		if err != nil {
			return fmt.Errorf("failed to create meta "+
				"bucket: %w", err)
		}

		encoded, err := json.Marshal(m.persistedState)
		if err != nil {
			return err
		}

		return metaBucket.Put([]byte(migrationStateKey), encoded)
	})
}

// setFinalState sets the final state of the migration.
func (m *MigrationState) setFinalState() {
	m.persistedState.CurrentBucketPath = NewBucketPath([][]byte{
		[]byte("complete"),
	})
	m.persistedState.LastUnprocessedKey = nil
	m.currentChunkBytes = 0
}

// newChunk creates a new chunk for the migration by resetting the
// non-persistent state.
func (m *MigrationState) newChunk() {
	m.currentChunkBytes = 0
	m.resuming = true
}

// VerificationState holds verification-specific state for resumability of the
// process.
type VerificationState struct {
	persistedState    *persistedState
	currentChunkBytes uint64
	resuming          bool
}

// read reads the verification state from the database.
func (v *VerificationState) read(metaDB *bbolt.DB) error {
	return metaDB.View(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket([]byte(migrationMetaBucket))
		if metaBucket == nil {
			return errNoMetaBucket
		}

		stateBytes := metaBucket.Get([]byte(verificationStateKey))
		if stateBytes == nil {
			return errNoStateFound
		}

		var state persistedState
		if err := json.Unmarshal(stateBytes, &state); err != nil {
			return fmt.Errorf("failed to unmarshal state: %w", err)
		}

		v.persistedState = &state
		return nil
	})
}

// write writes the verification state to the database.
func (v *VerificationState) write(metaDB *bbolt.DB) error {
	return metaDB.Update(func(tx *bbolt.Tx) error {
		metaBucket, err := tx.CreateBucketIfNotExists(
			[]byte(migrationMetaBucket),
		)
		if err != nil {
			return fmt.Errorf("failed to get meta bucket: %w", err)
		}

		encoded, err := json.Marshal(v.persistedState)
		if err != nil {
			return err
		}

		return metaBucket.Put([]byte(verificationStateKey), encoded)
	})
}

// setFinalState sets the final state of the verification.
func (v *VerificationState) setFinalState() {
	v.persistedState.CurrentBucketPath = NewBucketPath([][]byte{
		[]byte("complete"),
	})
	v.persistedState.LastUnprocessedKey = nil
	v.currentChunkBytes = 0
}
