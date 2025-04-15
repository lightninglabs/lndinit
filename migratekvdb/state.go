package migratekvdb

import (
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// persistedState tracks the migration or verification progress for
// resumability of the process.
type persistedState struct {
	// Currently processing bucket
	CurrentBucketPath BucketPath `json:"current_bucket_path"`

	// Last key processed in current bucket
	LastUnprocessedKey []byte `json:"last_unprocessed_key"`

	// Total processed keys
	ProcessedKeys int64 `json:"processed_keys"`

	// Number of buckets processed
	ProcessedBuckets int64 `json:"processed_buckets"`

	// Timestamp of the migration only set when the migration is started
	// from the beginning. In case of a resume the start time will still
	// be the initial time when the migration was started.
	StartTime time.Time `json:"start_time"`

	// Finished is set to true in case the verification is finished.
	Finished bool `json:"finished"`

	// FinishedTime is the time when the verification is finished.
	FinishedTime time.Time `json:"finished_time"`
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
func newPersistedState() persistedState {
	return persistedState{
		CurrentBucketPath:  NewBucketPath([][]byte{}),
		LastUnprocessedKey: nil,
		StartTime:          time.Now(),
	}
}

// MigrationState holds migration-specific state for resumability of the
// process.
type MigrationState struct {
	persistedState
	currentChunkBytes uint64
	resuming          bool

	db *bbolt.DB
}

// newMigrationState creates a new migration state.
func newMigrationState(db *bbolt.DB) *MigrationState {
	return &MigrationState{
		db: db,
	}
}

// read reads the migration state from the database.
func (m *MigrationState) read() error {
	return m.db.View(func(tx *bbolt.Tx) error {
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

		m.persistedState = state

		return nil
	})
}

func (m *MigrationState) write() error {
	return m.db.Update(func(tx *bbolt.Tx) error {
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
	m.persistedState.LastUnprocessedKey = []byte("complete")
	m.currentChunkBytes = 0
	m.persistedState.Finished = true
	m.persistedState.FinishedTime = time.Now()
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
	persistedState    persistedState
	currentChunkBytes uint64
	resuming          bool

	db *bbolt.DB
}

// newVerificationState creates a new verification state.
func newVerificationState(db *bbolt.DB) *VerificationState {
	return &VerificationState{
		db: db,
	}
}

// read reads the verification state from the database.
func (v *VerificationState) read() error {
	return v.db.View(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket([]byte(verificationMetaBucket))
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

		v.persistedState = state
		return nil
	})
}

// write writes the verification state to the database.
func (v *VerificationState) write() error {
	return v.db.Update(func(tx *bbolt.Tx) error {
		metaBucket, err := tx.CreateBucketIfNotExists(
			[]byte(verificationMetaBucket),
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
	v.persistedState.LastUnprocessedKey = []byte("complete")
	v.currentChunkBytes = 0
	v.persistedState.Finished = true
	v.persistedState.FinishedTime = time.Now()
}
