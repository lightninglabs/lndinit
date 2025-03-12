package migratekvdb

import (
	"encoding/json"
	"fmt"
	"hash"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
)

// persistedState tracks the migration progress for resumability of the process.
type persistedState struct {
	// Currently processing bucket
	CurrentBucketPath BucketPath `json:"current_bucket_path"`

	// Last key processed in current bucket
	LastProcessedKey []byte `json:"last_processed_key"`

	// Number of keys processed
	ProcessedKeys int64 `json:"processed_keys"`

	// Timestamp of the migration
	Timestamp time.Time `json:"timestamp"`

	// Hash of the last chunk
	LastChunkHash uint64 `json:"last_chunk_hash"`

	// We need to make sure that the chunk size remains the same during
	// the complete migration, because we are also creating verification
	// hahses for each chunk we later might compare therefore the size
	// needs to be constant.
	ChunkSize uint64 `json:"chunk_size"`
}

// String returns a string representation of the persisted state.
func (s *persistedState) String() string {
	return fmt.Sprintf("Path: %s, LastKey: %x, ProcessedKeys: %d, "+
		"LastHash: %d, ChunkSize: %d, Time: %s",
		s.CurrentBucketPath,
		s.LastProcessedKey,
		s.ProcessedKeys,
		s.LastChunkHash,
		s.ChunkSize,
		s.Timestamp.Format(time.RFC3339),
	)
}

// newPersistedState creates a new persisted state with the required chunk size.
// The chunk size needs to be persisted because the verification depends on the
// same chunk size.
func newPersistedState(chunkSize uint64) *persistedState {
	return &persistedState{
		ChunkSize:         chunkSize,
		CurrentBucketPath: NewBucketPath([][]byte{}),
		LastProcessedKey:  nil,
		ProcessedKeys:     0,
		LastChunkHash:     0,
		Timestamp:         time.Now(),
	}
}

// MigrationState holds migration-specific state for resumability of the
// process.
type MigrationState struct {
	persistedState    *persistedState
	currentChunkBytes uint64
	hash              hash.Hash64
	resuming          bool
}

// read reads the migration state from the database.
func (m *MigrationState) read(tx walletdb.ReadTx) error {
	metaBucket := tx.ReadBucket([]byte(migrationMetaBucket))
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
}

func (m *MigrationState) write(tx walletdb.ReadWriteTx) error {
	metaBucket, err := tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	encoded, err := json.Marshal(m.persistedState)
	if err != nil {
		return err
	}

	return metaBucket.Put([]byte(migrationStateKey), encoded)
}

// markComplete marks the migration as complete.
func (m *MigrationState) markComplete() {
	m.persistedState.CurrentBucketPath = NewBucketPath([][]byte{
		[]byte("complete"),
	})
	m.persistedState.LastProcessedKey = nil
	m.persistedState.Timestamp = time.Now()
	m.persistedState.LastChunkHash = m.hash.Sum64()
	m.currentChunkBytes = 0
}

// newChunk creates a new chunk for the migration by resetting the
// non-persistent state.
func (m *MigrationState) newChunk() {
	m.currentChunkBytes = 0
	m.resuming = true
	m.hash.Reset()
}

// VerificationState holds verification-specific state for resumability of the
// process.
type VerificationState struct {
	persistedState    *persistedState
	currentChunkBytes uint64
	hash              hash.Hash64
	resuming          bool
}

// read reads the verification state from the database.
func (v *VerificationState) read(tx walletdb.ReadTx) error {
	metaBucket := tx.ReadBucket([]byte(migrationMetaBucket))
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
}

// write writes the verification state to the database.
func (v *VerificationState) write(tx walletdb.ReadWriteTx) error {
	metaBucket, err := tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	encoded, err := json.Marshal(v.persistedState)
	if err != nil {
		return err
	}

	return metaBucket.Put([]byte(verificationStateKey), encoded)
}

// markComplete marks the verification as complete.
func (v *VerificationState) markComplete() {
	v.persistedState.CurrentBucketPath = NewBucketPath([][]byte{
		[]byte("complete"),
	})
	v.persistedState.LastProcessedKey = nil
	v.persistedState.Timestamp = time.Now()
	v.persistedState.LastChunkHash = v.hash.Sum64()
	v.currentChunkBytes = 0
}

// newChunk creates a new chunk for the verification.
func (v *VerificationState) newChunk() {
	v.currentChunkBytes = 0
	v.resuming = true
	v.hash.Reset()
}
