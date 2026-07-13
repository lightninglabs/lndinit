//go:build kvdb_postgres

package migratekvdb

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

var (
	errBulkBegin      = errors.New("begin bulk failed")
	errTargetNotEmpty = errors.New("target is not empty")
)

// fakeBulkTarget implements the lnd SQL bulk migration target interfaces with
// configurable responses for recovery and verification tests.
type fakeBulkTarget struct {
	truncateCalled   bool
	checkEmptyCalled bool
	empty            bool
	checkEmptyErr    error
	verifier         sqlbase.MigrationBulkKVVerifier
}

var _ sqlbase.MigrationBackend = (*fakeBulkTarget)(nil)

// CheckEmpty returns the configured empty-table response.
func (f *fakeBulkTarget) CheckEmpty(context.Context) (bool, error) {
	f.checkEmptyCalled = true

	return f.empty, f.checkEmptyErr
}

// TruncateTargetTable records that migration recovery truncated the target.
func (f *fakeBulkTarget) TruncateTargetTable(context.Context) error {
	f.truncateCalled = true

	return nil
}

// BeginBulk returns the configured fake bulk write failure.
func (*fakeBulkTarget) BeginBulk(
	context.Context) (sqlbase.MigrationBulkKVTx, error) {

	return nil, errBulkBegin
}

// BeginBulkVerify returns the configured fake bulk verifier.
func (f *fakeBulkTarget) BeginBulkVerify(
	context.Context) (sqlbase.MigrationBulkKVVerifier, error) {

	if f.verifier == nil {
		return nil, errors.New("unused")
	}

	return f.verifier, nil
}

// BeginReadTx is unused by the bulk migration tests.
func (*fakeBulkTarget) BeginReadTx() (walletdb.ReadTx, error) {
	return nil, errors.New("unused")
}

// BeginReadWriteTx is unused by the bulk migration tests.
func (*fakeBulkTarget) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	return nil, errors.New("unused")
}

// Copy is unused by the bulk migration tests.
func (*fakeBulkTarget) Copy(io.Writer) error {
	return errors.New("unused")
}

// Close closes the fake target.
func (*fakeBulkTarget) Close() error {
	return nil
}

// PrintStats is unused by the bulk migration tests.
func (*fakeBulkTarget) PrintStats() string {
	return ""
}

// View is unused by the bulk migration tests.
func (*fakeBulkTarget) View(func(walletdb.ReadTx) error, func()) error {
	return errors.New("unused")
}

// Update is unused by the bulk migration tests.
func (*fakeBulkTarget) Update(func(walletdb.ReadWriteTx) error, func()) error {
	return errors.New("unused")
}

// TestMigrateBulkRecoversInterruptedAttempt verifies that an interrupted bulk
// migration restarts without truncating when its target is empty.
func TestMigrateBulkRecoversInterruptedAttempt(t *testing.T) {
	metaDB := openTestMetaDB(t)
	migration := newMigrationState(metaDB)
	migration.persistedState = newPersistedState()
	migration.persistedState.BulkInProgress = true
	require.NoError(t, migration.write())

	migrator, err := New(Config{
		ChunkSize:    DefaultChunkSize,
		MetaDB:       metaDB,
		DBPrefixName: "test",
	})
	require.NoError(t, err)

	target := &fakeBulkTarget{empty: true}
	err = migrator.MigrateBulk(context.Background(), nil, target, false)
	require.ErrorIs(t, err, errBulkBegin)
	require.False(t, target.truncateCalled)
	require.True(t, target.checkEmptyCalled)
}

// TestMigrateBulkRecoveryRequiresReset verifies that recovery refuses to
// truncate a non-empty target without explicit operator authorization.
func TestMigrateBulkRecoveryRequiresReset(t *testing.T) {
	metaDB := openTestMetaDB(t)
	migration := newMigrationState(metaDB)
	migration.persistedState = newPersistedState()
	migration.persistedState.BulkInProgress = true
	require.NoError(t, migration.write())

	migrator, err := New(Config{
		ChunkSize:    DefaultChunkSize,
		MetaDB:       metaDB,
		DBPrefixName: "test",
	})
	require.NoError(t, err)

	target := &fakeBulkTarget{empty: false}
	err = migrator.MigrateBulk(context.Background(), nil, target, false)
	require.EqualError(t, err, "interrupted bulk migration target is not "+
		"empty; rerun with --reset-bulk-target to destroy its rows and "+
		"restart")
	require.False(t, target.truncateCalled)

	err = migrator.MigrateBulk(context.Background(), nil, target, true)
	require.ErrorIs(t, err, errBulkBegin)
	require.True(t, target.truncateCalled)
}

// TestMigrateBulkChecksFreshTarget verifies that a fresh bulk migration checks
// target emptiness and propagates errors without truncating the target.
func TestMigrateBulkChecksFreshTarget(t *testing.T) {
	metaDB := openTestMetaDB(t)

	migrator, err := New(Config{
		ChunkSize:    DefaultChunkSize,
		MetaDB:       metaDB,
		DBPrefixName: "test",
	})
	require.NoError(t, err)

	target := &fakeBulkTarget{
		empty:         false,
		checkEmptyErr: errTargetNotEmpty,
	}
	err = migrator.MigrateBulk(context.Background(), nil, target, false)
	require.ErrorIs(t, err, errTargetNotEmpty)
	require.True(t, target.checkEmptyCalled)
	require.False(t, target.truncateCalled)
}

// TestVerifyMigrationBulkDetectsTargetCorruption verifies that lndinit rejects
// corrupted target rows returned through the lnd bulk verifier interface.
func TestVerifyMigrationBulkDetectsTargetCorruption(t *testing.T) {
	const (
		rootID   = int64(1)
		nestedID = int64(2)
	)

	rootParent := rootID
	nestedParent := nestedID
	tests := []struct {
		name        string
		topLevel    []sqlbase.MigrationBulkChild
		children    map[int64][]sqlbase.MigrationBulkChild
		expectedErr string
	}{
		{
			name: "valid target",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
					bulkBucket(nestedID, &rootParent, "nested", 7),
				},
				nestedID: {
					bulkLeaf(0, &nestedParent, "child", []byte("value")),
				},
			},
		},
		{
			name: "leaf value changed",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("bob"),
					),
					bulkBucket(nestedID, &rootParent, "nested", 7),
				},
				nestedID: {
					bulkLeaf(0, &nestedParent, "child", []byte("value")),
				},
			},
			expectedErr: "value mismatch",
		},
		{
			name: "leaf changed to bucket",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkBucket(3, &rootParent, "name", 0),
					bulkBucket(nestedID, &rootParent, "nested", 7),
				},
				nestedID: {
					bulkLeaf(0, &nestedParent, "child", []byte("value")),
				},
			},
			expectedErr: "source is a value, target is a bucket",
		},
		{
			name: "bucket changed to leaf",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
					bulkLeaf(
						0, &rootParent, "nested",
						[]byte("wrong"),
					),
				},
			},
			expectedErr: "source is a nested bucket, target is a value",
		},
		{
			name: "extra child",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
					bulkBucket(nestedID, &rootParent, "nested", 7),
					bulkLeaf(
						0, &rootParent, "zzz",
						[]byte("extra"),
					),
				},
				nestedID: {
					bulkLeaf(0, &nestedParent, "child", []byte("value")),
				},
			},
			expectedErr: "extra entries",
		},
		{
			name: "missing child",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
				},
			},
			expectedErr: "missing key",
		},
		{
			name: "sequence changed",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
					bulkBucket(nestedID, &rootParent, "nested", 99),
				},
				nestedID: {
					bulkLeaf(0, &nestedParent, "child", []byte("value")),
				},
			},
			expectedErr: "sequence mismatch",
		},
		{
			name:        "root bucket missing",
			topLevel:    nil,
			children:    map[int64][]sqlbase.MigrationBulkChild{},
			expectedErr: "root bucket missing in target",
		},
		{
			name: "root bucket changed to leaf",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkLeaf(0, nil, "root", []byte("wrong")),
			},
			children:    map[int64][]sqlbase.MigrationBulkChild{},
			expectedErr: "source is a bucket, target is a value",
		},
		{
			name: "extra root bucket",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
				bulkBucket(3, nil, "extra", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
					bulkBucket(nestedID, &rootParent, "nested", 7),
				},
				nestedID: {
					bulkLeaf(0, &nestedParent, "child", []byte("value")),
				},
			},
			expectedErr: "unexpected top-level row",
		},
		{
			name: "extra target tombstone bucket",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
				bulkBucket(
					3, nil, string(channeldb.TombstoneKey), 0,
				),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &rootParent, "empty", []byte{}),
					bulkLeaf(
						0, &rootParent, "name",
						[]byte("alice"),
					),
					bulkBucket(
						nestedID, &rootParent, "nested", 7,
					),
				},
				nestedID: {
					bulkLeaf(
						0, &nestedParent, "child",
						[]byte("value"),
					),
				},
			},
			expectedErr: "unexpected top-level row",
		},
		{
			name: "child has no parent id",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, nil, "empty", []byte{}),
				},
			},
			expectedErr: "has no parent id",
		},
		{
			name: "child has unexpected parent id",
			topLevel: []sqlbase.MigrationBulkChild{
				bulkBucket(rootID, nil, "root", 0),
			},
			children: map[int64][]sqlbase.MigrationBulkChild{
				rootID: {
					bulkLeaf(0, &nestedParent, "empty", []byte{}),
				},
			},
			expectedErr: "unexpected parent id",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			sourceDB := newBulkVerifySourceDB(t)
			migrator := newFinishedBulkMigrator(t)
			target := &fakeBulkTarget{
				verifier: &fakeBulkVerifier{
					topLevel: test.topLevel,
					children: test.children,
				},
			}

			err := migrator.VerifyMigrationBulk(
				context.Background(), sourceDB, target,
			)
			if test.expectedErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, test.expectedErr)
		})
	}
}

// fakeBulkVerifier returns in-memory target rows through the lnd SQL bulk
// verification interface.
type fakeBulkVerifier struct {
	topLevel []sqlbase.MigrationBulkChild
	children map[int64][]sqlbase.MigrationBulkChild
}

// FetchTopLevel returns the configured fake top-level target rows.
func (f *fakeBulkVerifier) FetchTopLevel(
	context.Context) ([]sqlbase.MigrationBulkChild, error) {

	return f.topLevel, nil
}

// FetchChildren returns the configured fake child rows for parentIDs.
func (f *fakeBulkVerifier) FetchChildren(_ context.Context,
	parentIDs []int64) ([]sqlbase.MigrationBulkChild, error) {

	var children []sqlbase.MigrationBulkChild
	for _, parentID := range parentIDs {
		children = append(children, f.children[parentID]...)
	}

	return children, nil
}

// Rollback closes the fake verifier.
func (*fakeBulkVerifier) Rollback() error {
	return nil
}

// bulkBucket creates a fake target bucket row.
func bulkBucket(id int64, parentID *int64, key string,
	sequence uint64) sqlbase.MigrationBulkChild {

	return sqlbase.MigrationBulkChild{
		ID:       id,
		ParentID: parentID,
		Key:      []byte(key),
		IsBucket: true,
		Sequence: sequence,
	}
}

// bulkLeaf creates a fake target leaf row.
func bulkLeaf(id int64, parentID *int64, key string,
	value []byte) sqlbase.MigrationBulkChild {

	return sqlbase.MigrationBulkChild{
		ID:       id,
		ParentID: parentID,
		Key:      []byte(key),
		Value:    value,
	}
}

// newFinishedBulkMigrator creates a migrator whose meta DB records a completed
// migration matching newBulkVerifySourceDB.
func newFinishedBulkMigrator(t *testing.T) *Migrator {
	t.Helper()

	metaDB := openTestMetaDB(t)
	migration := newMigrationState(metaDB)
	migration.persistedState = newPersistedState()
	migration.persistedState.ProcessedKeys = 3
	migration.persistedState.ProcessedBuckets = 2
	migration.setFinalState()
	require.NoError(t, migration.write())

	migrator, err := New(Config{
		ChunkSize:    DefaultChunkSize,
		MetaDB:       metaDB,
		DBPrefixName: "test",
	})
	require.NoError(t, err)

	return migrator
}

// newBulkVerifySourceDB creates the tiny Bolt source used by the fake bulk
// verification tests.
func newBulkVerifySourceDB(t *testing.T) kvdb.Backend {
	t.Helper()

	args := []interface{}{
		t.TempDir() + "/source.db", true, time.Minute, false,
	}
	db, err := kvdb.Create(kvdb.BoltBackendName, args...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	err = db.Update(func(tx walletdb.ReadWriteTx) error {
		root, err := tx.CreateTopLevelBucket([]byte("root"))
		if err != nil {
			return err
		}
		if err := root.Put([]byte("empty"), []byte{}); err != nil {
			return err
		}
		if err := root.Put([]byte("name"), []byte("alice")); err != nil {
			return err
		}

		nested, err := root.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := nested.SetSequence(7); err != nil {
			return err
		}

		return nested.Put([]byte("child"), []byte("value"))
	}, func() {})
	require.NoError(t, err)

	return db
}

// openTestMetaDB opens a temporary migration metadata database for a test.
func openTestMetaDB(t *testing.T) *bbolt.DB {
	t.Helper()

	metaDB, err := bbolt.Open(t.TempDir()+"/meta.db", 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metaDB.Close())
	})

	return metaDB
}
