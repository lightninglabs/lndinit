//go:build kvdb_postgres

package migratekvdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
)

const (
	// DefaultBulkLeafBatchSize is the number of leaf rows accumulated before
	// flushing them through the backend bulk insert API.
	DefaultBulkLeafBatchSize = 10_000

	// DefaultBulkVerifyBatchSize is the number of bucket ids whose direct
	// children are fetched in one verification query.
	DefaultBulkVerifyBatchSize = 2_000
)

// bulkWriter buffers leaf rows while inserting bucket rows immediately through
// the lnd-owned migration bulk API.
type bulkWriter struct {
	ctx       context.Context
	tx        sqlbase.MigrationBulkKVTx
	batchSize int
	rows      []sqlbase.MigrationBulkLeaf
}

// insertBucket inserts a bucket row and returns its generated SQL id.
func (b *bulkWriter) insertBucket(parentID *int64, key []byte,
	seq uint64) (int64, error) {

	return b.tx.InsertBucket(b.ctx, parentID, key, seq)
}

// addLeaf buffers a leaf row and flushes the buffer once it reaches the batch
// size.
func (b *bulkWriter) addLeaf(parentID int64, key, value []byte) error {
	keyCopy := cloneBytes(key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.rows = append(b.rows, sqlbase.MigrationBulkLeaf{
		ParentID: parentID,
		Key:      keyCopy,
		Value:    valueCopy,
	})
	if len(b.rows) < b.batchSize {
		return nil
	}

	return b.flush()
}

// flush inserts all buffered leaf rows.
func (b *bulkWriter) flush() error {
	if len(b.rows) == 0 {
		return nil
	}

	err := b.tx.InsertLeaves(b.ctx, b.rows)
	b.rows = b.rows[:0]

	return err
}

// MigrateBulk performs a fresh bolt-to-SQL KV migration using the target
// backend's migration-only bulk API.
//
// The bulk path intentionally does not resume mid-bucket. It writes a
// BulkInProgress marker before inserting rows. If a previous run left that
// marker behind, the target is reset through the lnd-owned API and the
// migration starts over from the source.
func (m *Migrator) MigrateBulk(ctx context.Context, sourceDB kvdb.Backend,
	target sqlbase.MigrationBackend, resetTarget bool) error {

	m.cfg.Logger.Infof("Bulk-migrating database with prefix `%s`",
		m.cfg.DBPrefixName)

	if err := m.migration.read(); err != nil &&
		!errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound) {

		return fmt.Errorf("failed to read migration state: %w", err)
	}

	if m.migration.persistedState.Finished {
		m.cfg.Logger.Infof("Migration already finished")

		return nil
	}

	recovering := m.migration.persistedState.BulkInProgress
	m.migration.persistedState = newPersistedState()

	if recovering {
		empty, err := target.CheckEmpty(ctx)
		if err != nil {
			return fmt.Errorf("failed to check interrupted bulk "+
				"migration target emptiness: %w", err)
		}

		if !empty {
			if !resetTarget {
				return fmt.Errorf("interrupted bulk migration target " +
					"is not empty; rerun with " +
					"--reset-bulk-target to destroy its rows " +
					"and restart")
			}

			m.cfg.Logger.Warnf("Detected a non-empty target from an "+
				"interrupted bulk migration for prefix `%s`; "+
				"truncating it and starting over", m.cfg.DBPrefixName)

			if err := target.TruncateTargetTable(ctx); err != nil {
				return fmt.Errorf("failed to truncate bulk migration "+
					"target: %w", err)
			}
		} else {
			m.cfg.Logger.Infof("Interrupted bulk migration for prefix "+
				"`%s` left an empty target; starting over",
				m.cfg.DBPrefixName)
		}
	} else {
		empty, err := target.CheckEmpty(ctx)
		if err != nil {
			return fmt.Errorf("failed to check target emptiness: %w",
				err)
		}
		if !empty {
			return fmt.Errorf("target is not empty")
		}
	}

	m.migration.persistedState.BulkInProgress = true
	if err := m.migration.write(); err != nil {
		return fmt.Errorf("failed to write bulk in-progress marker: %w",
			err)
	}

	tx, err := target.BeginBulk(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin bulk migration: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	writer := &bulkWriter{
		ctx:       ctx,
		tx:        tx,
		batchSize: DefaultBulkLeafBatchSize,
	}

	err = sourceDB.View(func(sourceTx kvdb.RTx) error {
		return sourceTx.ForEachBucket(func(name []byte) error {
			// Skip the tombstone root bucket so migrate and verify
			// agree on the processed-bucket count. The tombstone is
			// only written after a successful migration, but a
			// re-migration of an already-tombstoned source into a
			// fresh target would otherwise count it here while
			// bulkVerifyTopLevel skips it.
			if bytes.Equal(name, channeldb.TombstoneKey) {
				m.cfg.Logger.Info("Tombstone key root bucket " +
					"found, skipping")

				return nil
			}

			sourceRoot := sourceTx.ReadBucket(name)
			if sourceRoot == nil {
				return fmt.Errorf("source root bucket not found "+
					"for key %x", name)
			}

			m.migration.persistedState.ProcessedBuckets++

			return m.bulkBucket(
				writer, sourceRoot, name, nil,
				NewBucketPath([][]byte{name}),
			)
		})
	}, func() {})
	if err != nil {
		return err
	}

	if err := writer.flush(); err != nil {
		return fmt.Errorf("failed to flush final leaf batch: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit bulk migration: %w", err)
	}

	m.cfg.Logger.Infof("Bulk migration complete, processed %d keys, "+
		"%d buckets", m.migration.persistedState.ProcessedKeys,
		m.migration.persistedState.ProcessedBuckets)

	m.migration.setFinalState()
	m.migration.persistedState.BulkInProgress = false

	return m.migration.write()
}

// bulkBucket inserts the bucket row for source, then walks its direct children.
// Leaf rows are buffered; nested buckets are inserted immediately so their SQL
// row ids can be used as parent ids for descendants.
func (m *Migrator) bulkBucket(writer *bulkWriter, source kvdb.RBucket,
	key []byte, parentID *int64, path BucketPath) error {

	m.logMigrationProgress(path)

	id, err := writer.insertBucket(parentID, key, source.Sequence())
	if err != nil {
		return fmt.Errorf("failed to insert bucket at path %v: %w",
			path, err)
	}

	cursor := source.ReadCursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		select {
		case <-writer.ctx.Done():
			return writer.ctx.Err()
		default:
		}

		if v == nil {
			nested := source.NestedReadBucket(k)
			if nested == nil {
				return fmt.Errorf("source nested bucket not "+
					"found at path %v key %s", path,
					loggableKeyName(k))
			}

			m.migration.persistedState.ProcessedBuckets++

			keyCopy := cloneBytes(k)
			if err := m.bulkBucket(
				writer, nested, keyCopy, &id,
				path.AppendBucket(keyCopy),
			); err != nil {
				return err
			}

			continue
		}

		if err := writer.addLeaf(id, k, v); err != nil {
			return fmt.Errorf("failed to add leaf at path %v "+
				"key %s: %w", path, loggableKeyName(k), err)
		}

		m.migration.persistedState.ProcessedKeys++
		m.migratedKeysSinceStart++
		m.logMigrationProgress(path)
	}

	return nil
}

// bulkVerifyFrontier tracks a source bucket and the corresponding SQL target
// row id waiting for verification.
type bulkVerifyFrontier struct {
	source kvdb.RBucket
	id     int64
	path   BucketPath
}

// VerifyMigrationBulk verifies a bulk-migrated SQL KV target using the
// backend's migration-only batched verification API.
func (m *Migrator) VerifyMigrationBulk(ctx context.Context,
	sourceDB kvdb.Backend, target sqlbase.MigrationBackend) error {

	m.startTimeVerification = time.Now()
	m.cfg.Logger.Infof("Bulk-verifying database with prefix `%s`",
		m.cfg.DBPrefixName)

	if err := m.migration.read(); err != nil {
		return fmt.Errorf("failed to read migration state: %w", err)
	}
	if !m.migration.persistedState.Finished {
		return fmt.Errorf("migration not finished; refusing to verify")
	}

	m.verification.persistedState = newPersistedState()

	verifier, err := target.BeginBulkVerify(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin bulk verification: %w", err)
	}
	defer func() {
		_ = verifier.Rollback()
	}()

	err = sourceDB.View(func(sourceTx kvdb.RTx) error {
		level, err := m.bulkVerifyTopLevel(ctx, sourceTx, verifier)
		if err != nil {
			return err
		}

		for len(level) > 0 {
			var next []bulkVerifyFrontier
			for start := 0; start < len(level); {
				end := start + DefaultBulkVerifyBatchSize
				if end > len(level) {
					end = len(level)
				}

				children, err := m.bulkVerifyBatch(
					ctx, verifier, level[start:end],
				)
				if err != nil {
					return err
				}

				next = append(next, children...)
				start = end
			}
			level = next
		}

		return nil
	}, func() {})
	if err != nil {
		return err
	}

	if m.migration.persistedState.ProcessedKeys !=
		m.verification.persistedState.ProcessedKeys {

		return fmt.Errorf("processed keys mismatch: migration=%d "+
			"verification=%d", m.migration.persistedState.ProcessedKeys,
			m.verification.persistedState.ProcessedKeys)
	}
	if m.migration.persistedState.ProcessedBuckets !=
		m.verification.persistedState.ProcessedBuckets {

		return fmt.Errorf("processed buckets mismatch: migration=%d "+
			"verification=%d",
			m.migration.persistedState.ProcessedBuckets,
			m.verification.persistedState.ProcessedBuckets)
	}

	m.verification.setFinalState()
	if err := m.verification.write(); err != nil {
		return err
	}

	m.logFinalStats()

	return nil
}

// bulkVerifyTopLevel matches source top-level buckets to target top-level rows.
func (m *Migrator) bulkVerifyTopLevel(ctx context.Context, sourceTx kvdb.RTx,
	verifier sqlbase.MigrationBulkKVVerifier) ([]bulkVerifyFrontier, error) {

	top, err := verifier.FetchTopLevel(ctx)
	if err != nil {
		return nil, err
	}

	topByKey := make(map[string]sqlbase.MigrationBulkChild, len(top))
	for _, child := range top {
		topByKey[string(child.Key)] = child
	}

	sourceRoots := make(map[string]struct{}, len(topByKey))
	var level []bulkVerifyFrontier
	err = sourceTx.ForEachBucket(func(name []byte) error {
		if bytes.Equal(name, channeldb.TombstoneKey) {
			m.cfg.Logger.Info("Tombstone key root bucket found, " +
				"skipping")

			return nil
		}

		sourceBucket := sourceTx.ReadBucket(name)
		if sourceBucket == nil {
			return fmt.Errorf("source root bucket missing: %s",
				loggableKeyName(name))
		}

		targetRow, ok := topByKey[string(name)]
		if !ok {
			return fmt.Errorf("root bucket missing in target: %s",
				loggableKeyName(name))
		}
		if !targetRow.IsBucket {
			return fmt.Errorf("type mismatch at root %s: source is "+
				"a bucket, target is a value", loggableKeyName(name))
		}
		if sourceBucket.Sequence() != targetRow.Sequence {
			return fmt.Errorf("sequence mismatch at root %s: "+
				"source=%d target=%d", loggableKeyName(name),
				sourceBucket.Sequence(), targetRow.Sequence)
		}

		sourceRoots[string(name)] = struct{}{}
		m.verification.persistedState.ProcessedBuckets++
		level = append(level, bulkVerifyFrontier{
			source: sourceBucket,
			id:     targetRow.ID,
			path:   NewBucketPath([][]byte{name}),
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	for key, child := range topByKey {
		if _, ok := sourceRoots[key]; ok {
			continue
		}

		return nil, fmt.Errorf("target has an unexpected top-level "+
			"row not present in source: %s",
			loggableKeyName(child.Key))
	}

	return level, nil
}

// bulkVerifyBatch fetches direct children for a batch of target bucket ids and
// compares them against their corresponding source buckets.
func (m *Migrator) bulkVerifyBatch(ctx context.Context,
	verifier sqlbase.MigrationBulkKVVerifier,
	batch []bulkVerifyFrontier) ([]bulkVerifyFrontier, error) {

	parentIDs := make([]int64, len(batch))
	batchByID := make(map[int64]bulkVerifyFrontier, len(batch))
	for i, item := range batch {
		parentIDs[i] = item.id
		batchByID[item.id] = item
	}

	children, err := verifier.FetchChildren(ctx, parentIDs)
	if err != nil {
		return nil, err
	}

	childrenByParent := make(map[int64][]sqlbase.MigrationBulkChild)
	for _, child := range children {
		if child.ParentID == nil {
			return nil, fmt.Errorf("target child row %d has no "+
				"parent id", child.ID)
		}
		if _, ok := batchByID[*child.ParentID]; !ok {
			return nil, fmt.Errorf("unexpected parent id %d in "+
				"bulk verification result", *child.ParentID)
		}

		childrenByParent[*child.ParentID] = append(
			childrenByParent[*child.ParentID], child,
		)
	}

	var next []bulkVerifyFrontier
	for _, item := range batch {
		subBuckets, err := m.bulkVerifyBucket(
			item, childrenByParent[item.id],
		)
		if err != nil {
			return nil, err
		}

		next = append(next, subBuckets...)
	}

	return next, nil
}

// bulkVerifyBucket compares one source bucket against the already-fetched
// target child rows and returns nested buckets to verify in the next batch.
func (m *Migrator) bulkVerifyBucket(item bulkVerifyFrontier,
	children []sqlbase.MigrationBulkChild) ([]bulkVerifyFrontier, error) {

	// The source cursor walks keys in strict bytewise order (a bolt
	// guarantee), and we compare it positionally against the target
	// children below. Sort the target rows into the same bytewise order so
	// verification does not depend on the order in which the backend's
	// FetchChildren happens to return them.
	sort.Slice(children, func(i, j int) bool {
		return bytes.Compare(children[i].Key, children[j].Key) < 0
	})

	cursor := item.source.ReadCursor()
	sourceKey, sourceValue := cursor.First()

	var next []bulkVerifyFrontier
	for _, child := range children {
		if sourceKey == nil {
			return nil, fmt.Errorf("target bucket has extra entries "+
				"at path %v, extra key %s", item.path,
				loggableKeyName(child.Key))
		}
		if !bytes.Equal(sourceKey, child.Key) {
			return nil, fmt.Errorf("key mismatch at path %v: "+
				"source=%s target=%s", item.path,
				loggableKeyName(sourceKey),
				loggableKeyName(child.Key))
		}

		switch {
		case sourceValue == nil:
			subBucket, err := m.bulkVerifyNestedBucket(item, child)
			if err != nil {
				return nil, err
			}

			next = append(next, subBucket)

		case child.IsBucket:
			return nil, fmt.Errorf("type mismatch at path %v "+
				"key %s: source is a value, target is a bucket",
				item.path, loggableKeyName(sourceKey))

		case !bytes.Equal(sourceValue, child.Value):
			return nil, fmt.Errorf("value mismatch at path %v "+
				"key %s", item.path, loggableKeyName(sourceKey))

		default:
			m.verification.persistedState.ProcessedKeys++
			m.verifiedKeysSinceStart++
		}

		sourceKey, sourceValue = cursor.Next()
	}

	if sourceKey != nil {
		return nil, fmt.Errorf("target bucket has fewer entries at "+
			"path %v, missing key %s", item.path,
			loggableKeyName(sourceKey))
	}

	m.logVerificationProgress(item.path)

	return next, nil
}

// bulkVerifyNestedBucket verifies that child represents the same nested bucket
// as the source key currently being compared.
func (m *Migrator) bulkVerifyNestedBucket(item bulkVerifyFrontier,
	child sqlbase.MigrationBulkChild) (bulkVerifyFrontier, error) {

	if !child.IsBucket {
		return bulkVerifyFrontier{}, fmt.Errorf("type mismatch at "+
			"path %v key %s: source is a nested bucket, target is "+
			"a value", item.path, loggableKeyName(child.Key))
	}

	sourceNested := item.source.NestedReadBucket(child.Key)
	if sourceNested == nil {
		return bulkVerifyFrontier{}, fmt.Errorf("source nested "+
			"bucket access failed at path %v key %s", item.path,
			loggableKeyName(child.Key))
	}
	if sourceNested.Sequence() != child.Sequence {
		return bulkVerifyFrontier{}, fmt.Errorf("sequence mismatch "+
			"at path %v key %s: source=%d target=%d", item.path,
			loggableKeyName(child.Key), sourceNested.Sequence(),
			child.Sequence)
	}

	keyCopy := cloneBytes(child.Key)
	m.verification.persistedState.ProcessedBuckets++

	return bulkVerifyFrontier{
		source: sourceNested,
		id:     child.ID,
		path:   item.path.AppendBucket(keyCopy),
	}, nil
}

// cloneBytes returns an owned copy of b.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)

	return out
}
