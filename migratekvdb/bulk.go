package migratekvdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lightningnetwork/lnd/kvdb"
)

// DefaultBulkBatchSize is the number of leaf rows accumulated before a single
// multi-row INSERT is issued. At 3 params/row this stays well under postgres'
// 65535-parameter statement limit.
const DefaultBulkBatchSize = 1000

// MaxBulkBatchSize is the largest permitted batch size. Each row contributes 3
// bind parameters (parent_id, key, value); postgres caps a statement at 65535
// parameters, so we cap rows at 65535/3 = 21845 to guarantee we never exceed it.
const MaxBulkBatchSize = 65535 / 3

// bulkRow is a buffered leaf key/value pair destined for the given bucket.
type bulkRow struct {
	parentID int64
	key      []byte
	value    []byte
}

// bulkWriter accumulates leaf rows and flushes them as multi-row INSERTs within
// a single SQL transaction. Bucket rows are inserted immediately (they need
// RETURNING id, and their children reference them via the parent_id FK).
type bulkWriter struct {
	ctx       context.Context
	tx        *sql.Tx
	table     string
	batchSize int
	rows      []bulkRow
}

// insertBucket inserts a bucket row and returns its generated id. parentID is
// nil for a top-level bucket (parent_id NULL). A non-zero sequence is persisted.
//
// The bucket row is inserted immediately (not buffered) because its generated
// id is needed right away for its children's parent_id, and because the
// parent_id foreign key requires the parent row to exist before any child row
// referencing it is inserted.
func (bw *bulkWriter) insertBucket(parentID *int64, key []byte,
	seq uint64) (int64, error) {

	kc := make([]byte, len(key))
	copy(kc, key)

	var id int64
	if seq != 0 {
		err := bw.tx.QueryRowContext(bw.ctx,
			"INSERT INTO "+bw.table+" (parent_id, key, sequence) "+
				"VALUES ($1,$2,$3) RETURNING id",
			parentID, kc, int64(seq),
		).Scan(&id)

		return id, err
	}

	err := bw.tx.QueryRowContext(bw.ctx,
		"INSERT INTO "+bw.table+" (parent_id, key) "+
			"VALUES ($1,$2) RETURNING id",
		parentID, kc,
	).Scan(&id)

	return id, err
}

// addLeaf buffers a leaf key/value pair, flushing when the batch is full. The
// key and value are copied because the source cursor reuses its buffers.
func (bw *bulkWriter) addLeaf(parentID int64, k, v []byte) error {
	kc := make([]byte, len(k))
	copy(kc, k)

	// Use make+copy (never append(nil,...)) so an empty-but-non-nil value is
	// stored as an empty BYTEA rather than NULL (NULL means "sub-bucket").
	vc := make([]byte, len(v))
	copy(vc, v)

	bw.rows = append(bw.rows, bulkRow{parentID: parentID, key: kc, value: vc})
	if len(bw.rows) >= bw.batchSize {
		return bw.flush()
	}

	return nil
}

// flush writes all buffered leaf rows as a single multi-row INSERT.
func (bw *bulkWriter) flush() error {
	if len(bw.rows) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO " + bw.table + " (parent_id, key, value) VALUES ")
	args := make([]interface{}, 0, len(bw.rows)*3)
	for i, r := range bw.rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * 3
		fmt.Fprintf(&sb, "($%d,$%d,$%d)", n+1, n+2, n+3)
		args = append(args, r.parentID, r.key, r.value)
	}

	_, err := bw.tx.ExecContext(bw.ctx, sb.String(), args...)
	bw.rows = bw.rows[:0]

	return err
}

// MigrateBulkSQL performs a fresh bolt->SQL migration using batched multi-row
// INSERTs instead of one INSERT per key, collapsing the per-key round-trips that
// dominate a networked postgres migration.
//
// It writes directly to the target table via the provided *sql.DB (opened with
// the same driver as the kvdb backend, e.g. "pgx"), bypassing kvdb's per-row
// Put. The entire migration runs in a single transaction: on success the
// migration state is marked finished; on error nothing is committed.
//
// NOTE: This path is fresh-only. Resume is not supported here; callers should
// use Migrate for resuming an interrupted migration.
func (m *Migrator) MigrateBulkSQL(ctx context.Context, sourceDB kvdb.Backend,
	db *sql.DB, table string, batchSize int) error {

	switch {
	case batchSize <= 0:
		batchSize = DefaultBulkBatchSize

	case batchSize > MaxBulkBatchSize:
		m.cfg.Logger.Warnf("Requested bulk batch size %d exceeds the "+
			"maximum of %d (postgres parameter limit); clamping",
			batchSize, MaxBulkBatchSize)
		batchSize = MaxBulkBatchSize
	}

	m.cfg.Logger.Infof("Bulk-migrating database with prefix `%s` into `%s` "+
		"(batch size %d)", m.cfg.DBPrefixName, table, batchSize)

	// Read any existing migration state. Bulk is always a fresh migration,
	// but we still need to detect (a) an already-finished migration and
	// (b) a previous bulk attempt that was interrupted.
	if err := m.migration.read(); err != nil &&
		!errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound) {

		return fmt.Errorf("failed to read migration state: %w", err)
	}

	if m.migration.persistedState.Finished {
		m.cfg.Logger.Infof("Migration already finished")

		return nil
	}

	// If a previous bulk attempt set the in-progress marker, it may have
	// partially or even fully committed to the target before crashing (the
	// SQL target and this bolt meta DB cannot share a transaction). We own
	// the table in that case, so recovery is simply: truncate and redo.
	recovering := m.migration.persistedState.BulkInProgress

	// Reset to a fresh state; bulk never resumes mid-way.
	m.migration.persistedState = newPersistedState()

	if recovering {
		m.cfg.Logger.Warnf("Detected an interrupted previous bulk "+
			"migration; truncating target table %s and restarting",
			table)

		if _, err := db.ExecContext(
			ctx, "TRUNCATE TABLE "+table,
		); err != nil {
			return fmt.Errorf("failed to truncate target table %s "+
				"for recovery: %w", table, err)
		}
	} else {
		// Fresh run: the target table must be empty.
		var count int
		if err := db.QueryRowContext(
			ctx, "SELECT COUNT(*) FROM "+table,
		).Scan(&count); err != nil {
			return fmt.Errorf("failed to count rows in target table "+
				"%s: %w", table, err)
		}
		if count > 0 {
			return fmt.Errorf("target table %s is not empty (%d "+
				"rows); refusing to bulk-migrate", table, count)
		}
	}

	// Persist the in-progress marker BEFORE writing any data so that a crash
	// after the SQL commit (but before we record completion) is recoverable
	// via the truncate-and-retry path above.
	m.migration.persistedState.BulkInProgress = true
	if err := m.migration.write(); err != nil {
		return fmt.Errorf("failed to write bulk in-progress marker: %w",
			err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	bw := &bulkWriter{
		ctx:       ctx,
		tx:        tx,
		table:     table,
		batchSize: batchSize,
	}

	err = sourceDB.View(func(rtx kvdb.RTx) error {
		return rtx.ForEachBucket(func(name []byte) error {
			src := rtx.ReadBucket(name)
			if src == nil {
				return fmt.Errorf("source root bucket not found "+
					"for key %x", name)
			}

			m.migration.persistedState.ProcessedBuckets++

			return m.bulkBucket(bw, src, name, nil)
		})
	}, func() {})
	if err != nil {
		return err
	}

	if err := bw.flush(); err != nil {
		return fmt.Errorf("failed to flush final batch: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit bulk migration: %w", err)
	}

	m.cfg.Logger.Infof("Bulk migration complete, processed %d keys, "+
		"%d buckets", m.migration.persistedState.ProcessedKeys,
		m.migration.persistedState.ProcessedBuckets)

	// Clear the in-progress marker and record completion. The SQL data is
	// already durably committed; if this meta write fails, the next run
	// sees the marker still set and recovers via truncate-and-retry.
	m.migration.setFinalState()
	m.migration.persistedState.BulkInProgress = false

	return m.migration.write()
}

// bulkBucket inserts the bucket row for src, then walks its contents: leaf
// key/values are buffered for batched insertion; nested buckets are recursed
// into (inserted immediately so their children's parent_id FK is satisfied).
func (m *Migrator) bulkBucket(bw *bulkWriter, src kvdb.RBucket, key []byte,
	parentID *int64) error {

	// Emit progress once per bucket (time-gated inside the logger) so that
	// bucket-heavy databases don't appear hung while migrating many small
	// nested buckets.
	m.logMigrationProgress(NewBucketPath([][]byte{key}))

	id, err := bw.insertBucket(parentID, key, src.Sequence())
	if err != nil {
		return fmt.Errorf("failed to insert bucket row: %w", err)
	}

	cursor := src.ReadCursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		select {
		case <-bw.ctx.Done():
			return bw.ctx.Err()
		default:
		}

		if v == nil {
			// Nested bucket: must exist before its children reference
			// it, so flush pending leaves is unnecessary (parent row
			// already inserted). Recurse.
			sub := src.NestedReadBucket(k)
			if sub == nil {
				return fmt.Errorf("source nested bucket not found "+
					"for key %x", k)
			}

			m.migration.persistedState.ProcessedBuckets++

			if err := m.bulkBucket(bw, sub, k, &id); err != nil {
				return err
			}

			continue
		}

		if err := bw.addLeaf(id, k, v); err != nil {
			return fmt.Errorf("failed to add leaf: %w", err)
		}

		m.migration.persistedState.ProcessedKeys++
		m.migratedKeysSinceStart++
		m.logMigrationProgress(NewBucketPath([][]byte{key}))
	}

	return nil
}
