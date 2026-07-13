package migratekvdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/kvdb"
)

// DefaultVerifyBatchSize is the number of buckets whose direct children are
// fetched in a single query during batched verification.
const DefaultVerifyBatchSize = 2000

// MaxVerifyBatchSize caps the batch so the constructed id-array literal stays
// reasonable.
const MaxVerifyBatchSize = 20000

// vFrontier is a bucket pending verification: its source handle, its target row
// id, and its path (for error messages / progress).
type vFrontier struct {
	src  kvdb.RBucket
	id   int64
	path BucketPath
}

// VerifyBatchedSQL verifies a bolt source against a SQL target using
// breadth-first, batched child fetches. The direct children of up to batchSize
// buckets are read in one query (WHERE parent_id = ANY(...)), instead of one
// query per bucket. This collapses the ~2-3 round-trips per bucket that
// dominate on bucket-heavy databases into ~total_buckets/batchSize queries,
// while keeping every query a plain index scan on (parent_id, key): no
// server-side sort, no work_mem blow-up, and O(level) client memory.
//
// The batch result is streamed and compared against each parent's source cursor
// inline, so a batch that happens to include a very wide bucket does not buffer
// all of its rows in memory.
//
// It is a fresh-only verification for a SQL target reached via a *sql.DB (pgx
// driver). The hierarchical ForAll/cursor path (VerifyMigration) remains for
// bolt targets and for resume.
func (m *Migrator) VerifyBatchedSQL(ctx context.Context, sourceDB kvdb.Backend,
	db *sql.DB, table string, batchSize int) error {

	switch {
	case batchSize <= 0:
		batchSize = DefaultVerifyBatchSize
	case batchSize > MaxVerifyBatchSize:
		batchSize = MaxVerifyBatchSize
	}

	m.startTimeVerification = time.Now()
	m.cfg.Logger.Infof("Batched verification of db with prefix `%s` from "+
		"table `%s` (batch size %d)", m.cfg.DBPrefixName, table, batchSize)

	// We need the finished migration state for the final count cross-check.
	if err := m.migration.read(); err != nil {
		return fmt.Errorf("failed to read migration state: %w", err)
	}
	if !m.migration.persistedState.Finished {
		return fmt.Errorf("migration not finished; refusing to verify")
	}

	// Batched verification always runs fresh.
	m.verification.persistedState = newPersistedState()

	// Run every target read inside a single read-only, repeatable-read
	// transaction so all queries observe one consistent snapshot (matching
	// the snapshot semantics of the kvdb.View path). It also keeps the whole
	// verification on one connection, so the prepared statement is reused.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly:  true,
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = sourceDB.View(func(stx kvdb.RTx) error {
		// Bootstrap: match source top-level buckets to target top-level
		// rows (parent_id IS NULL).
		topByKey, err := m.fetchTopLevel(ctx, tx, table)
		if err != nil {
			return err
		}

		var level []vFrontier
		srcRoots := make(map[string]struct{}, len(topByKey))
		err = stx.ForEachBucket(func(name []byte) error {
			// Skip the tombstone marker, mirroring the original verify.
			if bytes.Equal(name, channeldb.TombstoneKey) {
				return nil
			}

			src := stx.ReadBucket(name)
			if src == nil {
				return fmt.Errorf("source root bucket missing: %s",
					loggableKeyName(name))
			}

			tr, ok := topByKey[string(name)]
			if !ok {
				return fmt.Errorf("root bucket missing in target: "+
					"%s", loggableKeyName(name))
			}
			if tr.value != nil {
				return fmt.Errorf("type mismatch at root %s: source "+
					"is a bucket, target is a value",
					loggableKeyName(name))
			}
			if src.Sequence() != uint64(tr.seq) {
				return fmt.Errorf("sequence mismatch at root %s: "+
					"source=%d target=%d", loggableKeyName(name),
					src.Sequence(), tr.seq)
			}

			srcRoots[string(name)] = struct{}{}
			m.verification.persistedState.ProcessedBuckets++
			level = append(level, vFrontier{
				src:  src,
				id:   tr.id,
				path: NewBucketPath([][]byte{name}),
			})

			return nil
		})
		if err != nil {
			return err
		}

		// Assert the target has no extra top-level buckets beyond the
		// source's. This is safe (and not done by the sqlite-combining
		// compareDBs path) because each namespace has its own table in
		// postgres, so a stray top-level row here is real corruption. The
		// tombstone marker, if present, is not a source bucket and is
		// ignored.
		for k := range topByKey {
			if _, ok := srcRoots[k]; ok {
				continue
			}
			if bytes.Equal([]byte(k), channeldb.TombstoneKey) {
				continue
			}
			return fmt.Errorf("target has an unexpected top-level "+
				"bucket not present in source: %s",
				loggableKeyName([]byte(k)))
		}

		// Breadth-first, one level at a time, batching the child fetches.
		for len(level) > 0 {
			var next []vFrontier
			for start := 0; start < len(level); start += batchSize {
				end := start + batchSize
				if end > len(level) {
					end = len(level)
				}

				subs, err := m.verifyBatch(
					ctx, tx, table, level[start:end],
				)
				if err != nil {
					return err
				}
				next = append(next, subs...)
			}
			level = next
		}

		return nil
	}, func() {})
	if err != nil {
		return err
	}

	// Cross-check counts against the migration, exactly like VerifyMigration.
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

// fetchTopLevel returns the target's top-level (parent_id IS NULL) rows keyed by
// their key.
func (m *Migrator) fetchTopLevel(ctx context.Context, tx *sql.Tx,
	table string) (map[string]vChild, error) {

	rows, err := tx.QueryContext(ctx, "SELECT key, value, sequence, id "+
		"FROM "+table+" WHERE parent_id IS NULL ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]vChild)
	for rows.Next() {
		var c vChild
		var seq sql.NullInt64
		if err := rows.Scan(&c.key, &c.value, &seq, &c.id); err != nil {
			return nil, err
		}
		if seq.Valid {
			c.seq = seq.Int64
		}
		out[string(c.key)] = c
	}

	return out, rows.Err()
}

// vChild is a single target row (used for top-level bootstrap).
type vChild struct {
	key   []byte
	value []byte // nil => sub-bucket
	seq   int64
	id    int64
}

// verifyBatch fetches the direct children of every bucket in batch with a single
// query and compares them, streaming, against each bucket's source cursor. It
// returns the sub-buckets discovered (the next BFS level).
func (m *Migrator) verifyBatch(ctx context.Context, tx *sql.Tx, table string,
	batch []vFrontier) ([]vFrontier, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	byID := make(map[int64]vFrontier, len(batch))
	var ids strings.Builder
	ids.WriteByte('{')
	for i, it := range batch {
		if i > 0 {
			ids.WriteByte(',')
		}
		ids.WriteString(strconv.FormatInt(it.id, 10))
		byID[it.id] = it
	}
	ids.WriteByte('}')

	// Constant SQL (single bound array param) => one prepared statement
	// reused for every batch, so pgx's statement cache is not defeated.
	rows, err := tx.QueryContext(ctx, "SELECT parent_id, key, value, "+
		"sequence, id FROM "+table+" WHERE parent_id = ANY($1::bigint[]) "+
		"ORDER BY parent_id, key", ids.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []vFrontier
	seen := make(map[int64]bool, len(batch))

	// Streaming state for the bucket currently being compared.
	var (
		curPID  int64 = -1
		curItem vFrontier
		curCur  kvdb.RCursor
		sk, sv  []byte
		haveCur bool
	)

	finalize := func() error {
		if !haveCur {
			return nil
		}
		// Source must be exhausted; any leftover source key is missing
		// from the target.
		if sk != nil {
			return fmt.Errorf("target bucket has fewer entries at "+
				"path %v, missing key %s", curItem.path,
				loggableKeyName(sk))
		}
		m.logVerificationProgress(curItem.path)

		return nil
	}

	for rows.Next() {
		var (
			pid, cid int64
			tk, tv   []byte
			seq      sql.NullInt64
		)
		if err := rows.Scan(&pid, &tk, &tv, &seq, &cid); err != nil {
			return nil, err
		}

		if pid != curPID {
			if err := finalize(); err != nil {
				return nil, err
			}
			it, ok := byID[pid]
			if !ok {
				return nil, fmt.Errorf("unexpected parent_id %d "+
					"in batch result", pid)
			}
			curPID, curItem, haveCur = pid, it, true
			seen[pid] = true
			curCur = it.src.ReadCursor()
			sk, sv = curCur.First()
		}

		if sk == nil {
			return nil, fmt.Errorf("target bucket has extra entries "+
				"at path %v, extra key %s", curItem.path,
				loggableKeyName(tk))
		}
		if !bytes.Equal(sk, tk) {
			return nil, fmt.Errorf("key mismatch at path %v: "+
				"source=%s target=%s", curItem.path,
				loggableKeyName(sk), loggableKeyName(tk))
		}

		if sv == nil {
			// Source is a nested bucket; target must be one too.
			if tv != nil {
				return nil, fmt.Errorf("type mismatch at path %v "+
					"key %s: source is a nested bucket, "+
					"target is a value", curItem.path,
					loggableKeyName(sk))
			}

			srcNested := curItem.src.NestedReadBucket(sk)
			if srcNested == nil {
				return nil, fmt.Errorf("source nested bucket "+
					"access failed at path %v key %s",
					curItem.path, loggableKeyName(sk))
			}
			var s int64
			if seq.Valid {
				s = seq.Int64
			}
			if srcNested.Sequence() != uint64(s) {
				return nil, fmt.Errorf("sequence mismatch at path "+
					"%v key %s: source=%d target=%d",
					curItem.path, loggableKeyName(sk),
					srcNested.Sequence(), s)
			}

			kc := make([]byte, len(sk))
			copy(kc, sk)
			subs = append(subs, vFrontier{
				src:  srcNested,
				id:   cid,
				path: curItem.path.AppendBucket(kc),
			})
			m.verification.persistedState.ProcessedBuckets++
		} else {
			// Leaf: compare values. We deliberately do NOT log values
			// in the error to avoid exposing content.
			if !bytes.Equal(sv, tv) {
				return nil, fmt.Errorf("value mismatch at path %v "+
					"key %s", curItem.path, loggableKeyName(sk))
			}
			m.verification.persistedState.ProcessedKeys++
			m.verifiedKeysSinceStart++
		}

		sk, sv = curCur.Next()
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := finalize(); err != nil {
		return nil, err
	}

	// Any batched bucket with no rows returned must be empty on the source
	// side too.
	for _, it := range batch {
		if seen[it.id] {
			continue
		}
		if k, _ := it.src.ReadCursor().First(); k != nil {
			return nil, fmt.Errorf("target bucket is empty but "+
				"source is not at path %v, missing key %s",
				it.path, loggableKeyName(k))
		}
		m.logVerificationProgress(it.path)
	}

	return subs, nil
}
