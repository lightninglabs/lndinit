package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// discardWriter is an io.Writer that drops everything, to silence test logs.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// edgeCaseCounts holds the exact leaf-key and bucket counts written by
// buildEdgeCaseBoltSource, so tests can assert the migrator counted the same.
type edgeCaseCounts struct {
	leaves  int
	buckets int
}

// buildEdgeCaseBoltSource creates a bolt DB at path that deliberately exercises
// the tricky shapes the migration + verification must handle:
//   - empty ([]byte{}) leaf values (the SQLite nil-vs-empty regression)
//   - non-printable/binary keys and values
//   - keys where one is a prefix of another (cursor ordering / seek edges)
//   - a bucket with an explicit sequence number
//   - an empty bucket (no entries)
//   - multi-level nested buckets with leaves and sub-buckets interleaved by key
//
// It returns the exact counts written.
func buildEdgeCaseBoltSource(t *testing.T, path string) edgeCaseCounts {
	t.Helper()

	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout: time.Minute,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	var c edgeCaseCounts

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		// 1) Flat bucket with an empty value, binary key/value, and
		//    prefix-overlapping keys.
		flat, err := tx.CreateBucket([]byte("flat"))
		require.NoError(t, err)
		c.buckets++

		put := func(b *bbolt.Bucket, k, v []byte) {
			require.NoError(t, b.Put(k, v))
			c.leaves++
		}
		put(flat, []byte("alpha"), []byte("value-alpha"))
		put(flat, []byte("empty"), []byte{})              // empty value
		put(flat, []byte("k"), []byte("v"))               // prefix of "k1"
		put(flat, []byte("k1"), []byte("v1"))             // prefix of "k10"
		put(flat, []byte("k10"), []byte("v10"))           // ordering edge
		put(flat, []byte{0x00, 0xff, 0x10}, []byte{0x00}) // binary key/value
		put(flat, []byte("zeros"), []byte{0, 0, 0, 0})    // non-empty zero bytes

		// 2) Bucket with an explicit sequence number set.
		seqB, err := tx.CreateBucket([]byte("with-seq"))
		require.NoError(t, err)
		c.buckets++
		require.NoError(t, seqB.SetSequence(4242))
		put(seqB, []byte("one"), []byte("1"))
		put(seqB, []byte("two"), []byte("2"))

		// 3) An empty bucket (no entries).
		_, err = tx.CreateBucket([]byte("empty-bucket"))
		require.NoError(t, err)
		c.buckets++

		// 4) Nested buckets: leaves and sub-buckets interleaved by key
		//    order, three levels deep, one nested bucket carrying a
		//    sequence, and a nested empty value.
		nested, err := tx.CreateBucket([]byte("nested"))
		require.NoError(t, err)
		c.buckets++
		// Keys chosen so leaves and sub-buckets interleave when sorted:
		// "a-leaf" < "m-sub" < "z-leaf".
		put(nested, []byte("a-leaf"), []byte("first"))
		put(nested, []byte("z-leaf"), []byte{}) // empty value in nested

		sub, err := nested.CreateBucket([]byte("m-sub"))
		require.NoError(t, err)
		c.buckets++
		require.NoError(t, sub.SetSequence(7))
		put(sub, []byte("s1"), []byte("sv1"))
		put(sub, []byte("s2"), []byte("sv2"))

		subsub, err := sub.CreateBucket([]byte("deep"))
		require.NoError(t, err)
		c.buckets++
		put(subsub, []byte("d1"), []byte("deepval"))

		return nil
	}))

	return c
}

// compareBackends independently walks the source and target key/value stores and
// asserts they are byte-for-byte identical (nil and empty values treated as
// equal, matching bolt/SQL round-trip semantics). This is an independent check,
// separate from the migrator's own VerifyMigration.
func compareBackends(t *testing.T, src, target kvdb.Backend) {
	t.Helper()

	var walk func(sb, tb kvdb.RBucket, path string)
	walk = func(sb, tb kvdb.RBucket, path string) {
		require.NoError(t, sb.ForEach(func(k, v []byte) error {
			if v == nil {
				// Nested bucket on the source side.
				sn := sb.NestedReadBucket(k)
				tn := tb.NestedReadBucket(k)
				require.NotNil(t, tn, "missing nested bucket %s/%x",
					path, k)
				require.Equal(t, sn.Sequence(), tn.Sequence(),
					"sequence mismatch at %s/%x", path, k)
				walk(sn, tn, path+"/"+string(k))
				return nil
			}

			tv := tb.Get(k)
			require.True(t, bytes.Equal(v, tv),
				"value mismatch at %s/%x: src=%x target=%x",
				path, k, v, tv)
			return nil
		}))
	}

	require.NoError(t, src.View(func(stx kvdb.RTx) error {
		return target.View(func(ttx kvdb.RTx) error {
			return stx.ForEachBucket(func(name []byte) error {
				sb := stx.ReadBucket(name)
				tb := ttx.ReadBucket(name)
				require.NotNil(t, tb, "missing root bucket %x", name)
				require.Equal(t, sb.Sequence(), tb.Sequence(),
					"root sequence mismatch %x", name)
				walk(sb, tb, string(name))
				return nil
			})
		}, func() {})
	}, func() {}))
}
