package migratekvdb

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// BucketPath represents a path in the database with both string and raw
// representations.
type BucketPath struct {
	// StringPath is the hex-encoded path with / delimiters for logging.
	StringPath string

	// RawPath contains the original bucket names as raw bytes.
	RawPath [][]byte
}

// NewBucketPath creates a bucket path from raw bucket names.
func NewBucketPath(buckets [][]byte) BucketPath {
	// Create hex encoded version for string representation.
	stringParts := make([]string, len(buckets))
	for i, bucket := range buckets {
		stringParts[i] = loggableKeyName(bucket)
	}

	return BucketPath{
		StringPath: strings.Join(stringParts, "/"),
		RawPath:    buckets,
	}
}

// HasPath returns true if the BucketPath contains any path elements.
func (bp BucketPath) HasPath() bool {
	return len(bp.RawPath) > 0
}

// AppendBucket creates a new BucketPath with an additional bucket.
func (bp BucketPath) AppendBucket(bucket []byte) BucketPath {
	newRawPath := make([][]byte, len(bp.RawPath)+1)
	copy(newRawPath, bp.RawPath)
	newRawPath[len(bp.RawPath)] = bucket

	// Create new string path.
	newStringPath := bp.StringPath
	if newStringPath != "" {
		newStringPath += "/"
	}
	newStringPath += loggableKeyName(bucket)

	return BucketPath{
		StringPath: newStringPath,
		RawPath:    newRawPath,
	}
}

// Equal compares two bucket paths for equality.
func (bp BucketPath) Equal(other BucketPath) bool {
	if len(bp.RawPath) != len(other.RawPath) {
		return false
	}

	for i := range bp.RawPath {
		if !bytes.Equal(bp.RawPath[i], other.RawPath[i]) {
			return false
		}
	}

	return true
}

// CreateChunkKey creates a key using raw bucket names and the last key in the
// chunk to make sure the chunk key is unique.
func (bp BucketPath) CreateChunkKey(last []byte) []byte {
	// Use raw bucket names for hash key creation.
	var key bytes.Buffer
	for _, bucket := range bp.RawPath {
		key.Write(bucket)
		// Add separator between buckets.
		key.WriteByte(0)
	}
	key.Write(last)
	return key.Bytes()
}

// String implements the Stringer interface.
func (bp BucketPath) String() string {
	return bp.StringPath
}

// MarshalJSON implements the json.Marshaler interface by marshaling
// the raw byte arrays directly.
func (bp BucketPath) MarshalJSON() ([]byte, error) {
	// Marshal the raw paths directly.
	return json.Marshal(bp.RawPath)
}

// UnmarshalJSON implements the json.Unmarshaler interface by unmarshaling
// directly into the raw byte arrays.
func (bp *BucketPath) UnmarshalJSON(data []byte) error {
	// Unmarshal into raw paths.
	var rawPath [][]byte
	if err := json.Unmarshal(data, &rawPath); err != nil {
		return fmt.Errorf("failed to unmarshal bucket path: %w", err)
	}

	// Create the string path from the raw paths.
	hexParts := make([]string, len(rawPath))
	for i, bucket := range rawPath {
		hexParts[i] = hex.EncodeToString(bucket)
	}

	// Set both representations.
	bp.RawPath = rawPath
	bp.StringPath = strings.Join(hexParts, "/")
	return nil
}
