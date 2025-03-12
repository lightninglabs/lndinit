package migratekvdb

import (
	"encoding/hex"

	"github.com/btcsuite/btcwallet/walletdb"
)

// loggableKeyName returns a string representation of a bucket key suitable for
// logging.
func loggableKeyName(key []byte) string {
	// For known bucket names, return as string if printable ASCII.
	if isPrintableASCII(key) {
		return string(key)
	}
	// Otherwise return hex encoding
	return hex.EncodeToString(key)
}

// hasSpecialChars returns true if any of the characters in the given string
// cannot be printed.
func isPrintableASCII(b []byte) bool {
	for _, c := range b {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

// getOrCreateMetaBucket gets or creates the migration metadata bucket
func getOrCreateMetaBucket(tx walletdb.ReadWriteTx) (
	walletdb.ReadWriteBucket, error) {

	metaBucket := tx.ReadWriteBucket([]byte(migrationMetaBucket))
	if metaBucket != nil {
		return metaBucket, nil
	}

	return tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
}
