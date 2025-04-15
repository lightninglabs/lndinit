package migratekvdb

import "errors"

var (
	// errNoMetaBucket is returned when the migration metadata bucket is
	// not found.
	errNoMetaBucket = errors.New("migration metadata bucket not " +
		"found")

	// errNoStateFound is returned when the migration state is not found.
	errNoStateFound = errors.New("no migration state found")

	// errChunkSizeExceeded is returned when the chunk size limit is reached
	// during migration, indicating that the migration should continue
	// with a new transaction. It should close the reading and write
	// transaction and continue where it stopped.
	errChunkSizeExceeded = errors.New("chunk size exceeded")

	// errMigrationComplete is returned when the migration is already
	// completed.
	errMigrationComplete = errors.New("migration already completed")

	// errVerificationComplete is returned when the verification is already
	// completed.
	errVerificationComplete = errors.New("verification already completed")
)
