//go:build !kvdb_postgres

package main

import (
	"context"
	"testing"

	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/stretchr/testify/require"
)

// TestOpenPostgresBulkBackendRequiresBuildTag verifies that a binary built
// without Postgres support reports how to enable bulk migration.
func TestOpenPostgresBulkBackendRequiresBuildTag(t *testing.T) {
	t.Parallel()

	_, err := openPostgresBulkBackend(
		context.Background(), &postgres.Config{}, "test",
	)
	require.ErrorContains(t, err, "rebuild with -tags=kvdb_postgres")
}
