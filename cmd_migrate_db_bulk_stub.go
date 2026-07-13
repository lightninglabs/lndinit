//go:build !kvdb_postgres

package main

import (
	"context"
	"fmt"

	"github.com/lightningnetwork/lnd/kvdb/postgres"
)

// openPostgresBulkBackend reports that bulk migration is unavailable in builds
// without the kvdb_postgres tag.
func openPostgresBulkBackend(context.Context, *postgres.Config,
	string) (*destinationBackend, error) {

	return nil, fmt.Errorf("postgres bulk migration support is not " +
		"available; rebuild with -tags=kvdb_postgres")
}
