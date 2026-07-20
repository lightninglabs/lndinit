//go:build kvdb_postgres

package main

import (
	"context"

	"github.com/lightninglabs/lndinit/migratekvdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
)

// postgresBulkMigrationRunner contains the Postgres-only migration backend.
// Keeping this type in a tagged file prevents its tagged lnd APIs from leaking
// into the default build.
type postgresBulkMigrationRunner struct {
	target sqlbase.MigrationBackend
}

// migrate runs the Postgres-only bulk migration against the runner's target.
func (p *postgresBulkMigrationRunner) migrate(ctx context.Context,
	migrator *migratekvdb.Migrator, source kvdb.Backend,
	resetTarget bool) error {

	return migrator.MigrateBulk(ctx, source, p.target, resetTarget)
}

// verify compares the bulk-migrated target against the source database.
func (p *postgresBulkMigrationRunner) verify(ctx context.Context,
	migrator *migratekvdb.Migrator, source kvdb.Backend) error {

	return migrator.VerifyMigrationBulk(ctx, source, p.target)
}

// openPostgresBulkBackend opens a Postgres destination with the migration-only
// bulk capabilities enabled.
func openPostgresBulkBackend(ctx context.Context, cfg *postgres.Config,
	prefix string) (*destinationBackend, error) {

	db, err := postgres.NewMigrationBackend(ctx, cfg, prefix)
	if err != nil {
		return nil, err
	}

	return &destinationBackend{
		Backend: db,
		bulk: &postgresBulkMigrationRunner{
			target: db,
		},
	}, nil
}
