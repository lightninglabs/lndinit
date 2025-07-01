package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btclog/v2"
	"github.com/jessevdk/go-flags"
	"github.com/lightninglabs/lndinit/migratekvdb"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/healthcheck"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
	"github.com/lightningnetwork/lnd/kvdb/sqlite"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/signal"
	"github.com/lightningnetwork/lnd/watchtower/wtdb"
	"go.etcd.io/bbolt"
)

var (
	// alreadyMigratedKey is the key under which we add a tag in the target/
	// destination DB after we've successfully and completely migrated it
	// from a source DB.
	alreadyMigratedKey = []byte("data-migration-already-migrated")

	// defaultDataDir is the default data directory for lnd.
	defaultDataDir = filepath.Join(btcutil.AppDataDir("lnd", false), "data")
)

const (
	// walletMetaBucket is the name of the meta bucket in the wallet db
	// for the wallet ready marker.
	walletMetaBucket = "lnwallet"

	// walletReadyKey is the key in the wallet meta bucket for the wallet
	// ready marker.
	walletReadyKey = "ready"
)

// Bolt is the configuration for a bolt database.
type Bolt struct {
	DBTimeout time.Duration `long:"dbtimeout" description:"Specify the timeout value used when opening the database."`
	DataDir   string        `long:"data-dir" description:"Lnd data dir where bolt dbs are located."`
	TowerDir  string        `long:"tower-dir" description:"Lnd watchtower dir where bolt dbs for the watchtower server are located."`
}

// Sqlite is the configuration for a sqlite database.
type Sqlite struct {
	DataDir  string         `long:"data-dir" description:"Lnd data dir where sqlite dbs are located."`
	TowerDir string         `long:"tower-dir" description:"Lnd watchtower dir where sqlite dbs for the watchtower server are located."`
	Config   *sqlite.Config `group:"sqlite-config" namespace:"sqlite-config" description:"Sqlite config."`
}

// SourceDB represents the source database, which can only be bolt for now.
type SourceDB struct {
	Backend string `long:"backend" description:"The source database backend." choice:"bolt"`
	Bolt    *Bolt  `group:"bolt" namespace:"bolt" description:"Bolt settings."`
}

// DestDB represents the destination database, which can be either postgres or
// sqlite.
type DestDB struct {
	Backend  string           `long:"backend" description:"The destination database backend." choice:"postgres" choice:"sqlite"`
	Postgres *postgres.Config `group:"postgres" namespace:"postgres" description:"Postgres settings."`
	Sqlite   *Sqlite          `group:"sqlite" namespace:"sqlite" description:"Sqlite settings."`
}

// Init should be called upon start to pre-initialize database for sql
// backends. If max connections are not set, the amount of connections will be
// unlimited however we only use one connection during the migration.
func (db *DestDB) Init() error {
	switch {
	case db.Backend == lncfg.PostgresBackend:
		sqlbase.Init(db.Postgres.MaxConnections)

	case db.Backend == lncfg.SqliteBackend:
		sqlbase.Init(db.Sqlite.Config.MaxConnections)
	}

	return nil
}

type migrateDBCommand struct {
	Source            *SourceDB `group:"source" namespace:"source" long:"" short:"" description:""`
	Dest              *DestDB   `group:"dest" namespace:"dest" long:"" short:"" description:""`
	Network           string    `long:"network" short:"n" description:"Network of the db files to migrate (used to navigate into the right directory)"`
	PprofPort         int       `long:"pprof-port" description:"Enable pprof profiling on the specified port"`
	ForceNewMigration bool      `long:"force-new-migration" description:"Force a new migration from the beginning of the source DB so the resume state will be discarded"`
	ForceVerifyDB     bool      `long:"force-verify-db" description:"Force a verification verifies two already marked (tombstoned and already migrated) dbs to make sure that the source db equals the content of the destination db"`
	ChunkSize         uint64    `long:"chunk-size" description:"Chunk size for the migration in bytes"`
}

func newMigrateDBCommand() *migrateDBCommand {
	return &migrateDBCommand{
		Source: &SourceDB{
			Backend: lncfg.BoltBackend,
			Bolt: &Bolt{
				DBTimeout: kvdb.DefaultDBTimeout,
				TowerDir:  defaultDataDir,
				DataDir:   defaultDataDir,
			},
		},
		Dest: &DestDB{
			Backend:  lncfg.PostgresBackend,
			Postgres: &postgres.Config{},
			Sqlite: &Sqlite{
				Config:   &sqlite.Config{},
				TowerDir: defaultDataDir,
				DataDir:  defaultDataDir,
			},
		},
		Network: "mainnet",
	}
}

func (x *migrateDBCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"migrate-db",
		"Migrate the complete database state of lnd to a new backend",
		`
	Migrate the full database state of lnd from a source (for example the
	set of bolt database files such as channel.db and wallet.db) database
	to a SQL destination database.

	IMPORTANT: Please read the data migration guide located	in the file
	docs/data-migration.md of the main lnd repository before using this
	command!

	NOTE: The migration can take a long time depending on the amount of data
	that needs to be written! The migration happens in chunks therefore it
	can be resumed in case of an interruption. The migration also includes
	a verification to assure that the migration is consistent.
	As long as NEITHER the source nor destination database has been started/
	run with lnd, the migration can be repeated/resumed in case of an error
	since the data will just be overwritten again in the destination.

	Once a database was successfully and completely migrated from the source
	to the destination, the source will be marked with a 'tombstone' tag
	while the destination will get an 'already migrated' tag.
	A database with a tombstone cannot be started with lnd anymore to
	prevent from an old state being used by accident.
	To prevent overwriting a destination database by accident, the same
	database/namespace pair cannot be used as the target of a data migration
	twice, which is checked through the 'already migrated' tag.`,
		x,
	)
	return err
}

// optionalDBs are the databases that can be skipped if they don't
// exist.
var (
	optionalDBs = map[string]bool{
		lncfg.NSTowerClientDB: true,
		lncfg.NSTowerServerDB: true,
		lncfg.NSNeutrinoDB:    true,
	}

	// allDBPrefixes defines all databases that should be migrated.
	allDBPrefixes = []string{
		lncfg.NSChannelDB,
		lncfg.NSMacaroonDB,
		lncfg.NSDecayedLogDB,
		lncfg.NSTowerClientDB,
		lncfg.NSTowerServerDB,
		lncfg.NSWalletDB,
		lncfg.NSNeutrinoDB,
	}

	// databasesWithMigrations are the databases that have new versions
	// introduced during their lifetime and therefore need to be up to
	// date before we can start the migration to SQL backends.
	databasesWithMigrations = []string{
		lncfg.NSChannelDB,
		lncfg.NSTowerClientDB,
	}
)

func (x *migrateDBCommand) Execute(_ []string) error {
	// We currently only allow migrations from bolt to sqlite/postgres.
	if err := x.validateDBBackends(); err != nil {
		return fmt.Errorf("invalid database configuration: %w", err)
	}

	// We keep track of the DBs that we have migrated.
	migratedDBs := []string{}

	// Add pprof server if enabled.
	if x.PprofPort > 0 {
		go func() {
			pprofAddr := fmt.Sprintf("localhost:%d", x.PprofPort)
			logger.Infof("Starting pprof server on %s", pprofAddr)
			err := http.ListenAndServe(pprofAddr, nil)
			if err != nil {
				logger.Errorf("Error starting pprof "+
					"server: %v", err)
			}
		}()
	}

	// get the context for the migration which is tied to the signal
	// interceptor.
	ctx := getContext()

	// Before starting any migration, make sure that all existing dbs have
	// the latest migrations applied so that we do only migrate when all
	// dbs are up to date. For example it might happen that a user run a
	// watchtower client earlier and has it switched off since new
	// migrations have been introduced. In that case we do not start the
	// migration but first let the user either delete the outdated db or
	// migrate it by activating the watchtower client again.
	for _, prefix := range databasesWithMigrations {
		logger.Infof("Checking db version of database %s", prefix)

		// Check if the database file exists before trying to open it.
		dbPath := getBoltDBPath(x.Source, prefix, x.Network)
		if dbPath == "" {
			return fmt.Errorf("unknown prefix: %s", prefix)
		}

		// Open and check the database version.
		srcDb, err := openSourceDb(x.Source, prefix, x.Network, true)
		if err == kvdb.ErrDbDoesNotExist {
			// Only skip if it's an optional because it's not
			// required to run a wtclient or wtserver for example.
			if optionalDBs[prefix] {
				logger.Warnf("Skipping checking db version "+
					"of optional DB %s: not found", prefix)

				continue
			}
		}
		if err != nil {
			return fmt.Errorf("failed to open source db with "+
				"prefix `%s` for version check: %w", prefix,
				err)
		}

		switch prefix {
		case lncfg.NSChannelDB:
			err := checkChannelDBMigrationsApplied(srcDb)
			if err != nil {
				srcDb.Close()
				return err
			}

		case lncfg.NSTowerClientDB:
			err := checkWTClientDBMigrationsApplied(srcDb)
			if err != nil {
				srcDb.Close()
				return err
			}
		}

		srcDb.Close()
		logger.Infof("Version check passed for database %s", prefix)
	}

	for _, prefix := range allDBPrefixes {
		logger.Infof("Attempting to migrate DB with prefix `%s`", prefix)

		// Create a separate meta db for each db to store the
		// migration/verification state. This db will be deleted
		// after the migration is successful.
		metaDBPath := filepath.Join(
			x.Source.Bolt.DataDir, prefix+"-migration-meta.db",
		)

		srcDb, err := openSourceDb(
			x.Source, prefix, x.Network, true,
		)
		if err == kvdb.ErrDbDoesNotExist {
			// Only skip if it's an optional because it's not
			// required to run a wtclient or wtserver for example.
			if optionalDBs[prefix] {
				logger.Warnf("Skipping optional DB %s: not "+
					"found", prefix)
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("failed to open source db with "+
				"prefix `%s`: %w", prefix, err)
		}
		defer srcDb.Close()
		logger.Infof("Opened source DB with prefix `%s` successfully",
			prefix)

		// We open the destination DB as well to make sure both that
		// both DBs are either marked or not.
		destDb, err := openDestDb(ctx, x.Dest, prefix, x.Network)
		if err != nil {
			return fmt.Errorf("failed to open destination "+
				"db with prefix `%s`: %w", prefix, err)
		}
		defer destDb.Close()

		logger.Infof("Opened destination DB with prefix `%s` "+
			"successfully", prefix)

		// Check that the source database and the destination database
		// are either both marked with a tombstone or a migrated marker.
		logger.Infof("Checking tombstone marker on source DB and "+
			"migrated marker on destination DB with prefix `%s`",
			prefix)

		sourceMarker, err := checkMarkerPresent(
			srcDb, channeldb.TombstoneKey,
		)
		sourceDbTombstone := err == nil
		if err != nil && !errors.Is(err, channeldb.ErrMarkerNotPresent) {
			return err
		}

		// Also make sure that the destination DB hasn't been marked as
		// successfully having been the target of a migration. We only
		// mark a destination DB as successfully migrated at the end of
		// a successful and complete migration.
		destMarker, err := checkMarkerPresent(
			destDb, alreadyMigratedKey,
		)
		destDbMigrated := err == nil
		if err != nil && !errors.Is(err, channeldb.ErrMarkerNotPresent) {
			return err
		}
		switch {
		case sourceDbTombstone && destDbMigrated:
			if x.ForceVerifyDB {
				// Make sure the meta db is not deleted so we
				// can get the migration stats.
				// We delete the verification complete marker
				// only when this marker is set we clear the
				// verification state. then we should be good
				// to go.
				if !lnrpc.FileExists(metaDBPath) {
					return fmt.Errorf("cannot verify migration "+
						"for db with prefix `%s` because the "+
						"migration meta db does not exist",
						prefix)
				}

				// Open the db where we store the migration/verification state.
				metaDB, err := bbolt.Open(metaDBPath, 0600, nil)
				if err != nil {
					logger.Errorf("Error opening db: %v", err)
				}
				defer metaDB.Close()

				logger.Infof("Opened meta db at path: %s", metaDBPath)

				// Verify the migration.
				migrator, err := migratekvdb.New(migratekvdb.Config{
					Logger:       logger.SubSystem("MIGKV-" + prefix),
					ChunkSize:    x.ChunkSize,
					MetaDB:       metaDB,
					DBPrefixName: prefix,
				})
				if err != nil {
					return err
				}

				err = migrator.VerifyMigration(ctx, srcDb, destDb, true)
				if err != nil {
					return err
				}

				logger.Infof("Verification of migration of db with prefix "+
					"`%s` completed", prefix)

			}
			logger.Infof("Skipping DB with prefix `%s` because the "+
				"source DB is marked with a tombstone and the "+
				"destination DB is marked as already migrated. "+
				"Tag reads: source: `%s`, destination: `%s`",
				prefix, sourceMarker, destMarker)

			migratedDBs = append(migratedDBs, prefix)

			continue

		case sourceDbTombstone && !destDbMigrated:
			return fmt.Errorf("DB with prefix `%s` source DB is "+
				"marked with a tombstone but the "+
				"destination DB is not marked as already "+
				"migrated. This is not allowed. Tag reads: "+
				"source: `%s`, destination: `%s`",
				prefix, sourceMarker, destMarker)

		case !sourceDbTombstone && destDbMigrated:
			return fmt.Errorf("DB with prefix `%s` source DB is "+
				"not marked with a tombstone but the "+
				"destination DB is marked as already migrated. "+
				"This is not allowed. Tag reads: source: `%s`, "+
				"destination: `%s`",
				prefix, sourceMarker, destMarker)
		}

		// In case we want to start a new migration we delete the
		// migration meta db if it exits. This can only be done if
		// the db is not already successfully migrated otherwise
		// previous marker checks will prevent us to reach this point.
		if x.ForceNewMigration {
			// Before proceeding with the migration we check that
			// the destination db is empty.
			var topLevelBuckets [][]byte
			err := kvdb.View(destDb, func(tx kvdb.RTx) error {
				return tx.ForEachBucket(func(bucket []byte) error {
					bucketCopy := make([]byte, len(bucket))
					copy(bucketCopy, bucket)
					topLevelBuckets = append(
						topLevelBuckets, bucketCopy,
					)

					return nil
				})
			}, func() {})
			if err != nil {
				return err
			}

			if len(topLevelBuckets) > 0 {
				logger.Infof("Cannot force new migration "+
					"of db with prefix `%s` because the "+
					"destination db has data - delete "+
					"it manually first", prefix)

				return fmt.Errorf("destination db with prefix `%s` "+
					"has data, refusing to overwrite it", prefix)
			}

			logger.Info("Forcing new migration, deleting " +
				"migration meta db and all previous data from" +
				" the destination db")

			if lnrpc.FileExists(metaDBPath) {
				err := os.Remove(metaDBPath)
				if err != nil {
					return fmt.Errorf("failed to delete "+
						"migration meta db: %v", err)
				}

				logger.Infof("Deleted migration meta db at "+
					"path: %s", metaDBPath)
			}
		}

		// Open the db where we store the migration/verification state.
		metaDB, err := bbolt.Open(metaDBPath, 0600, nil)
		if err != nil {
			logger.Errorf("Error opening db: %v", err)
		}
		defer metaDB.Close()

		logger.Infof("Opened meta db at path: %s", metaDBPath)

		// Configure and run migration.
		cfg := migratekvdb.Config{
			Logger:       logger.SubSystem("MIGKV-" + prefix),
			ChunkSize:    x.ChunkSize,
			MetaDB:       metaDB,
			DBPrefixName: prefix,
		}

		migrator, err := migratekvdb.New(cfg)
		if err != nil {
			return err
		}

		err = migrator.Migrate(ctx, srcDb, destDb)
		if err != nil {
			return err
		}
		logger.Infof("Migration of db with prefix %s completed", prefix)

		// We migrated the DB successfully, now we verify the migration.
		err = migrator.VerifyMigration(
			ctx, srcDb, destDb, false,
		)
		if err != nil {
			return err
		}

		logger.Infof("Verification of migration of db with prefix "+
			"`%s` completed", prefix)

		// Migrate wallet created marker. This is done after the
		// migration to ensure the verification of the migration
		// succeeds.
		//
		// NOTE: We always need to add the wallet marker if the db is
		// not a `bolt` db, which is already resticted by the
		// destination db config.
		if prefix == lncfg.NSWalletDB {
			err := createWalletMarker(destDb, logger)
			if err != nil {
				return err
			}
		}

		// If we get here, we've successfully migrated the DB and can
		// now set the tombstone marker on the source database and the
		// already migrated marker on the target database.
		// We need to reopen the db in write mode.
		srcDb.Close()
		logger.Infof("We are now opening the source db with prefix `%s` "+
			"in write mode to set the tombstone marker. This "+
			"might take a while (~10 minutes for large databases) "+
			"to sync the freelist..., so please be patient it is the "+
			"final step for this db.", prefix)

		srcDb, err = openSourceDb(x.Source, prefix, x.Network, false)
		if err != nil {
			return err
		}

		if err := addMarker(srcDb, channeldb.TombstoneKey); err != nil {
			return err
		}

		// Add already migrated marker to the destination DB.
		if err := addMarker(destDb, alreadyMigratedKey); err != nil {
			return err
		}

		logger.Infof("Migration of DB with prefix `%s` completed "+
			"successfully", prefix)

		migratedDBs = append(migratedDBs, prefix)

		// Removing meta db.
		err = metaDB.Close()
		if err != nil {
			logger.Errorf("Error closing meta db: %v", err)
		}

		// Close the db connection to cleanup the state.
		err = srcDb.Close()
		if err != nil {
			logger.Errorf("Error closing source db: %v", err)
		}
		err = destDb.Close()
		if err != nil {
			logger.Errorf("Error closing destination db: %v", err)
		}

		// Create migration completed file, this will only create the
		// file for bolt databases.
		if err := createMigrationCompletedFile(x.Source, prefix,
			x.Network, x.Dest.Backend); err != nil {
			return err
		}
	}

	logger.Info("!!!Migration of all mandatory db parts completed " +
		"successfully!!!")

	logger.Infof("Migrated DBs: %v", migratedDBs)

	return nil
}

// validateDBBackends ensures that only migrations from bolt to sqlite/postgres
// are allowed.
func (x *migrateDBCommand) validateDBBackends() error {
	// Source must be bolt
	if x.Source.Backend != lncfg.BoltBackend {
		return fmt.Errorf("source database must be bolt, got: %s",
			x.Source.Backend)
	}

	// Destination must be sqlite or postgres.
	switch x.Dest.Backend {
	case lncfg.SqliteBackend, lncfg.PostgresBackend:
		return nil
	default:
		return fmt.Errorf("destination database must be sqlite or "+
			"postgres, got: %s", x.Dest.Backend)
	}
}

// openSourceDb opens the source database and also checks if there is enough
// free space on the source directory to hold a copy of the database.
func openSourceDb(cfg *SourceDB, prefix, network string,
	readonly bool) (kvdb.Backend, error) {

	path := getBoltDBPath(cfg, prefix, network)
	if path == "" {
		return nil, fmt.Errorf("unknown prefix: %s", prefix)
	}

	const (
		noFreelistSync = true
		timeout        = time.Minute
	)

	args := []interface{}{
		path, noFreelistSync, timeout, readonly,
	}
	backend := kvdb.BoltBackendName
	logger.Infof("Opening bolt backend at %s for prefix '%s'",
		path, prefix)

	db, err := kvdb.Open(backend, args...)
	if err != nil {
		return nil, err
	}

	// Get the size of the database.
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("error determining source database "+
			"with prefix %s: %v", prefix, err)
	}
	dbSize := fi.Size()

	// Because the destination can also just be a postgres dsn, we just
	// check if the source dir has enough free space to hold a copy of the
	// db.
	freeSpace, err := healthcheck.AvailableDiskSpace(cfg.Bolt.DataDir)
	if err != nil {
		return nil, fmt.Errorf("error determining source directory "+
			"free space: %v", err)
	}

	if freeSpace < uint64(dbSize) {
		return nil, fmt.Errorf("not enough free space on source "+
			"directory to migrate db: %d bytes required, "+
			"%d bytes available", dbSize, freeSpace)
	}

	logger.Debugf("Source DB size: %d bytes", dbSize)

	return db, nil
}

// openDestDb opens the different types of databases.
func openDestDb(ctx context.Context, cfg *DestDB, prefix,
	network string) (kvdb.Backend, error) {

	backend := cfg.Backend

	// Init the db connections for sql backends.
	err := cfg.Init()
	if err != nil {
		return nil, err
	}

	// Settings to open a particular db backend.
	var args []interface{}

	switch backend {
	case kvdb.PostgresBackendName:
		args = []interface{}{
			ctx,
			&postgres.Config{
				Dsn:            cfg.Postgres.Dsn,
				Timeout:        time.Minute,
				MaxConnections: 10,
			},
			prefix,
		}

		logger.Infof("Opening postgres backend at `%s` with prefix `%s`",
			cfg.Postgres.Dsn, prefix)

	case kvdb.SqliteBackendName:
		// Directories where the db files are located.
		graphDir := lncfg.CleanAndExpandPath(
			filepath.Join(cfg.Sqlite.DataDir, "graph", network),
		)
		walletDir := lncfg.CleanAndExpandPath(
			filepath.Join(
				cfg.Sqlite.DataDir, "chain", "bitcoin", network,
			),
		)

		// In case the data directory was set but the watchtower is
		// still the default one, we use the data directory for the
		// watchtower as well.
		towerServerDir := lncfg.CleanAndExpandPath(
			filepath.Join(
				cfg.Sqlite.TowerDir, "watchtower", "bitcoin",
				network,
			),
		)
		if cfg.Sqlite.DataDir != defaultDataDir &&
			cfg.Sqlite.TowerDir == defaultDataDir {

			towerServerDir = lncfg.CleanAndExpandPath(
				filepath.Join(
					cfg.Sqlite.DataDir, "watchtower",
					"bitcoin", network,
				),
			)
		}

		var dbName string
		var path string
		switch prefix {
		case lncfg.NSChannelDB:
			path = graphDir
			dbName = lncfg.SqliteChannelDBName

		case lncfg.NSWalletDB:
			path = walletDir
			dbName = lncfg.SqliteChainDBName

		case lncfg.NSMacaroonDB:
			path = walletDir
			dbName = lncfg.SqliteChainDBName

		case lncfg.NSDecayedLogDB:
			path = graphDir
			dbName = lncfg.SqliteChannelDBName

		case lncfg.NSTowerClientDB:
			path = graphDir
			dbName = lncfg.SqliteChannelDBName

		case lncfg.NSTowerServerDB:
			path = towerServerDir
			dbName = lncfg.SqliteChannelDBName

		case lncfg.NSNeutrinoDB:
			path = walletDir
			dbName = lncfg.SqliteNeutrinoDBName
		}

		// We check if the path exists to avoid receiving sqlite
		// misleading errors. Because sqlite will report out of
		// memory issues if the path does not exist.
		if err := checkPathExists(path); err != nil {
			return nil, fmt.Errorf("destination directory (%s) "+
				"not found: %v", path, err)
		}

		args = []interface{}{
			ctx,
			&sqlite.Config{
				Timeout: time.Minute,
			},
			path,
			dbName,
			prefix,
		}

		logger.Infof("Opening sqlite backend at %s  "+
			"for prefix '%s'", filepath.Join(path, dbName),
			prefix)

	default:
		return nil, fmt.Errorf("unknown backend: %v", backend)
	}

	return kvdb.Open(backend, args...)
}

// checkMarkerPresent checks if a marker is present in the database.
func checkMarkerPresent(db kvdb.Backend, markerKey []byte) ([]byte, error) {
	var (
		markerValue []byte
		err         error
	)
	err = kvdb.View(db, func(tx kvdb.RTx) error {
		markerValue, err = channeldb.CheckMarkerPresent(tx, markerKey)
		return err
	}, func() {})
	if err != nil {
		return nil, err
	}

	return markerValue, nil
}

// addMarker adds a marker to the database.
func addMarker(db kvdb.Backend, markerKey []byte) error {
	err := kvdb.Update(db, func(tx kvdb.RwTx) error {
		markerValue := []byte(
			fmt.Sprintf("lndinit migrate-db %s", time.Now().
				Format(time.RFC3339)),
		)

		return channeldb.AddMarker(tx, markerKey, markerValue)
	}, func() {})
	if err != nil {
		return err
	}

	return nil
}

// createWalletMarker creates a marker in the wallet database to indicate it's
// ready for use. This is only needed for non-bolt databases.
func createWalletMarker(db kvdb.Backend, logger btclog.Logger) error {
	logger.Info("Creating 'wallet created' marker")

	err := kvdb.Update(db, func(tx kvdb.RwTx) error {
		metaBucket, err := tx.CreateTopLevelBucket(
			[]byte(walletMetaBucket),
		)
		if err != nil {
			return fmt.Errorf("failed to create meta "+
				"bucket: %w", err)
		}

		return metaBucket.Put(
			[]byte(walletReadyKey), []byte(walletReadyKey),
		)
	}, func() {})
	if err != nil {
		return fmt.Errorf("failed to create wallet marker: %w", err)
	}

	logger.Info("Successfully created 'wallet created' marker")

	return nil
}

// checkChannelDBMigrationsApplied checks if the channel DB migrations are
// applied.
func checkChannelDBMigrationsApplied(db kvdb.Backend) error {
	var meta channeldb.Meta
	err := kvdb.View(db, func(tx kvdb.RTx) error {
		return channeldb.FetchMeta(&meta, tx)
	}, func() {
		meta = channeldb.Meta{}
	})
	if err != nil {
		return err
	}

	if meta.DbVersionNumber != channeldb.LatestDBVersion() {
		return fmt.Errorf("refusing to migrate source database with "+
			"version %d while latest known DB version is %d; "+
			"please upgrade the DB before using the data "+
			"migration tool", meta.DbVersionNumber,
			channeldb.LatestDBVersion())
	}

	return nil
}

// checkWTClientDBMigrationsApplied checks if the watchtower client DB
// migrations are applied.
func checkWTClientDBMigrationsApplied(db kvdb.Backend) error {
	version, err := wtdb.CurrentDatabaseVersion(db)
	if err != nil {
		return err
	}

	if version != wtdb.LatestDBMigrationVersion() {
		return fmt.Errorf("refusing to migrate source database with "+
			"version %d while latest known DB version is %d; "+
			"please upgrade the DB before using the data "+
			"migration tool or delete the wtclient.db manually "+
			"in case you don't plan to use the watchtower client "+
			"anymore", version, wtdb.LatestDBMigrationVersion())
	}

	return nil
}

// getBoltDBPath returns the full path for a given database type and prefix.
func getBoltDBPath(cfg *SourceDB, prefix, network string) string {
	// Directories where the db files are located.
	graphDir := lncfg.CleanAndExpandPath(
		filepath.Join(cfg.Bolt.DataDir, "graph", network),
	)
	walletDir := lncfg.CleanAndExpandPath(
		filepath.Join(
			cfg.Bolt.DataDir, "chain", "bitcoin", network,
		),
	)

	towerServerDir := lncfg.CleanAndExpandPath(
		filepath.Join(
			cfg.Bolt.TowerDir, "watchtower", "bitcoin",
			network,
		),
	)
	if cfg.Bolt.DataDir != defaultDataDir &&
		cfg.Bolt.TowerDir == defaultDataDir {

		towerServerDir = lncfg.CleanAndExpandPath(
			filepath.Join(
				cfg.Bolt.DataDir, "watchtower",
				"bitcoin", network,
			),
		)
	}

	switch prefix {
	case lncfg.NSChannelDB:
		return filepath.Join(graphDir, lncfg.ChannelDBName)

	case lncfg.NSWalletDB:
		return filepath.Join(walletDir, lncfg.WalletDBName)

	case lncfg.NSMacaroonDB:
		return filepath.Join(walletDir, lncfg.MacaroonDBName)

	case lncfg.NSDecayedLogDB:
		return filepath.Join(graphDir, lncfg.DecayedLogDbName)

	case lncfg.NSTowerClientDB:
		return filepath.Join(graphDir, lncfg.TowerClientDBName)

	case lncfg.NSTowerServerDB:
		return filepath.Join(towerServerDir, lncfg.TowerServerDBName)

	case lncfg.NSNeutrinoDB:
		// TODO(ziggie): Can be updated as soon as new LND vesion is
		// available.
		return filepath.Join(walletDir, "neutrino.db")
	}

	return ""
}

// createMigrationCompletedFile creates an empty file indicating that a bolt
// database was successfully migrated to a different backend. This is only
// created when migrating FROM a bolt database TO another backend type.
func createMigrationCompletedFile(sourceDB *SourceDB, prefix,
	network, targetType string) error {

	// Only create completion file when migrating FROM bolt.
	if sourceDB.Backend != lncfg.BoltBackend {
		return nil
	}

	dbPath := getBoltDBPath(sourceDB, prefix, network)
	dir := filepath.Dir(dbPath)
	dbName := filepath.Base(dbPath)

	timestamp := time.Now().Format("2006-01-02-15-04")
	markerName := fmt.Sprintf(
		"%s.migrated-to-%s-%s", dbName, targetType, timestamp,
	)
	markerPath := filepath.Join(dir, markerName)

	f, err := os.Create(markerPath)
	if err != nil {
		return fmt.Errorf("failed to create migration completed "+
			"file at %s: %w", markerPath, err)
	}
	defer f.Close()

	logger.Infof("Created migration completed file at %s", markerPath)

	return nil
}

// checkPathExists verifies that the directory exists.
func checkPathExists(path string) error {
	dir := filepath.Dir(path)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist, "+
			"please create it first", dir)
	} else if err != nil {
		return fmt.Errorf("failed to check directory %s: %v", dir, err)
	}

	return nil
}

func getContext() context.Context {
	// Hook interceptor for os signals. We need to except the case where
	// we call the function multiple times.
	shutdownInterceptor, err := signal.Intercept()
	shutdownInterceptor.ShutdownChannel()

	// TODO(ziggie): This is a hack to avoid an error when running the
	// migration tests for sqlite and postgres, because both are using the
	// same main routine.
	if err != nil && !strings.Contains(err.Error(), "intercept "+
		"already started") {

		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctxc, cancel := context.WithCancel(context.Background())
	go func() {
		<-shutdownInterceptor.ShutdownChannel()
		cancel()
	}()
	return ctxc
}
