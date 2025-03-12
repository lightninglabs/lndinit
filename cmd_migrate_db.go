package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btclog/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/jessevdk/go-flags"
	"github.com/lightninglabs/lndinit/migratekvdb"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/kvdb/sqlbase"
	"github.com/lightningnetwork/lnd/kvdb/sqlite"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/watchtower/wtdb"
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

// DB is the parent configuration for all database types.
type DB struct {
	Backend  string           `long:"backend" description:"The selected database backend." choice:"bolt" choice:"postgres" choice:"sqlite"`
	Bolt     *Bolt            `group:"bolt" namespace:"bolt" description:"Bolt settings."`
	Postgres *postgres.Config `group:"postgres" namespace:"postgres" description:"Postgres settings."`
	Sqlite   *Sqlite          `group:"sqlite" namespace:"sqlite" description:"Sqlite settings."`
}

// Init should be called upon start to pre-initialize database for sql
// backends. If max connections are not set, the amount of connections will be
// unlimited however we only use one connection during the migration.
func (db *DB) Init() error {
	switch {
	case db.Backend == lncfg.PostgresBackend:
		sqlbase.Init(db.Postgres.MaxConnections)

	case db.Backend == lncfg.SqliteBackend:
		sqlbase.Init(db.Sqlite.Config.MaxConnections)
	}

	return nil
}

// isBoltDB returns true if the db is a type bolt db.
func (db *DB) isBoltDB() bool {
	return db.Backend == lncfg.BoltBackend
}

type migrateDBCommand struct {
	Source            *DB    `group:"source" namespace:"source" long:"" short:"" description:""`
	Dest              *DB    `group:"dest" namespace:"dest" long:"" short:"" description:""`
	Network           string `long:"network" short:"n" description:"Network of the db files to migrate (used to navigate into the right directory)"`
	PprofPort         int    `long:"pprof-port" description:"Enable pprof profiling on the specified port"`
	ForceNewMigration bool   `long:"force-new-migration" description:"Force a new migration from the beginning of the source DB so the resume state will be discarded"`
	ChunkSize         uint64 `long:"chunk-size" description:"Chunk size for the migration in bytes"`
}

func newMigrateDBCommand() *migrateDBCommand {
	return &migrateDBCommand{
		Source: &DB{
			Backend: lncfg.BoltBackend,
			Bolt: &Bolt{
				DBTimeout: kvdb.DefaultDBTimeout,
				TowerDir:  defaultDataDir,
				DataDir:   defaultDataDir,
			},
			Postgres: &postgres.Config{},
			Sqlite: &Sqlite{
				Config:   &sqlite.Config{},
				TowerDir: defaultDataDir,
				DataDir:  defaultDataDir,
			},
		},
		Dest: &DB{
			Backend: lncfg.PostgresBackend,
			Bolt: &Bolt{
				DBTimeout: kvdb.DefaultDBTimeout,
				TowerDir:  defaultDataDir,
				DataDir:   defaultDataDir,
			},
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

func (x *migrateDBCommand) Execute(_ []string) error {
	// TODO(ziggie): Add interceptor to cleanly shutdown the migration
	// if it's interrupted.

	// We currently only allow migrations from bolt to sqlite/postgres.
	if err := x.validateDBBackends(); err != nil {
		return fmt.Errorf("invalid database configuration: %w", err)
	}

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

	// optionalDBs are the databases that can be skipped if they don't
	// exist.
	var optionalDBs = map[string]bool{
		lncfg.NSTowerClientDB: true,
		lncfg.NSTowerServerDB: true,
	}

	// allDBPrefixes defines all databases that should be migrated.
	var allDBPrefixes = []string{
		lncfg.NSChannelDB,
		lncfg.NSMacaroonDB,
		lncfg.NSDecayedLogDB,
		lncfg.NSTowerClientDB,
		lncfg.NSTowerServerDB,
		lncfg.NSWalletDB,
	}

	for _, prefix := range allDBPrefixes {
		logger.Infof("Attempting to migrate DB with prefix %s", prefix)

		srcDb, err := openDb(x.Source, prefix, x.Network)
		if err == walletdb.ErrDbDoesNotExist {
			// Only skip if it's an optional because it's not
			// required to run a wtclient or wtserver.
			if optionalDBs[prefix] {
				logger.Warnf("Skipping optional DB %s: not "+
					"found", prefix)
				continue
			}

			return fmt.Errorf("required database %s not found",
				prefix)
		}
		if err != nil {
			return fmt.Errorf("failed to open source db %s: %w",
				prefix, err)
		}
		logger.Info("Opened source DB")

		destDb, err := openDb(x.Dest, prefix, x.Network)
		if err != nil {
			return fmt.Errorf("failed to open destination "+
				"db %s: %w", prefix, err)
		}
		logger.Info("Opened destination DB")

		// Check that the source database hasn't been marked with a
		// tombstone yet. Once we set the tombstone we see the DB as not
		// viable for migration anymore to avoid old state overwriting
		// new state. We only set the tombstone at the end of a
		// successful and complete migration.
		logger.Info("Checking tombstone marker on source DB")
		marker, err := checkMarkerPresent(srcDb, channeldb.TombstoneKey)
		if err == nil {
			logger.Infof("Skipping DB with prefix %s because the "+
				"source DB was marked with a tombstone which "+
				"means it was already migrated successfully. "+
				"Tombstone reads: %s", prefix, marker)

			continue
		}
		if err != channeldb.ErrMarkerNotPresent {
			return err
		}

		// Check that the source DB has had all its schema migrations
		// applied before we migrate any of its data. Currently only
		// migration of the channel.db and the watchtower.db exist.
		//
		// TODO(ziggie): Use the tagged LND 19 version here.

		// Check channel.db migration.
		if prefix == lncfg.NSChannelDB {
			logger.Info("Checking DB version of source DB " +
				"(channel.db)")

			err := checkChannelDBMigrationsApplied(srcDb)
			if err != nil {
				return err
			}
		}

		// Check watchtower client DB migrations.
		if prefix == lncfg.NSTowerClientDB {
			logger.Info("Checking DB version of source DB " +
				"(wtclient.db)")

			err := checkWTClientDBMigrationsApplied(srcDb)
			if err != nil {
				return err
			}
		}

		// Also make sure that the destination DB hasn't been marked as
		// successfully having been the target of a migration. We only
		// mark a destination DB as successfully migrated at the end of
		// a successful and complete migration.
		logger.Info("Checking if migration was already applied to " +
			"destination DB")
		marker, err = checkMarkerPresent(destDb, alreadyMigratedKey)
		if err == nil {
			logger.Infof("Skipping DB with prefix %s because the "+
				"destination DB was marked as already having "+
				"been the target of a successful migration. "+
				"Tag reads: %s", prefix, marker)

			continue
		}
		if err != channeldb.ErrMarkerNotPresent {
			return err
		}

		// Configure and run migration.
		cfg := migratekvdb.Config{
			Logger:            logger.SubSystem("MIGKV"),
			ForceNewMigration: x.ForceNewMigration,
			ChunkSize:         x.ChunkSize,
		}

		migrator, err := migratekvdb.New(cfg)
		if err != nil {
			return err
		}

		err = migrator.Migrate(context.Background(), srcDb, destDb)
		if err != nil {
			return err
		}
		logger.Info("Migration completed")

		// We migrated the DB successfully, now we verify the migration.
		err = migrator.VerifyMigration(context.Background(), destDb)
		if err != nil {
			return err
		}

		logger.Info("Verification completed")

		// Migrate wallet created marker. This is done after the
		// migration to ensure the verification of the migration
		// succeeds.
		if prefix == lncfg.NSWalletDB && !x.Dest.isBoltDB() {
			err := createWalletMarker(destDb, logger)
			if err != nil {
				return err
			}
		}

		// If we get here, we've successfully migrated the DB and can
		// now set the tombstone marker on the source database and the
		// already migrated marker on the target database.
		if err := addMarker(srcDb, channeldb.TombstoneKey); err != nil {
			return err
		}
		if err := addMarker(destDb, alreadyMigratedKey); err != nil {
			return err
		}

		logger.Infof("Migration of DB with prefix %s completed "+
			"successfully", prefix)

		// Close the db connection to cleanup the state.
		srcDb.Close()
		destDb.Close()

	}

	logger.Info("!!!Migration of all mandatory db parts completed " +
		"successfully!!!")

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

	// Destination must be sqlite or postgres
	switch x.Dest.Backend {
	case lncfg.SqliteBackend, lncfg.PostgresBackend:
		return nil
	default:
		return fmt.Errorf("destination database must be sqlite or "+
			"postgres, got: %s", x.Dest.Backend)
	}
}

// openDb opens the different types of databases.
func openDb(cfg *DB, prefix, network string) (walletdb.DB, error) {
	backend := cfg.Backend

	// Init the db connections for sql backends.
	err := cfg.Init()
	if err != nil {
		return nil, err
	}

	// Settings to open a particular db backend.
	var args []interface{}

	switch backend {
	case lncfg.BoltBackend:
		// Directories where the db files are located.
		graphDir := lncfg.CleanAndExpandPath(
			filepath.Join(cfg.Bolt.DataDir, "graph", network),
		)
		walletDir := lncfg.CleanAndExpandPath(
			filepath.Join(
				cfg.Bolt.DataDir, "chain", "bitcoin", network,
			),
		)

		// In case the data directory was set but the watchtower is
		// still the default one, we use the `data` directory for the
		// watchtower as well.
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

		// Path to the db file.
		var path string
		switch prefix {
		case lncfg.NSChannelDB:
			path = filepath.Join(graphDir, lncfg.ChannelDBName)

		case lncfg.NSMacaroonDB:
			path = filepath.Join(walletDir, lncfg.MacaroonDBName)

		case lncfg.NSDecayedLogDB:
			path = filepath.Join(graphDir, lncfg.DecayedLogDbName)

		case lncfg.NSTowerClientDB:
			path = filepath.Join(graphDir, lncfg.TowerClientDBName)

		case lncfg.NSTowerServerDB:
			path = filepath.Join(
				towerServerDir, lncfg.TowerServerDBName,
			)

		case lncfg.NSWalletDB:
			path = filepath.Join(walletDir, lncfg.WalletDBName)
		}

		const (
			noFreelistSync = true
			timeout        = time.Minute
		)

		args = []interface{}{
			path, noFreelistSync, timeout,
		}
		backend = kvdb.BoltBackendName
		logger.Infof("Opening bolt backend at %s for prefix '%s'",
			path, prefix)

	case kvdb.PostgresBackendName:
		args = []interface{}{
			context.Background(),
			&postgres.Config{
				Dsn:            cfg.Postgres.Dsn,
				Timeout:        time.Minute,
				MaxConnections: 10,
			},
			prefix,
		}

		logger.Infof("Opening postgres backend at %s with prefix '%s'",
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

		case lncfg.NSWalletDB:
			path = walletDir
			dbName = lncfg.SqliteChainDBName

		case lncfg.NSNeutrinoDB:
			dbName = lncfg.SqliteNeutrinoDBName
		}

		args = []interface{}{
			context.Background(),
			&sqlite.Config{
				Timeout: time.Minute,
			},
			path,
			dbName,
			prefix,
		}

		logger.Infof("Opening sqlite backend with prefix '%s'", prefix)

	default:
		return nil, fmt.Errorf("unknown backend: %v", backend)
	}

	return kvdb.Open(backend, args...)
}

// checkMarkerPresent checks if a marker is present in the database.
func checkMarkerPresent(db walletdb.DB, markerKey []byte) ([]byte, error) {
	rtx, err := db.BeginReadTx()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rtx.Rollback() }()

	return channeldb.CheckMarkerPresent(rtx, markerKey)
}

// addMarker adds a marker to the database.
func addMarker(db walletdb.DB, markerKey []byte) error {
	rwtx, err := db.BeginReadWriteTx()
	if err != nil {
		logger.Errorf("Failed to begin read write transaction: %v", err)
		return err
	}

	markerValue := []byte(fmt.Sprintf("lndinit migrate-db %s", time.Now()))
	if err := channeldb.AddMarker(rwtx, markerKey, markerValue); err != nil {
		return err
	}

	return rwtx.Commit()
}

// createWalletMarker creates a marker in the wallet database to indicate it's
// ready for use. This is only needed for non-bolt databases.
func createWalletMarker(db walletdb.DB, logger btclog.Logger) error {
	logger.Info("Creating 'wallet created' marker")

	tx, err := db.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	metaBucket, err := tx.CreateTopLevelBucket([]byte(walletMetaBucket))
	if err != nil {
		return fmt.Errorf("failed to create meta bucket: %w", err)
	}

	err = metaBucket.Put([]byte(walletReadyKey), []byte(walletReadyKey))
	if err != nil {
		return fmt.Errorf("failed to put wallet ready marker: %w", err)
	}

	logger.Info("Committing 'wallet created' marker")
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing 'wallet created' "+
			"marker: %w", err)
	}

	return nil
}

// checkChannelDBMigrationsApplied checks if the channel DB migrations are
// applied.
func checkChannelDBMigrationsApplied(db walletdb.DB) error {
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
func checkWTClientDBMigrationsApplied(db walletdb.DB) error {
	version, err := wtdb.CurrentDatabaseVersion(db)
	if err != nil {
		return err
	}

	if version != wtdb.LatestDBMigrationVersion() {
		return fmt.Errorf("refusing to migrate source database with "+
			"version %d while latest known DB version is %d; "+
			"please upgrade the DB before using the data "+
			"migration tool", version, wtdb.LatestDBMigrationVersion())
	}

	return nil
}
