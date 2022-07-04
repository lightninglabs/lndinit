package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/jessevdk/go-flags"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/etcd"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/signal"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// alreadyMigratedKey is the key under which we add a tag in the target/
	// destination DB after we've successfully and completely migrated it
	// from a source DB.
	alreadyMigratedKey = []byte("data-migration-already-migrated")

	// etcdTimeout is the time we allow a single etcd transaction to take.
	etcdTimeout = time.Second * 5
)

const (
	// EtcdMigrationMaxCallSize is the maximum size in bytes we allow a TX
	// message for etcd to be. This must be large enough to accommodate the
	// largest single value we ever expect to be in one of our databases.
	EtcdMigrationMaxCallSize = 100 * 1024 * 1024
)

type Bolt struct {
	DBTimeout time.Duration `long:"dbtimeout" description:"Specify the timeout value used when opening the database."`
	DataDir   string        `long:"data-dir" description:"Lnd data dir where bolt dbs are located."`
	TowerDir  string        `long:"tower-dir" description:"Lnd watchtower dir where bolt dbs for the watchtower server are located."`
	Network   string        `long:"network" description:"Network within data dir where bolt dbs are located."`
}

type DB struct {
	Backend  string           `long:"backend" description:"The selected database backend."`
	Etcd     *etcd.Config     `group:"etcd" namespace:"etcd" description:"Etcd settings."`
	Bolt     *Bolt            `group:"bolt" namespace:"bolt" description:"Bolt settings."`
	Postgres *postgres.Config `group:"postgres" namespace:"postgres" description:"Postgres settings."`
}

func (d *DB) isRemote() bool {
	return d.Backend == lncfg.EtcdBackend ||
		d.Backend == lncfg.PostgresBackend
}

func (d *DB) isEtcd() bool {
	return d.Backend == lncfg.EtcdBackend
}

type migrateDBCommand struct {
	Source *DB `group:"source" namespace:"source" long:"source" short:"s" description:"The source database where the data is read from"`
	Dest   *DB `group:"dest" namespace:"dest" long:"dest" short:"d" description:"The destination database where the data is written to"`
}

func newMigrateDBCommand() *migrateDBCommand {
	return &migrateDBCommand{
		Source: &DB{
			Backend: lncfg.BoltBackend,
			Etcd:    &etcd.Config{},
			Bolt: &Bolt{
				DBTimeout: kvdb.DefaultDBTimeout,
				Network:   "mainnet",
			},
			Postgres: &postgres.Config{},
		},
		Dest: &DB{
			Backend: lncfg.EtcdBackend,
			Etcd: &etcd.Config{
				MaxMsgSize: EtcdMigrationMaxCallSize,
			},
			Bolt: &Bolt{
				DBTimeout: kvdb.DefaultDBTimeout,
				Network:   "mainnet",
			},
			Postgres: &postgres.Config{},
		},
	}
}

func (x *migrateDBCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"migrate-db",
		"Migrate the complete database state of lnd to a new backend",
		`
	Migrate the full database state of lnd from a source (for example the
	set of bbolt database files such as channel.db and wallet.db) database
	to a destination (for example a remote etcd or postgres) database.

	IMPORTANT: Please read the data migration guide located	in the file
	docs/data-migration.md of the main lnd repository before using this
	command!

	NOTE: The migration can take a long time depending on the amount of data
	that needs to be written! Because of the number of operations that need
	to be completed, the migration cannot occur in a single database
	transaction. Therefore the migration is not 100% atomic but happens
	bucket by bucket.
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
	twice, which is checked through the 'already migrated' tag.
		`,
		x,
	)
	return err
}

func (x *migrateDBCommand) Execute(_ []string) error {
	// Since this will potentially run for a while, make sure we catch any
	// interrupt signals.
	_, err := signal.Intercept()
	if err != nil {
		return fmt.Errorf("error intercepting signals: %v", err)
	}

	var prefixes = []string{
		lncfg.NSChannelDB, lncfg.NSMacaroonDB, lncfg.NSDecayedLogDB,
		lncfg.NSTowerClientDB, lncfg.NSTowerServerDB, lncfg.NSWalletDB,
	}

	for _, prefix := range prefixes {
		log("Migrating DB with prefix %s", prefix)

		srcDb, err := openDb(x.Source, prefix)
		if err != nil {
			if err == walletdb.ErrDbDoesNotExist &&
				x.Source.Backend == lncfg.BoltBackend {

				log("Skipping DB with prefix %s because "+
					"source does not exist", prefix)
				continue
			}
			return err
		}
		log("Opened source DB")

		destDb, err := openDb(x.Dest, prefix)
		if err != nil {
			return err
		}
		log("Opened destination DB")

		// Check that the source database hasn't been marked with a
		// tombstone yet. Once we set the tombstone we see the DB as not
		// viable for migration anymore to avoid old state overwriting
		// new state. We only set the tombstone at the end of a
		// successful and complete migration.
		log("Checking tombstone marker on source DB")
		marker, err := checkMarkerPresent(srcDb, channeldb.TombstoneKey)
		if err == nil {
			log("Skipping DB with prefix %s because the source "+
				"DB was marked with a tombstone which "+
				"means it was already migrated successfully. "+
				"Tombstone reads: %s", prefix, marker)
			continue
		}
		if err != channeldb.ErrMarkerNotPresent {
			return err
		}

		// Check that the source DB has had all its schema migrations
		// applied before we migrate any of its data. This only applies
		// to the channel DB as that is the only DB that has migrations.
		log("Checking DB version of source DB")
		if prefix == lncfg.NSChannelDB {
			if err := checkMigrationsApplied(srcDb); err != nil {
				return err
			}
		}

		// Also make sure that the destination DB hasn't been marked as
		// successfully having been the target of a migration. We only
		// mark a destination DB as successfully migrated at the end of
		// a successful and complete migration.
		log("Checking if migration was already applied to target DB")
		marker, err = checkMarkerPresent(destDb, alreadyMigratedKey)
		if err == nil {
			log("Skipping DB with prefix %s because the "+
				"destination DB was marked as already having "+
				"been the target of a successful migration. "+
				"Tag reads: %s", prefix, marker)
			continue
		}
		if err != channeldb.ErrMarkerNotPresent {
			return err
		}

		// Using ReadWrite otherwise there is no access to the sequence
		// number.
		srcTx, err := srcDb.BeginReadWriteTx()
		if err != nil {
			return err
		}

		if x.Dest.isEtcd() {
			log("Starting the migration to the etcd backend")
			err := x.migrateEtcd(srcTx, prefix)
			if err != nil {
				return err
			}
		} else {
			log("Starting the migration to the target backend")
			err := x.migrateKvdb(srcTx, destDb, prefix)
			if err != nil {
				return err
			}
		}

		// We're done now, so we can roll back the read transaction of
		// the source DB.
		if err := srcTx.Rollback(); err != nil {
			return fmt.Errorf("error rolling back source tx: %v",
				err)
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
	}

	return nil
}

func (x *migrateDBCommand) migrateKvdb(srcTx walletdb.ReadWriteTx,
	destDb walletdb.DB, prefix string) error {

	err := srcTx.ForEachBucket(func(key []byte) error {
		log("Copying top-level bucket '%s'", loggableKeyName(key))

		destTx, err := destDb.BeginReadWriteTx()
		if err != nil {
			return err
		}

		destBucket, err := destTx.CreateTopLevelBucket(key)
		if err != nil {
			return fmt.Errorf("error creating top level bucket "+
				"'%s': %v", loggableKeyName(key), err)
		}

		srcBucket := srcTx.ReadWriteBucket(key)
		err = copyBucketKvdb(srcBucket, destBucket)
		if err != nil {
			return fmt.Errorf("error copying bucket '%s': %v",
				loggableKeyName(key), err)
		}

		log("Committing bucket '%s'", loggableKeyName(key))
		if err := destTx.Commit(); err != nil {
			return fmt.Errorf("error committing bucket '%s': %v",
				loggableKeyName(key), err)
		}

		return nil

	})
	if err != nil {
		return fmt.Errorf("error enumerating top level buckets: %v",
			err)
	}

	// Migrate wallet created marker.
	if prefix == lncfg.NSWalletDB && x.Dest.isRemote() {
		const (
			walletMetaBucket = "lnwallet"
			walletReadyKey   = "ready"
		)

		log("Creating 'wallet created' marker")
		destTx, err := destDb.BeginReadWriteTx()
		if err != nil {
			return err
		}

		metaBucket, err := destTx.CreateTopLevelBucket(
			[]byte(walletMetaBucket),
		)
		if err != nil {
			return err
		}

		err = metaBucket.Put(
			[]byte(walletReadyKey), []byte(walletReadyKey),
		)
		if err != nil {
			return err
		}

		log("Committing 'wallet created' marker")
		if err := destTx.Commit(); err != nil {
			return fmt.Errorf("error committing 'wallet created' "+
				"marker: %v", err)
		}
	}

	return nil
}

func copyBucketKvdb(src walletdb.ReadWriteBucket,
	dest walletdb.ReadWriteBucket) error {

	if err := dest.SetSequence(src.Sequence()); err != nil {
		return fmt.Errorf("error copying sequence number")
	}

	return src.ForEach(func(k, v []byte) error {
		if v == nil {
			srcBucket := src.NestedReadWriteBucket(k)
			destBucket, err := dest.CreateBucket(k)
			if err != nil {
				return fmt.Errorf("error creating bucket "+
					"'%s': %v", loggableKeyName(k), err)
			}

			if err := copyBucketKvdb(srcBucket, destBucket); err != nil {
				return fmt.Errorf("error copying bucket "+
					"'%s': %v", loggableKeyName(k), err)
			}

			return nil
		}

		err := dest.Put(k, v)
		if err != nil {
			return fmt.Errorf("error copying key '%s': %v",
				loggableKeyName(k), err)
		}

		return nil
	})
}

func (x *migrateDBCommand) migrateEtcd(srcTx walletdb.ReadWriteTx,
	prefix string) error {

	ctx := context.Background()
	cfg := x.Dest.Etcd.CloneWithSubNamespace(prefix)
	destDb, ctx, cancel, err := etcd.NewEtcdClient(ctx, *cfg)
	if err != nil {
		return err
	}
	defer cancel()

	err = srcTx.ForEachBucket(func(key []byte) error {
		log("Copying top-level bucket '%s'", loggableKeyName(key))

		return migrateBucketEtcd(
			ctx, destDb, []string{string(key)},
			srcTx.ReadWriteBucket(key),
		)
	})
	if err != nil {
		return err
	}

	return nil
}

func openDb(cfg *DB, prefix string) (walletdb.DB, error) {
	backend := cfg.Backend

	var args []interface{}

	graphDir := filepath.Join(cfg.Bolt.DataDir, "graph", cfg.Bolt.Network)
	walletDir := filepath.Join(
		cfg.Bolt.DataDir, "chain", "bitcoin", cfg.Bolt.Network,
	)
	towerServerDir := filepath.Join(
		cfg.Bolt.TowerDir, "bitcoin", cfg.Bolt.Network,
	)

	switch backend {
	case lncfg.BoltBackend:
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
		log("Opening bbolt backend at %s for prefix '%s'", path, prefix)

	case kvdb.EtcdBackendName:
		args = []interface{}{
			context.Background(),
			cfg.Etcd.CloneWithSubNamespace(prefix),
		}
		log("Opening etcd backend at %s with namespace '%s'",
			cfg.Etcd.Host, prefix)

	case kvdb.PostgresBackendName:
		args = []interface{}{
			context.Background(),
			&postgres.Config{
				Dsn: cfg.Postgres.Dsn,
			},
			prefix,
		}
		log("Opening postgres backend at %s with prefix '%s'",
			cfg.Postgres.Dsn, prefix)

	default:
		return nil, fmt.Errorf("unknown backend: %v", backend)
	}

	return kvdb.Open(backend, args...)
}

func putKeyValueEtcd(ctx context.Context, cli *clientv3.Client, key,
	value string) error {

	ctx, cancel := context.WithTimeout(ctx, etcdTimeout)
	defer cancel()

	_, err := cli.Put(ctx, key, value)
	return err
}

func migrateBucketEtcd(ctx context.Context, cli *clientv3.Client, path []string,
	bucket walletdb.ReadWriteBucket) error {

	err := putKeyValueEtcd(
		ctx, cli, etcd.BucketKey(path...), etcd.BucketVal(path...),
	)
	if err != nil {
		return err
	}

	var children []string
	err = bucket.ForEach(func(k, v []byte) error {
		key := string(k)
		if v != nil {
			err := putKeyValueEtcd(
				ctx, cli, etcd.ValueKey(key, path...),
				string(v),
			)
			if err != nil {
				return err
			}
		} else {
			children = append(children, key)
		}

		return nil
	})
	if err != nil {
		return err
	}

	seq := bucket.Sequence()
	if seq != 0 {
		// Store the number as a string.
		err := putKeyValueEtcd(
			ctx, cli, etcd.SequenceKey(path...),
			strconv.FormatUint(seq, 10),
		)
		if err != nil {
			return err
		}
	}

	for _, child := range children {
		childPath := append(path, child)
		err := migrateBucketEtcd(
			ctx, cli, childPath,
			bucket.NestedReadWriteBucket([]byte(child)),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkMarkerPresent(db walletdb.DB, markerKey []byte) ([]byte, error) {
	rtx, err := db.BeginReadTx()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rtx.Rollback() }()

	return channeldb.CheckMarkerPresent(rtx, markerKey)
}

func addMarker(db walletdb.DB, markerKey []byte) error {
	rwtx, err := db.BeginReadWriteTx()
	if err != nil {
		return err
	}

	markerValue := []byte(fmt.Sprintf("lndinit migrate-db %s", time.Now()))
	if err := channeldb.AddMarker(rwtx, markerKey, markerValue); err != nil {
		return err
	}

	return rwtx.Commit()
}

func checkMigrationsApplied(db walletdb.DB) error {
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

// loggableKeyName returns a printable name of the given key.
func loggableKeyName(key []byte) string {
	strKey := string(key)
	if hasSpecialChars(strKey) {
		return hex.EncodeToString(key)
	}

	return strKey
}

// hasSpecialChars returns true if any of the characters in the given string
// cannot be printed.
func hasSpecialChars(s string) bool {
	for _, b := range s {
		if !(b >= 'a' && b <= 'z') && !(b >= 'A' && b <= 'Z') &&
			!(b >= '0' && b <= '9') && b != '-' && b != '_' {

			return true
		}
	}

	return false
}
