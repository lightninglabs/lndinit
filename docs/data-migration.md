# Data migration

This document describes the process of migrating `LND`'s database state from one
type of database backend (for example the `bbolt` based database files `*.db`
such as the `channel.db` or `wallet.db` files) to another (for example the new 
`postgres` or `sqlite` databases introduced in `lnd v0.14.0-beta`).

**Note:** Currently only migrations from `bolt` to either `postgres` or 
`sqlite` are supported. KV based databases will be phased out in the future. We
 are planning to add additonal support to migrate all `etcd` databases to `sql`
 databases. Moreover it is currently also not supported to move the database 
 from `sqlite` to `postgres` or vice versa, because this migration only takes 
 care of the key value data and there already exist the possiblity to have the 
 invoices in native sql which this tool does not support.


## Prepare the destination database

To be able to execute the migration successfully you have to update to [LND 
v0.19.0](https://github.com/lightningnetwork/lnd/releases/tag/untagged-6824d65b23dc77e94e22) 
which makes sure that the intial pre-migration state of the kv-db is up to date.

### Using postgres as the destination remote database

Prepare a user and database as described in the [Postgres](
   https://github.com/lightningnetwork/lnd/blob/master/docs/postgres.md)
documentation. You'll need the Data Source Name (DSN) for both the data
migration and then the `lnd` configuration, so keep that string somewhere
(should be something with the format of `postgres://xx:yy@localhost:5432/zz`).

No additional steps are required to prepare the Postgres database for the data
migration. The migration tool will create the database schema automatically, so
no DDL scripts need to be run in advance. But to speed up the migration process
you should take a look at the following [postgres server tuning guide](https://gist.github.com/djkazic/526fa3e032aea9578997f88b45b91fb9)

### Using sqlite as the destination remote database

No particular preparation is needed for `sqlite` compared to the `postgres`
case. Similar to the `bolt` case there will be separated db files created for
each individual `bolt` database.



## Prepare the source database

Assuming we want to migrate the database state from the pre-0.19.0 individual
`bbolt` based `*.db` files to a remote database, we first need to make sure the
source files are in the correct state.

The following steps should be performed *before* running the data migration (
some of them are marked as optional and can be neglected for small databases
e.g. 200MB
):
1. Stop `lnd`
2. Upgrade the `lnd` binary to the latest version (e.g. `v0.19.0-beta` or later)
3. (optional) Make sure to add config options like 
   `gc-canceled-invoices-on-startup=true` and `db.bolt.auto-compact=true` to 
   your `lnd.conf` to optimize the source database size by removing canceled 
   invoices and compacting it on startup.
4. Remove any data from the source database that you can. The fewer
   entries are in the source database, the quicker the migration will complete.
   For example failed payments (or their failed HTLC attempts) can be removed
   with `lncli deletepayments --all`. This can make a huge difference for 
   routing nodes which rebalance a lot. Make sure you restart LND and compact 
   the db after the failed payments were deleted so it has an effect on the size
   of the db.
5. (optional) Also make sure to migrate the revocation log for all channels 
   active prior to `lnd@0.15.0` by activating the config setting `--db.prune-revocation`.
   This version introduced an optimized revocation log storage system that
   reduces the storage footprint. All channels will be automatically migrated
   to this new format when the setting is enabled.
6. If you have ever run the watchtower client you need to make sure before
   starting the migration that you either activate it again and restart LND so
   that the db is up to date (the newest migrations have been applied) or you
   delete the `wtclient.db` manually so that the migration can proceed
   successfully because you do not plan to use the wtclient anymore.
7. Start `lnd` normally, using the flags mentioned above but not yet changing
   any database backend related configuration options. Check the log that the
   database schema was migrated successfully, for example: `Checking for
   schema update: latest_version=XX, db_version=XX`. This relates to the 
   kv-schema NOT any SQL schema, this makes sure all inital migration of the 
   old database were performed. This migration tool is only allowed for DBs
   which have all the latest db migrations applied up to LND 19. This makes sure
   that all LND nodes which want to migrate to the SQL world have the latest db
   modifications in place before migrating to a different DB type. If that is
   not the case the migration will be refused. 
8. Stop `lnd` again and make sure it isn't started again by accident during the
   data migration (e.g. disable any `systemd` or other scripts that start/stop
   `lnd`).

## Run the migration

Depending on the destination database type, run the migration with a command
similar to one of the following examples:

**Example: Migrate from `bbolt` to `sqlite`:**

```shell
lndinit --debuglevel info  migrate-db \
--source.bolt.data-dir /home/myuser/.lnd/data  \
--dest.backend sqlite  \
--dest.sqlite.data-dir  /home/myuser/.lnd/data  --network mainnet 
```
If you were running a watchtower server, and it had a different directory set
compared to the default `LND` directory make sure you also add the tower dir
setting. It has to be the directory to the `watchtower` dir, excluding the
`watchtower` name e.g.:
`--source.bolt.tower-dir /home/myuser/towerdir`.


**Example: Migrate from `bbolt` to `postgres`:**

```shell
lndinit  --debuglevel info migrate-db \
      --source.bolt.data-dir /home/myuser/.lnd/data \
      --dest.backend postgres \
      --dest.postgres.dsn=postgres://postgres:postgres@localhost:5432/postgres
```

Also set the watchtower directory in case you used a different path, see above.

This migration tool depends on the directory structure of
the LND software. This means make sure you link the correct folder because the 
`bolt` database has several database files in serveral subfolders. This 
migration tool will make sure that all the required databases are migrated. It 
will not require a `wtclient.db` or a `watchtower.db`.

The migration is resumable and happens in chunks of default 20MB to make it 
compatible with most of the systems including low power devices like 
raspberry pis. However if you have better setup with way more RAM feel free to 
increase the `--chunk-size` to something like
200MB which should speed up the migration. You can also change the chunk size
during the migration as well. If you want to start the migration from the 
beginning (can only be done if the migration did still not succeed) use the 
flag `--force-new-migration` which can be used in combination with a 
new `chunksize` limit.

In case you have successfully migrated several nodes and are not sure anymore 
which source db corresponds to which destination db there is a flag called
`force-verify-db` which only works if both dbs are marked as successfully
migrated. It will verify the contents of the db.


## After the migration was successful

Make sure the whole migration process succeeds before starting the new node with
the new underlying database. As mentioned above there are several database files
at play here so all of them have to succeed to garantee a successful migration.

If the migration succeeded successfully and you see the following log entry of
the migration tool you are good to go to start-up your lnd node with the new
database.

```shell
[INF]: LNDINIT !!!Migration of all mandatory db parts completed successfully!!!
```

The mandatory dbs are:
* `channel.db`
* `macaroons.db`
* `sphinxreplay.db`
* `wallet.db`

The optional dbs are:

* `wtclient.db`
* `watchtower.db`
* `neutrino.db`

### LND config setting for `sqlite` backend

```shell
[db]
db.backend=sqlite
```

There are several other sqlite setttings you can tweak to make it fit your 
needs, take a look at the [lnd-sample-config](https://github.com/lightningnetwork/lnd/blob/b6d8ecc7479f7517368814c398b0fbe0e8c52fed/sample-lnd.conf).

### LND config setting for `postgres` backend

```shell
[db]
db.backend=postgres

[postgres]

db.postgres.dsn=postgres://xx:yy@localhost:5432/zz
```

Use the same connection string you used for the migration. Also take a look at 
the postgres knobs in the [lnd-sample-config](https://github.com/lightningnetwork/lnd/blob/b6d8ecc7479f7517368814c398b0fbe0e8c52fed/sample-lnd.conf).


This is the output of the migration cmd help settings:

```shell
lndinit migrate-db -h
Usage:
  lndinit [OPTIONS] migrate-db [migrate-db-OPTIONS]

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
twice, which is checked through the 'already migrated' tag.

Application Options:
  -e, --error-on-existing                             Exit with code EXIT_CODE_TARGET_EXISTS (128) instead of 0 if the result of an action is already present
  -d, --debuglevel=                                   Set the log level (Off, Critical, Error, Warn, Info, Debug, Trace)

Help Options:
  -h, --help                                          Show this help message

[migrate-db command options]
      -n, --network=                                  Network of the db files to migrate (used to navigate into the right directory) (default: mainnet)
          --pprof-port=                               Enable pprof profiling on the specified port
          --force-new-migration                       Force a new migration from the beginning of the source DB so the resume state will be discarded
          --force-verify-db                           Force a verification verifies two already marked (tombstoned and already migrated) dbs to make sure that the source db equals the
                                                      content of the destination db
          --chunk-size=                               Chunk size for the migration in bytes

    source:
          --source.backend=[bolt]                     The source database backend. (default: bolt)

    bolt:
          --source.bolt.dbtimeout=                    Specify the timeout value used when opening the database. (default: 1m0s)
          --source.bolt.data-dir=                     Lnd data dir where bolt dbs are located.
          --source.bolt.tower-dir=                    Lnd watchtower dir where bolt dbs for the watchtower server are located.

    dest:
          --dest.backend=[postgres|sqlite]            The destination database backend. (default: postgres)

    postgres:
          --dest.postgres.dsn=                        Database connection string.
          --dest.postgres.timeout=                    Database connection timeout. Set to zero to disable.
          --dest.postgres.maxconnections=             The maximum number of open connections to the database. Set to zero for unlimited.

    sqlite:
          --dest.sqlite.data-dir=                     Lnd data dir where sqlite dbs are located.
          --dest.sqlite.tower-dir=                    Lnd watchtower dir where sqlite dbs for the watchtower server are located.

    sqlite-config:
          --dest.sqlite.sqlite-config.timeout=        The time after which a database query should be timed out.
          --dest.sqlite.sqlite-config.busytimeout=    The maximum amount of time to wait for a database connection to become available for a query.
          --dest.sqlite.sqlite-config.maxconnections= The maximum number of open connections to the database. Set to zero for unlimited.
          --dest.sqlite.sqlite-config.pragmaoptions=  A list of pragma options to set on a database connection. For example, 'auto_vacuum=incremental'. Note that the flag must be specified multiple times if multiple options are to be set.
```