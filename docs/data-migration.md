# Data migration

This document describes the process of migrating `LND`'s database state from one
type of database backend (for example the `bbolt` based database files `*.db`
such as the `channel.db` or `wallet.db` files) to another (for example the new `postgres` or `sqlite` databases introduced in `lnd v0.14.0-beta`).

**Note:** Currently only migrations from `bolt` to either `postgres` or `sqlite` are supported. KV based databases will be phased out in the future. We are planning to add additonal support to migrate all `etcd` databases to `sql` databases. Moreover it is currently also not supported to move the database from `sqlite` to `postgres` or vice versa, because this migration only takes care of the key value data and there already exist the possiblity to have the invoices in native sql which this tool does not support.


## Prepare the destination database

### Using postgres as the destination remote database

Prepare a user and database as described in the [Postgres](
   https://github.com/lightningnetwork/lnd/blob/master/docs/postgres.md)
documentation. You'll need the Data Source Name (DSN) for both the data
migration and then the `lnd` configuration, so keep that string somewhere
(should be something with the format of `postgres://xx:yy@localhost:5432/zz`).

No additional steps are required to prepare the Postgres database for the data
migration. The migration tool will create the database schema automatically, so
no DDL scripts need to be run in advance.

## Prepare the source database

Assuming we want to migrate the database state from the pre-0.19.0 individual
`bbolt` based `*.db` files to a remote database, we first need to make sure the
source files are in the correct state.

The following steps should be performed *before* running the data migration:
1. Stop `lnd`
2. Upgrade the `lnd` binary to the latest version (e.g. `v0.19.0-beta` or later)
3. Make sure to add config options like `gc-canceled-invoices-on-startup=true`
   and `db.bolt.auto-compact=true` to your `lnd.conf` to optimize the source
   database size by removing canceled invoices and compacting it on startup.
4. Also make sure to migrate the revocation log for all channels active prior
   to `lnd@0.15.0` by activating the config setting `--db.prune-revocation`.
   This version introduced an optimized revocation log storage system that
   reduces the storage footprint. All channels will be automatically migrated
   to this new format when the setting is enabled.
5. Start `lnd` normally, using the flags mentioned above but not yet changing
   any database backend related configuration options. Check the log that the
   database schema was migrated successfully, for example: `Checking for
   schema update: latest_version=XX, db_version=XX`
6. Remove any data from the source database that you can. The fewer entries are
   in the source database, the quicker the migration will complete. For example
   failed payments (or their failed HTLC attempts) can be removed with
   `lncli deletepayments`.
7. Stop `lnd` again and make sure it isn't started again by accident during the
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
If you were running a watchtower server, and it had a different directory set compared to the default `LND` directory make sure you also add the tower dir setting. It has to be the directory to the `watchtower` dir, excluding the `watchtower` name e.g.:
`--source.bolt.tower-dir /home/myuser/towerdir`.

 **IMPORTANT:** When selecting the destination directory for the `sqlite` 
 case it is recommended to use the same as the source directory. This
 guarantees that all the necessary sub-dbs will be placed in the correct 
 sub-directory. Otherwise make sure that the destination directory has the same
 sub-dir structure as the `.lnd` directory so that files can be placed in the 
 equivalent sub-dir and then the folder can be copied as a whole (however make
 sure you also copy all the other data which is part of the `.lnd` folder). 
 Failing to match the required structure may result in errors, such as:

 ```shell
 [ERR]: LNDINIT Runtime error: failed to open destination db channeldb: unable to open database file: out of memory (14)
 ```

**Example: Migrate from `bbolt` to `postgres`:**

```shell
lndinit  --debuglevel info migrate-db \
      --source.bolt.data-dir /home/myuser/.lnd/data \
      --dest.backend postgres \
      --dest.postgres.dsn=postgres://postgres:postgres@localhost:5432/postgres
```

Also set the watchtower directory in case you used a different path, see above.

This migration tool depends on the directory structure of
the LND software. This means make sure you link the correct folder because the `bolt` database has several database files in serveral subfolders. This migration tool will make sure that all the required databases are migrated. It will not require a `wtclient.db` or a `watchtower.db`.

The migration is resumable and happens in chunks of default 20MB to make it compatible with most of the systems. However if you have better
setup feel free to increase the `--chunk-size` which should speed up the migration.
If you happened to choose a smaller chunksize and want to increase it you need to start the whole migration from the beginning because the migration will also be verified and for the verification the `chunksize` needs to be constant for the whole process. Therefore if you want to start the migration from the beginning (can only be done if the migration did still not succeed) use the flag `--force-new-migration` which can be used in combination with a new `chunksize` limit.



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
* `channeldb`
* `macaroondb`
* `decayedlogdb`
* `walletdb`

The optional dbs are:

* `towerclientdb`
* `towerserverdb`

### LND config setting for `sqlite` backend

```shell
[db]
db.backend=sqlite
```

There are several other sqlite setttings you can tweak to make it fit your needs, take a look at the [lnd-sample-config](https://github.com/lightningnetwork/lnd/blob/b6d8ecc7479f7517368814c398b0fbe0e8c52fed/sample-lnd.conf).

### LND config setting for `postgres` backend

```shell
[db]
db.backend=postgres

[postgres]

db.postgres.dsn=postgres://xx:yy@localhost:5432/zz
```

Use the same connection string you used for the migration. Also take a look at the postgres knobs in the [lnd-sample-config](https://github.com/lightningnetwork/lnd/blob/b6d8ecc7479f7517368814c398b0fbe0e8c52fed/sample-lnd.conf).