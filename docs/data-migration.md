# Data migration

This document describes the process of migrating `lnd`'s database state from one
type of database backend (for example the `bbolt` based database files `*.db`
such as the `channel.db` or `wallet.db` files) to another (for example the new
remote database backends such as `etcd` or `postgres` introduced in
`lnd v0.14.0-beta`).

## Prepare the destination database

### Using etcd as the destination remote database

When using `etcd` as the main remote database, some specific environment
variables need to be set to ensure smooth operations both during the data
migration and also later for running `lnd` in production.

Make sure you are running **at least `etcd v3.5.0` or later** with the following
environment variables (or their command line flag counterparts) set:

```shell
# Allow lnd to batch up to 16k operations into a single transaction with a max
# total TX size of 104MB.
ETCD_MAX_TXN_OPS=16384
ETCD_MAX_REQUEST_BYTES=104857600

# Keep 10k revisions for raft consensus in clustered mode.
ETCD_AUTO_COMPACTION_RETENTION=10000
ETCD_AUTO_COMPACTION_MODE=revision

# Allow the total database size to be up to 15GB. Adjust this to your needs!
ETCD_QUOTA_BACKEND_BYTES=16106127360
```

Make sure you set the `ETCD_QUOTA_BACKEND_BYTES` to something that is
sufficiently larger than your current size of the `channel.db` file!

### Using postgres as the destination remote database

Prepare a user and database as described in the [Postgres](postgres.md)
documentation. You'll need the Data Source Name (DSN) for both the data
migration and then the `lnd` configuration, so keep that string somewhere
(should be something with the format of `postgres://xx:yy@localhost:5432/zz`).

No additional steps are required to prepare the Postgres database for the data
migration. The migration tool will create the database schema automatically, so
no DDL scripts need to be run in advance.

## Prepare the source database

Assuming we want to migrate the database state from the pre-0.14.0 individual
`bbolt` based `*.db` files to a remote database, we first need to make sure the
source files are in the correct state.

The following steps should be performed *before* running the data migration:
1. Stop `lnd`
2. Upgrade the `lnd` binary to the latest version (e.g. `v0.14.0-beta` or later)
3. Make sure to add config options like `gc-canceled-invoices-on-startup=true`
   and `db.bolt.auto-compact=true` to your `lnd.conf` to optimize the source
   database size by removing canceled invoices and compacting it on startup.
4. Start `lnd` normally, using the flags mentioned above but not yet changing
   any database backend related configuration options. Check the log that the
   database schema was migrated successfully, for example: `Checking for
   schema update: latest_version=XX, db_version=XX`
5. Remove any data from the source database that you can. The fewer entries are
   in the source database, the quicker the migration will complete. For example
   failed payments (or their failed HTLC attempts) can be removed with
   `lncli deletepayments`.
6. Stop `lnd` again and make sure it isn't started again by accident during the
   data migration (e.g. disable any `systemd` or other scripts that start/stop
   `lnd`).

NOTE: If you were using the experimental `etcd` cluster mode that was introduced
in `lnd v0.13.0` it is highly recommended starting a fresh node. While the data
can in theory be migrated from the partial/mixed `etcd` and `bbolt` based state
the migration tool does not support it.

## Run the migration

Depending on the destination database type, run the migration with a command
similar to one of the following examples:

**Example: Migrate from `bbolt` to `etcd`:**

```shell
⛰  lndinit -v migrate-db \
      --source.bolt.data-dir /home/myuser/.lnd/data \
      --source.bolt.tower-dir /home/myuser/.lnd/watchtower \
      --dest.backend etcd \
      --dest.etcd.host=my-etcd-cluster-address:2379
```
If you weren't running a watchtower server, you can remove the line with
`--source.bolt.tower-dir`.

**Example: Migrate from `bbolt` to `postgres`:**

```shell
⛰  lndinit -v migrate-db \
      --source.bolt.data-dir /home/myuser/.lnd/data \
      --source.bolt.tower-dir /home/myuser/.lnd/data/watchtower \
      --dest.backend postgres \
      --dest.postgres.dsn=postgres://postgres:postgres@localhost:5432/postgres
```

If you weren't running a watchtower server, you can remove the line with
`--source.bolt.tower-dir`.
