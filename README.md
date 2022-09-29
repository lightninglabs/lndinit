# lndinit: a wallet initializer utility for lnd

This repository contains the source for the `lndinit` command.
The main purpose of `lndinit` is to help automate the `lnd` wallet
initialization, including seed and password generation.

- [Requirements](#requirements)
- [Subcommands](#subcommands)
  - [`gen-password`](#gen-password)
  - [`gen-seed`](#gen-seed)
  - [`load-secret`](#load-secret)
  - [`store-secret`](#store-secret)
  - [`init-wallet`](#init-wallet)
  - [`wait-ready`](#wait-ready)
  - [`migrate-db`](#migrate-db)
- [Example usage](#example-usage)
  - [Basic setup](#example-use-case-1-basic-setup)
  - [Kubernetes](#example-use-case-2-kubernetes)
- [Logging and idempotent operations](#logging-and-idempotent-operations)

## Requirements

Most commands of this tool operate independently of `lnd` and therefore don't
require a specific version to be installed.

The commands `wait-ready` and `init-wallet` only work with `lnd v0.14.2-beta`
and later though.

A recent version of Kubernetes is needed when interacting with secrets stored in
k8s. Any version `>= v1.8` should work.

---

## Subcommands

Most commands work without `lnd` running, as they are designed to do some provisioning work _before_ `lnd` is started.

### gen-password
`gen-password` generates a random password (no `lnd` needed)

### gen-seed
`gen-seed` generates a random seed phrase

No `lnd` needed, but seed will be in `lnd`-specific [`aezeed` format](https://github.com/lightningnetwork/lnd/blob/master/aezeed/README.md)

### load-secret
`load-secret` interacts with kubernetes to read from secrets (no `lnd` needed)

### store-secret
`store-secret` interacts with kubernetes to write to secrets (no `lnd` needed)

### init-wallet
`init-wallet` has two modes:
- `--init-type=file` creates an `lnd` specific `wallet.db` file
  - Only works if `lnd` is NOT running yet
- `--init-type=rpc` calls the `lnd` RPC to create a wallet
  - Use this mode if you are using a remote database as `lnd`'s storage backend instead of bolt DB based file databases
  - Needs `lnd` to be running and no wallet to exist

### wait-ready
`wait-ready` waits for `lnd` to be ready by connecting to `lnd`'s status RPC
- Needs `lnd` to run, eventually

### migrate-db
`migrate-db` migrates the content of one `lnd` database to another, for example
from `bbolt` to Postgres. See [data migration guide](docs/data-migration.md) for
more information.

---

## Example Usage

### Example use case 1: Basic setup

This is a very basic example that shows the purpose of the different sub
commands of the `lndinit` binary. In this example, all secrets are stored in
files. This is normally not a good security practice as potentially other users
or processes on a system can read those secrets if the permissions aren't set
correctly. It is advised to store secrets in dedicated secret storage services
like Kubernetes Secrets or HashiCorp Vault.

#### 1. Generate a seed without a seed passphrase

Create a new seed if one does not exist yet.

```shell
$ if [[ ! -f /safe/location/seed.txt ]]; then
    lndinit gen-seed > /safe/location/seed.txt
  fi
```

#### 2. Generate a wallet password

Create a new wallet password if one does not exist yet.

```shell
$ if [[ ! -f /safe/location/walletpassword.txt ]]; then
    lndinit gen-password > /safe/location/walletpassword.txt
  fi
```

#### 3. Initialize the wallet

Create the wallet database with the given seed and password files. If the wallet
already exists, we make sure we can actually unlock it with the given password
file. This will take a few seconds in any case.

```shell
$ lndinit -v init-wallet \
    --secret-source=file \
    --file.seed=/safe/location/seed.txt \
    --file.wallet-password=/safe/location/walletpassword.txt \
    --init-file.output-wallet-dir=$HOME/.lnd/data/chain/bitcoin/mainnet \
    --init-file.validate-password
```

#### 4. Start and auto unlock lnd

With everything prepared, we can now start lnd and instruct it to auto unlock
itself with the password in the file we prepared.

```shell
$ lnd \
    --bitcoin.active \
    ...
    --wallet-unlock-password-file=/safe/location/walletpassword.txt
```

### Example use case 2: Kubernetes

This example shows how Kubernetes (k8s) Secrets can be used to store the wallet
seed and password. The pod running those commands must be provisioned with a
service account that has permissions to read/create/modify secrets in a given
namespace.

Here's an example of a service account, role provision and pod definition:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: lnd-provision-account


---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: lnd-update-secrets-role
  namespace: default
rules:
  - apiGroups: [ "" ]
    resources: [ "secrets" ]
    verbs: [ "get", "list", "watch", "update" ]


---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: lnd-update-secrets-role-binding
  namespace: default
roleRef:
  kind: Role
  name: lnd-update-secrets-role
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: lnd-provision-account
    namespace: default


---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lnd-pod
spec:
  strategy:
    type: Recreate
  replicas: 1
  template:
    spec:
      # We use the special service account created, so the init script is able
      # to update the secret as expected.
      serviceAccountName: lnd-provision-account

      containers:
        # The main lnd container
        - name: lnd
          
          # The lndinit image is an image based on the main lnd image that just
          # adds the lndinit binary to it. The tag name is simply:
          #   <lndinit-version>-lnd-<lnd-version>
          image: lightninglabs/lndinit:v0.1.0-lnd-v0.14.2-beta
          env:
            - name: WALLET_SECRET_NAME
              value: lnd-wallet-secret
            - name: WALLET_DIR
              value: /root/.lnd/data/chain/bitcoin/mainnet
            - name: CERT_DIR
              value: /root/.lnd
            - name: UPLOAD_RPC_SECRETS
              value: '1'
            - name: RPC_SECRETS_NAME
              value: lnd-rpc-secrets
          command: [ '/init-wallet-k8s.sh' ]
          args: [
              '--bitcoin.mainnet',
              '...',
              '--wallet-unlock-password-file=/tmp/wallet-password',
          ]
```

The `/init-wallet-k8s.sh` script that is invoked in the example above can be
found in this repository:
[`example-init-wallet-k8s.sh`](example-init-wallet-k8s.sh)
The script executes the steps described in this example and also uploads the
RPC secrets (`tls.cert` and all `*.macaroon` files) to another secret so apps
using the `lnd` node can access those secrets.

#### 1. Generate a seed passphrase (optional)

Generate a new seed passphrase. If an entry with the key already exists in the
k8s secret, it is not overwritten, and the operation is a no-op.

```shell
$ lndinit gen-password \
    | lndinit -v store-secret \
    --target=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.secret-key-name=seed-passphrase
```

#### 2. Generate a seed using the passphrase

Generate a new seed with the passphrase created before. If an entry with that
key already exists in the k8s secret, it is not overwritten, and the operation
is a no-op.

```shell
$ lndinit -v gen-seed \
    --passphrase-k8s.secret-name=lnd-secrets \
    --passphrase-k8s.secret-key-name=seed-passphrase \
    | lndinit -v store-secret \
    --target=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.secret-key-name=seed
```

#### 3. Generate a wallet password

Generate a new wallet password. If an entry with that key already exists in the
k8s secret, it is not overwritten, and the operation is a no-op.

```shell
$ lndinit gen-password \
    | lndinit -v store-secret \
    --target=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.secret-key-name=wallet-password
```

#### 4. Initialize the wallet, attempting a test unlock with the password

Create the wallet database with the given seed, seed passphrase and wallet
password loaded from a k8s secret. If the wallet already exists, we make sure we
can actually unlock it with the given password file. This will take a few
seconds in any case.

```shell
$ lndinit -v init-wallet \
    --secret-source=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.seed-key-name=seed \
    --k8s.seed-passphrase-key-name=seed-passphrase \
    --k8s.wallet-password-key-name=wallet-password \
    --init-file.output-wallet-dir=$HOME/.lnd/data/chain/bitcoin/mainnet \
    --init-file.validate-password
```

The above is an example for a file/bbolt based node. For such a node creating
the wallet directly as a file is the most secure option, since it doesn't
require the node to spin up the wallet unlocker RPC (which doesn't use macaroons
and is therefore un-authenticated).

But in setups where the wallet isn't a file (since all state is in a remote
database such as etcd or Postgres), this method cannot be used.
Instead, the wallet needs to be initialized through RPC, as shown in the next
example:

```shell
$ lndinit -v init-wallet \
    --secret-source=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.seed-key-name=seed \
    --k8s.seed-passphrase-key-name=seed-passphrase \
    --k8s.wallet-password-key-name=wallet-password \
    --init-type=rpc \
    --init-rpc.server=localhost:10009 \
    --init-rpc.tls-cert-path=$HOME/.lnd/tls.cert
```

**NOTE**: If this is used in combination with the
`--wallet-unlock-password-file=` flag in `lnd` for automatic unlocking, then the
`--wallet-unlock-allow-create` flag also needs to be set. Otherwise, `lnd` won't
be starting the wallet unlocking RPC that is used for initializing the wallet.

The following example shows how to use the `lndinit init-wallet` command to
create a watch-only wallet from a previously exported accounts JSON file:

```shell
$ lndinit -v init-wallet \
    --secret-source=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.seed-key-name=seed \
    --k8s.seed-passphrase-key-name=seed-passphrase \
    --k8s.wallet-password-key-name=wallet-password \
    --init-type=rpc \
    --init-rpc.server=localhost:10009 \
    --init-rpc.tls-cert-path=$HOME/.lnd/tls.cert \
    --init-rpc.watch-only \
    --init-rpc.accounts-file=/tmp/accounts.json
```

#### 5. Store the wallet password in a file

Because we now only have the wallet password as a value in a k8s secret, we need
to retrieve it and store it in a file that `lnd` can read to auto unlock.

```shell
$ lndinit -v load-secret \
    --source=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.secret-key-name=wallet-password > /safe/location/walletpassword.txt
```

**Security notice**:

Any process or user that has access to the file system of the container can
potentially read the password if it's stored as a plain file.
For an extra bump in security, a named pipe can be used instead of a file. That
way the password can only be read exactly once from the pipe during `lnd`'s
startup.

```shell
# Create a FIFO pipe first. This will behave like a file except that writes to
# it will only occur once there's a reader on the other end.
$ mkfifo /tmp/wallet-password

# Read the secret from Kubernetes and write it to the pipe. This will only
# return once lnd is actually reading from the pipe. Therefore we need to run
# the command as a background process (using the ampersand notation).
$ lndinit load-secret \
    --source=k8s \
    --k8s.secret-name=lnd-secrets \
    --k8s.secret-key-name=wallet-password > /tmp/wallet-password &

# Now run lnd and point it to the named pipe.
$ lnd \
    --bitcoin.active \
    ...
    --wallet-unlock-password-file=/tmp/wallet-password
```

#### 6. Start and auto unlock lnd

With everything prepared, we can now start lnd and instruct it to auto unlock
itself with the password in the file we prepared.

```shell
$ lnd \
    --bitcoin.active \
    ...
    --wallet-unlock-password-file=/safe/location/walletpassword.txt
```

---

## Logging and idempotent operations

By default, `lndinit` aborts and exits with a zero return code if the desired
result is already achieved (e.g. a secret key or a wallet database already
exist). This can make it hard to follow exactly what is happening when debugging
the initialization. To assist with debugging, the following two flags can be
used:

- `--verbose (-v)`: Log debug information to `stderr`.
- `--error-on-existing (-e)`: Exit with a non-zero return code (128) if the
  result of an operation already exists. See example below.

**Example**:

```shell
# Treat every non-zero return code as abort condition (default for k8s container
# commands).
$ set -e

# Run the command and catch any non-zero return code in the ret variable. The
# logical OR is required to not fail because of above setting.
$ ret=0
$ lndinit --error-on-existing init-wallet ... || ret=$?
$ if [[ $ret -eq 0 ]]; then
    echo "Successfully initialized wallet."
  elif [[ $ret -eq 128 ]]; then
    echo "Wallet already exists, skipping initialization."
  else
    echo "Failed to initialize wallet!"
    exit 1
  fi
```

