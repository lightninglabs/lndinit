#!/bin/bash

set -e

# The address of the Vault server. Inside a Kubernetes cluster this is usually
# the in-cluster service DNS name. It can also be provided to lndinit via the
# VAULT_ADDR environment variable instead of the --vault.addr flag.
VAULT_ADDR=${VAULT_ADDR:-http://vault.vault.svc.cluster.local:8200}
export VAULT_ADDR

# The Vault Kubernetes auth role that binds this pod's ServiceAccount to a
# policy allowing access to the wallet secret path.
VAULT_AUTH_ROLE=${VAULT_AUTH_ROLE:-lnd}

# The mount path of the KV v2 secrets engine and the path of the wallet secret
# within it (excluding the engine mount and the "data/" segment).
VAULT_KV_MOUNT=${VAULT_KV_MOUNT:-secret}
VAULT_SECRET_PATH=${VAULT_SECRET_PATH:-lnd/mynode/wallet}

WALLET_DIR=${WALLET_DIR:-~/.lnd/data/chain/bitcoin/mainnet}
WALLET_PASSWORD_FILE=${WALLET_PASSWORD_FILE:-/tmp/wallet-password}
CERT_DIR=${CERT_DIR:-~/.lnd}
UPLOAD_RPC_SECRETS=${UPLOAD_RPC_SECRETS:-0}
RPC_SECRETS_PATH=${RPC_SECRETS_PATH:-lnd/mynode/rpc}

echo "[STARTUP] Asserting wallet password exists in secret ${VAULT_SECRET_PATH}"
lndinit gen-password \
  | lndinit -v store-secret \
  --target=vault \
  --vault.auth-role="${VAULT_AUTH_ROLE}" \
  --vault.kv-mount="${VAULT_KV_MOUNT}" \
  --vault.secret-path="${VAULT_SECRET_PATH}" \
  --vault.secret-key-name=walletpassword

echo ""
echo "[STARTUP] Asserting seed exists in secret ${VAULT_SECRET_PATH}"
lndinit gen-seed \
  | lndinit -v store-secret \
  --target=vault \
  --vault.auth-role="${VAULT_AUTH_ROLE}" \
  --vault.kv-mount="${VAULT_KV_MOUNT}" \
  --vault.secret-path="${VAULT_SECRET_PATH}" \
  --vault.secret-key-name=walletseed

echo ""
echo "[STARTUP] Asserting wallet is created with values from secret ${VAULT_SECRET_PATH}"
lndinit -v init-wallet \
  --secret-source=vault \
  --vault.auth-role="${VAULT_AUTH_ROLE}" \
  --vault.kv-mount="${VAULT_KV_MOUNT}" \
  --vault.secret-path="${VAULT_SECRET_PATH}" \
  --vault.seed-key-name=walletseed \
  --vault.wallet-password-key-name=walletpassword \
  --init-file.output-wallet-dir="${WALLET_DIR}" \
  --init-file.validate-password

echo ""
echo "[STARTUP] Preparing lnd auto unlock file"

# To make sure the password can be read exactly once (by lnd itself), we create
# a named pipe. Because we can only write to such a pipe if there's a reader on
# the other end, we need to run this in a sub process in the background.
mkfifo "${WALLET_PASSWORD_FILE}"
lndinit -v load-secret \
  --source=vault \
  --vault.auth-role="${VAULT_AUTH_ROLE}" \
  --vault.kv-mount="${VAULT_KV_MOUNT}" \
  --vault.secret-path="${VAULT_SECRET_PATH}" \
  --vault.secret-key-name=walletpassword > "${WALLET_PASSWORD_FILE}" &

# In case we want to upload the TLS certificate and macaroons to Vault once lnd
# is ready, we can use the wait-ready and store-secret commands in combination
# to wait until lnd is ready and then batch upload the files to Vault.
if [[ "${UPLOAD_RPC_SECRETS}" == "1" ]]; then
  echo ""
  echo "[STARTUP] Starting RPC secret uploader process in background"
  lndinit -v wait-ready \
    && lndinit -v store-secret \
    --batch \
    --overwrite \
    --target=vault \
    --vault.auth-role="${VAULT_AUTH_ROLE}" \
    --vault.kv-mount="${VAULT_KV_MOUNT}" \
    --vault.secret-path="${RPC_SECRETS_PATH}" \
    "${CERT_DIR}/tls.cert" \
    "${WALLET_DIR}"/*.macaroon &
fi

# And finally start lnd. We need to use "exec" here to make sure all signals are
# forwarded correctly.
echo ""
echo "[STARTUP] Starting lnd with flags: $@"
exec lnd "$@"
