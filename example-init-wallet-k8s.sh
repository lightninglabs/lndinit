#!/bin/bash

set -e

WALLET_SECRET_NAME=${WALLET_SECRET_NAME:-lnd-wallet-secret}
WALLET_SECRET_NAMESPACE=${WALLET_SECRET_NAMESPACE:-default}
WALLET_DIR=${WALLET_DIR:-~/.lnd/data/chain/bitcoin/mainnet}
WALLET_PASSWORD_FILE=${WALLET_PASSWORD_FILE:-/tmp/wallet-password}
CERT_DIR=${CERT_DIR:-~/.lnd}
UPLOAD_RPC_SECRETS=${UPLOAD_RPC_SECRETS:-0}
RPC_SECRETS_NAME=${RPC_SECRETS_NAME:-lnd-rpc-secret}
RPC_SECRETS_NAMESPACE=${RPC_SECRETS_NAMESPACE:-default}
NETWORK=${NETWORK:-mainnet}
RPC_SERVER=${RPC_SERVER:-localhost:10009}
REMOTE_SIGNING=${REMOTE_SIGNING:0}
REMOTE_SIGNER_RPC_SECRETS_DIR=${REMOTE_SIGNER_RPC_SECRETS_DIR:-/tmp}
REMOTE_SIGNER_RPC_SECRETS_NAME=${REMOTE_SIGNER_RPC_SECRETS_NAME:-lnd-signer-rpc-secret}
REMOTE_SIGNER_RPC_SECRETS_NAMESPACE=${REMOTE_SIGNER_RPC_SECRETS_NAMESPACE:-signer}

echo "[STARTUP] Asserting wallet password exists in secret ${WALLET_SECRET_NAME}"
lndinit gen-password \
  | lndinit -v store-secret \
  --target=k8s \
  --k8s.base64 \
  --k8s.namespace="${WALLET_SECRET_NAMESPACE}" \
  --k8s.secret-name="${WALLET_SECRET_NAME}" \
  --k8s.secret-key-name=walletpassword

echo ""
echo "[STARTUP] Asserting seed exists in secret ${WALLET_SECRET_NAME}"
lndinit gen-seed \
  | lndinit -v store-secret \
  --target=k8s \
  --k8s.base64 \
  --k8s.namespace="${WALLET_SECRET_NAMESPACE}" \
  --k8s.secret-name="${WALLET_SECRET_NAME}" \
  --k8s.secret-key-name=walletseed

echo ""
echo "[STARTUP] Asserting wallet is created with values from secret ${WALLET_SECRET_NAME}"
lndinit -v init-wallet \
  --secret-source=k8s \
  --k8s.base64 \
  --k8s.namespace="${WALLET_SECRET_NAMESPACE}" \
  --k8s.secret-name="${WALLET_SECRET_NAME}" \
  --k8s.seed-key-name=walletseed \
  --k8s.wallet-password-key-name=walletpassword \
  --init-file.output-wallet-dir="${WALLET_DIR}" \
  --init-file.validate-password

echo ""
echo "[STARTUP] Preparing lnd auto unlock file"

# To make sure the password can be read exactly once (by lnd itself), we create
# a named pipe. Because we can only write to such a pipe if there's a reader on
# the other end, we need to run this in a sub process in the background.
mkfifo "${WALLET_PASSWORD_FILE}"
lndinit -v load-secret \
  --source=k8s \
  --k8s.base64 \
  --k8s.namespace="${WALLET_SECRET_NAMESPACE}" \
  --k8s.secret-name="${WALLET_SECRET_NAME}" \
  --k8s.secret-key-name=walletpassword > "${WALLET_PASSWORD_FILE}" &

# In case we have a remote signing setup, we also need to provision the RPC
# secrets of the remote signer.
if [[ "${REMOTE_SIGNING}" == "1" ]]; then
  echo "[STARTUP] Provisioning remote signer RPC secrets"
  lndinit -v load-secret \
    --source=k8s \
    --k8s.base64 \
    --k8s.namespace="${REMOTE_SIGNER_RPC_SECRETS_NAMESPACE}" \
    --k8s.secret-name="${REMOTE_SIGNER_RPC_SECRETS_NAME}" \
    --k8s.secret-key-name=tls.cert > "${REMOTE_SIGNER_RPC_SECRETS_DIR}/tls.cert"
  lndinit -v load-secret \
    --source=k8s \
    --k8s.base64 \
    --k8s.namespace="${REMOTE_SIGNER_RPC_SECRETS_NAMESPACE}" \
    --k8s.secret-name="${REMOTE_SIGNER_RPC_SECRETS_NAME}" \
    --k8s.secret-key-name=admin.macaroon > "${REMOTE_SIGNER_RPC_SECRETS_DIR}/admin.macaroon"
fi

# In case we want to upload the TLS certificate and macaroons to k8s secrets as
# well once lnd is ready, we can use the wait-ready and store-secret commands in
# combination to wait until lnd is ready and then batch upload the files to k8s.
if [[ "${UPLOAD_RPC_SECRETS}" == "1" ]]; then
  echo ""
  echo "[STARTUP] Starting RPC secret uploader process in background"
  lndinit -v wait-ready \
    && lncli --network "${NETWORK}" --rpcserver "${RPC_SERVER}" \
    --tlscertpath "${CERT_DIR}/tls.cert" \
    --macaroonpath "${WALLET_DIR}/walletkit.macaroon" \
    wallet accounts list > /tmp/accounts.json \
    && lndinit -v store-secret \
    --batch \
    --overwrite \
    --target=k8s \
    --k8s.base64 \
    --k8s.namespace="${RPC_SECRETS_NAMESPACE}" \
    --k8s.secret-name="${RPC_SECRETS_NAME}" \
    "${CERT_DIR}/tls.cert" \
    "${WALLET_DIR}"/*.macaroon \
    /tmp/accounts.json &
fi

# And finally start lnd. We need to use "exec" here to make sure all signals are
# forwarded correctly.
echo ""
echo "[STARTUP] Starting lnd with flags: $@"
exec lnd "$@"
