# lndinit Vault Integration — Design & Plan (KV v2 variant)

Status: reviewed — decisions locked, ready to execute
Author: (design pass)
Related: [lightninglabs/lndinit#62](https://github.com/lightninglabs/lndinit/pull/62) (to be superseded)

### Locked decisions

1. **Scope:** `lndinit` Go change **and** the `lightning-infra` wiring (policy,
   k8s auth role, lnd chart init-script) — two PRs, one per repo.
2. **PR strategy:** open a **fresh PR** that supersedes #62; close #62.
3. **KV version:** **v2 only.** Drop PR #62's v1 behavior; no version toggle.
4. **Auth:** use the official `vault/api/auth/kubernetes` helper (mount +
   audience configurable).
5. **Architecture:** Option A — direct-to-Vault for both read and write; the
   seed/password never become a k8s Secret.

Still open (rollout detail, decide before step 5 of §11): whether to migrate
existing k8s-Secret seeds into Vault or adopt Vault only for new nodes.

## 1. Purpose

PR #62 adds a `vault` secret source/target to `lndinit` (init-wallet,
store-secret, load-secret). It is a straight port of an old branch from
Oliver's `lnd` fork and was written against a **KV v1** Vault with a
hand-rolled Kubernetes login. Our production Vault in `lightning-infra` looks
different today. This doc specifies a **new variant** of the Vault integration
that targets the Vault we actually run now, so that `lnd` pods can keep their
wallet seed and password in Vault (never in an etcd-backed k8s Secret).

## 2. What PR #62 does today

- New `vault.go` with `getClientVault` / `readVault` / `saveVault` /
  `storeSecretVault`, mirroring the existing `k8s.go` shape.
- Wires a third choice into three commands: `init-wallet --secret-source=vault`,
  `store-secret --target=vault`, `load-secret --source=vault`.
- Auth: hand-rolled `POST /v1/auth/kubernetes/login` with the pod's
  ServiceAccount JWT + a role, then sets the returned client token.
- Secret access: `client.Logical().Read(name)` and
  `client.Logical().Write(name, flatMap)`, reading `secret.Data[key]` directly.
- Config: `api.DefaultConfig()` (reads `VAULT_ADDR` and friends from env).
- Adds `example-init-wallet-vault.sh` mirroring the k8s example.

## 3. What our Vault actually is (from `lightning-infra`)

Concrete facts that drive the design (evidence in `lightning-infra`):

- **Engine: KV v2**, mount `secret/`. Values live at `secret/data/<path>`,
  metadata at `secret/metadata/<path>`. The infra provisioner uses
  `client.KVv2("secret")` (`containers/vault-github-provisioner/main.go`), and
  VSO manifests declare `type: kv-v2`, `mount: secret`.
- **Auth: Kubernetes auth method** at mount `auth/kubernetes`; roles bind a
  ServiceAccount + namespace to a policy, with token `audiences: ["vault"]`
  (see `charts/*/templates/static-auth.yaml`,
  `charts/vault-secrets-operator/values-*.yaml`). GitHub auth exists too, but
  that is for humans, not workloads.
- **Server address (in-cluster):** `http://vault.vault.svc.cluster.local:8200`
  (`defaultVaultConnection` in the VSO values). Ingress URLs per env exist for
  humans.
- **VSO is the standard read path** for most workloads: `VaultAuth` +
  `VaultStaticSecret` sync a Vault path into a k8s Secret. VSO is read-only and
  cannot generate/write secrets.
- **Current lnd flow does NOT use Vault.** `lnd` charts run `lndinit
  gen-password` / `gen-seed` and `store-secret --target=k8s`, then auto-unlock
  by reading the password back from the k8s Secret through a FIFO. The seed and
  password therefore sit in etcd as a k8s Secret.
- **Deploy model:** GitOps via ArgoCD; changes land through
  `charts/<name>/values-<env>.yaml` and app-info files.

## 4. Gap analysis: PR #62 vs. our Vault

| Concern | PR #62 | Our Vault | Consequence |
|---|---|---|---|
| KV version | v1 (`Logical().Read/Write`, flat `Data`) | **v2** (nested `data`, `/data/` path) | PR #62 reads/writes the wrong shape; a v2 read returns `{data:{...}, metadata:{...}}`, so `Data[key]` is nil. **Must fix.** |
| Auth | hand-rolled HTTP login | `auth/kubernetes`, mount configurable, audiences `["vault"]` | Works, but brittle; no mount/audience config. Prefer official helper. |
| Path model | single `--secret-name` used as literal path | mount + KV path + key | Need explicit mount + path + key fields. |
| Write semantics | read-modify-`Write` whole map | KV v2 `Patch`/`Put` | Use KVv2 helper to avoid clobbering sibling keys. |
| Server addr | env only | in-cluster DNS default | Keep env-driven; optionally allow `--vault.addr`. |

## 5. Scope & architecture decision

The store/generate path (gen-seed → persist) **must** talk to Vault directly —
VSO cannot write. So this remains a direct-to-Vault feature in `lndinit`. The
only real fork is the *consume* (auto-unlock) path:

- **Option A — direct Vault for read and write (recommended).** `lndinit`
  authenticates via k8s auth and reads the seed/password directly at init and
  unlock time. The seed **never** materializes as a k8s Secret. This is the
  security win and the natural modernization of PR #62.
- **Option B — VSO for the read path.** VSO syncs `secret/data/lnd/<node>` into
  a k8s Secret; `lndinit` keeps `--secret-source=k8s` for unlock. Less code, but
  the seed lands in etcd, defeating much of the point. Store path still needs
  direct Vault.

This plan assumes **Option A**.

## 6. Proposed CLI surface

Rework `vaultSecretOptions` (shared by all three commands) to KV v2 + explicit
auth config. Field/flag names mirror the k8s source where possible.

```
# Connection & auth (shared)
--vault.addr                 (optional; else VAULT_ADDR / api.DefaultConfig)
--vault.auth-mount=kubernetes
--vault.auth-role=<role>                (required)
--vault.auth-token-path=/var/run/secrets/kubernetes.io/serviceaccount/token

# KV v2 location (shared)
--vault.kv-mount=secret
--vault.secret-path=lnd/<node>/wallet   (KV path, WITHOUT mount or /data/)
--vault.secret-key-name=<key>           (store-secret / load-secret)

# init-wallet only (multiple keys within one secret)
--vault.seed-key-name=walletseed
--vault.seed-passphrase-key-name=<opt>
--vault.wallet-password-key-name=walletpassword
```

Notes:
- Rename PR #62's `--vault.secret-name` → `--vault.secret-path` to reflect KV v2
  path semantics, and add `--vault.kv-mount` / `--vault.auth-mount`.
- **Audience** is intentionally *not* a flag: the token audience is a property
  of the projected ServiceAccount token the pod mounts (set in the pod spec's
  `serviceAccountToken` projection and matched by the Vault role), so
  `--vault.auth-token-path` just points at that token. The official k8s auth
  helper exposes no client-side audience option.
- `main.go`: keep `storageVault = "vault"`; add `vault` to the `choice:` lists
  on `--secret-source`, `--source`, `--target`.

## 7. Go implementation plan (`vault.go` rewrite)

Dependencies (add to `go.mod`):
- `github.com/hashicorp/vault/api` (already pulled by PR #62).
- `github.com/hashicorp/vault/api/auth/kubernetes` (small helper module) — for
  `NewKubernetesAuth(role, WithMountPath(...), WithServiceAccountTokenPath(...))`.

Client construction:
```go
cfg := api.DefaultConfig()          // VAULT_ADDR, TLS, etc. from env
if opts.Addr != "" { cfg.Address = opts.Addr }
client, _ := api.NewClient(cfg)

k8sAuth, _ := auth.NewKubernetesAuth(
    opts.AuthRole,
    auth.WithMountPath(opts.AuthMount),                 // default "kubernetes"
    auth.WithServiceAccountTokenPath(opts.AuthTokenPath),
)
if _, err := client.Auth().Login(ctx, k8sAuth); err != nil { ... }
```
(If we prefer zero extra modules, keep PR #62's hand-rolled login but make the
mount path configurable. Recommendation: use the official helper.)

Read (`readVault`) — KV v2:
```go
kv := client.KVv2(opts.KVMount)                 // default "secret"
sec, err := kv.Get(ctx, opts.SecretPath)        // handles /data/ automatically
raw, ok := sec.Data[opts.SecretKeyName].(string)
// not-found and empty-entry errors as today; TrimRight "\r\n" as PR #62 does.
```

Write (`storeSecretVault` / `saveVault`) — KV v2, no clobber:
```go
kv := client.KVv2(opts.KVMount)
existing, err := kv.Get(ctx, opts.SecretPath)   // errors.Is(err, ErrSecretNotFound) -> empty
data := existing.Data (or {})                   // read-modify-write merge
if data[key] != nil && !overwrite { return errTargetExists }
data[key] = content
kv.Put(ctx, opts.SecretPath, data)              // full write of merged map
```
- Read-modify-`Put` (rather than `Patch`) so the lnd policy only needs
  `create/read/update` on `secret/data/lnd/*` — `Patch` would require the
  separate `patch` capability.
- `errTargetExists` sentinel is preserved so `-e/--error-on-existing` and the
  existing exit-code contract keep working (see `main.go`).
- `jsonVaultObject` for `load-secret --output=json` can be sourced from
  `sec.Raw` (request_id, lease_*, renewable) to keep parity with PR #62.

`init-wallet` `readInput` switch: the `case storageVault:` block from PR #62
stays structurally the same, just pointing at the new options
(seed/passphrase/password keys under one `--vault.secret-path`).

## 8. `lightning-infra` wiring (companion changes)

These are the infra-side pieces that make Option A run (separate PR in
`lightning-infra`, GitOps):

1. **Vault policy** (e.g. `lnd`), KV v2 capabilities scoped to lnd paths:
   ```hcl
   path "secret/data/lnd/*"     { capabilities = ["create", "read", "update"] }
   path "secret/metadata/lnd/*" { capabilities = ["read"] }
   ```
2. **k8s auth role** binding the lnd ServiceAccount(s)/namespace(s) to the
   policy:
   ```
   vault write auth/kubernetes/role/lnd \
     bound_service_account_names=<lnd-sa> \
     bound_service_account_namespaces=<ns...> \
     token_policies=lnd token_ttl=1h
   ```
   Fold this into the vault-provisioner setup job pattern rather than by hand.
3. **lnd chart**: switch `configmap-init-script.yaml` from the `--target=k8s`
   calls to the `vault` variant (`store-secret --target=vault`,
   `init-wallet --secret-source=vault`, `load-secret --source=vault`), and set
   env: `VAULT_ADDR=http://vault.vault.svc.cluster.local:8200`,
   `VAULT_ROLE=lnd`, `VAULT_SECRET_PATH=lnd/<node>/wallet`. Project the SA token
   with `audience: vault` (or rely on the default projected token if the role
   accepts it).
4. **Image**: bump the `lndinit` image tag once the new binary is released
   (the lnd chart already uses an `lndinit`-bundled image).

## 9. Security considerations

- Seed/password never become a k8s Secret under Option A — they live only in
  Vault and transit the pod as an in-memory value / FIFO, matching the existing
  auto-unlock FIFO trick.
- Least-privilege policy: lnd role gets `create/read/update` on `secret/data/lnd/*`
  only; no `delete`, no cross-namespace paths.
- Token audience pinned to `vault`; SA-token path configurable for non-default
  projections.
- Idempotency: `errTargetExists` + `-e` preserves "assert exists, don't
  overwrite" semantics so a restart won't rotate the seed. **Critical**: seed
  overwrite must never happen silently on an existing wallet.

## 10. Testing plan

- **Unit**: table tests for KV v2 read/merge-write shaping against a stubbed
  `Logical()`/httptest Vault (mirror `k8s_test.go` style). Cover: missing
  secret, missing key, empty value, overwrite vs. no-overwrite (`errTargetExists`),
  trailing-newline trim.
- **Integration (manual/dev)**: dev-mode Vault (`vault server -dev`) with KV v2
  + k8s auth stubbed via token; run the full `example-init-wallet-vault.sh`
  against it.
- **regtest e2e**: in a kind/dev cluster with the real Vault chart, run the lnd
  init script end-to-end (gen → store → init → unlock).
- Keep the k8s path tests green; the vault path is additive.

## 11. Rollout / migration

1. Land `lndinit` Go change (KV v2 + k8s auth helper), cut a release + image.
2. Land `lightning-infra` policy + role (provisioner) — no workload impact yet.
3. Flip one **staging** lnd node's init script to the vault variant; verify
   clean init and unlock across a pod restart.
4. Promote to testnet, then prod, per the usual ArgoCD env progression.
5. For existing nodes with a seed already in a k8s Secret: one-time migrate by
   `store-secret --target=vault` from the current values, verify, then remove
   the k8s Secret. (Or leave existing nodes as-is and only use Vault for new
   nodes — TBD, see open questions.)

## 12. Execution order

Two PRs, `lndinit` first (it produces the binary/image the infra depends on):

**PR 1 — `lndinit` (this repo), fresh branch superseding #62:**
1. Add deps: `vault/api`, `vault/api/auth/kubernetes`.
2. Rewrite `vault.go`: KV v2 (`client.KVv2`) read + merge-write, official k8s
   auth login, new `vaultSecretOptions` (mount/path/auth-mount/role/audiences).
3. Update `main.go` `choice:` tags to include `vault` on the three commands.
4. Update `cmd_init_wallet.go` / `cmd_load_secret.go` / `cmd_store_secret.go`
   `vault` cases to the new options.
5. Rewrite `example-init-wallet-vault.sh` for KV v2 flags + `VAULT_ADDR`.
6. Unit tests (`vault_test.go`) mirroring `k8s_test.go`.
7. README: document the `vault` source/target.
8. Open fresh PR; close #62 with a pointer.

**PR 2 — `lightning-infra` (companion, after image is cut):**
1. Vault policy `lnd` (`secret/data/lnd/*` create/read/update) via the
   provisioner setup-job pattern.
2. k8s auth role `lnd` binding lnd SA + namespace(s) → policy.
3. lnd chart: init-script → vault variant; env `VAULT_ADDR`, `VAULT_ROLE`,
   `VAULT_SECRET_PATH`; SA token `audience: vault`; bump `lndinit` image.
4. Roll staging → testnet → prod per §11.

Still open (decide before §11 step 5): existing-node seed migration vs.
Vault-only-for-new-nodes.
