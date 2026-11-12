# lnd regression test

Every lnd-based `lightninglabs/lndinit` image that `docker.yml` publishes,
release tags and the nightly build alike, is run through this test by the
`regression` job before the workflow goes green. The job creates a sandbox
namespace on the staging EKS cluster through lightning-operator's
`Environment` CRD, deploys a three-node regtest network running the
published image, measures throughput, soaks it under continuous payment load
for an hour, judges the run, and deletes the sandbox. Everything lives in this
directory; the workflow in `.github/workflows/regression.yml` only sequences
the phases of `run.sh`.

## What a run does

| Phase | What happens | Time |
|---|---|---|
| `setup` | apply an `Environment` in the `lnd-regression` namespace, wait for `Ready`, mint a token for the runner ServiceAccount the Environment bound inside the sandbox, wait for the runner's sandbox permissions | 1 min |
| `scenario` | apply `manifests/scenario.yaml`: regtest `BitcoinNode` mining every 30s, `LNDNode` alice (SQLite, native SQL), bob (bbolt) and carol (SQLite via kvdb) on the image under test, balanced channels alice - bob - carol, and two continuous `PaymentGenerator`s (3 workers each, about 1 payment/s): alice and bob directly, alice and carol forwarded through bob. Wait until every node is Running and synced, every channel Open and both generators have settled a payment; record startup and channel open times | 5 to 15 min |
| `bench` | after a 5 minute warm-up, run a batch `PaymentGenerator` that sends 1500 payments with no delay at concurrency 4, three times; the median payments/s is the throughput number | 5 to 10 min |
| `soak` | continuous load for 60 minutes with a status snapshot every minute; at the end scrape lnd's own exporter and lndmon from each node pod and the soak runner's own metrics from its pod, all through the apiserver proxy, and scan logs for panics | 60 min |
| `gate` | compute the functional and performance signals, compare with `thresholds.json` and `baseline.json`, write `result.json`, fail the job on any breach | 1 min |
| `teardown` | delete the `Environment`, which deletes the sandbox namespace and everything in it; runs on success, failure and cancellation | 1 to 5 min |

Thresholds and load settings live in `thresholds.json`; the job summary and
`result.json` report the values applied. Nightly runs use the wider
performance bands under `performance.nightly`.

### Functional gate

The run fails on any of: payment failure rate above 1% after warm-up (with
at least 100 attempts, direct and forwarded payments together), no payment
settled for 10 minutes, a node out of
chain sync or not Running for more than 2 minutes, any channel closing
(cooperative, force, breach, failed or abandoned), any lnd pod restart, any
panic in lnd logs, the soak generator's counters going backwards (its runner
pod was replaced), or the soak generator not Running at the end. Snapshots
with an Inactive channel are counted and reported but do not fail the run.

### Performance gate

| Metric | Source |
|---|---|
| `throughput_pps` | median of the benchmark runs, payments per second, timed by the runner container's own lifetime (started to finished) so pod scheduling and image pull do not count; the soak generators keep running meanwhile, so this is a lower bound |
| `payment_latency_p95_seconds` | the soak runner's `lightning_payment_runner_payment_duration_seconds`, successful payments in both directions, when its image exports metrics (the lightning-operator v0.2.1 runner or later); otherwise lnd's exporter, `grpc_server_handling_seconds` for `SendPaymentV2` (all outcomes; the histogram has no status label), worst node |
| `invoice_latency_p95_seconds` | the runner's `lightning_payment_runner_invoice_duration_seconds` (`AddInvoice` as the client sees it); otherwise lnd's histogram for `AddInvoice` (the receiver's database write path), worst node |
| `startup_seconds` | apply to Running and synced, worst node; `startup_seconds_lndmon` is lndmon's unlock plus start time |
| `rss_bytes_max` | `process_resident_memory_bytes` at the end of the soak, worst node |

`result.json` records where the two latency numbers came from
(`performance.latency_source`, `runner` or `lnd`); they are only judged
against a baseline recorded from the same source, so a runner image bump
cannot fail a run on its own.

Each metric is compared with `baseline.json`. A metric more than the
configured percentage worse than baseline is a breach. A metric with no
baseline value is recorded only, so the first runs after a change cannot fail
on performance. The baseline is the last green run on a final lnd release:
download that run's `result.json` artifact, run
`.github/regression/run.sh baseline result.json`, and open a PR with the new
`baseline.json`, so every baseline move is reviewed.

## Reading a result

The regression job's summary on the lndinit workflow run shows pass or fail,
the image, the sandbox namespace, every breached check, the functional and
performance tables and the full `result.json`. The same files are attached as
the `lnd-regression-<tag>` artifact for 90 days: `result.json`,
`timings.json`, `bench.json`, `snapshots.jsonl` and the end-of-run
`metrics/`. While a run is live, the sandbox namespace
`lnd-regression-run-<run id>-<attempt>` is visible on the cluster's
dashboards when filtered by namespace, since the nodes' lndmon
ServiceMonitors carry the label the cluster's metrics agent selects on.

## Running by hand

For a tag that is already on Docker Hub: Actions, "Regression test", "Run
workflow", enter the tag (`v0.1.38-beta-lnd-v0.21.6-beta` or
`daily-testing-20261104`). `soak_minutes` shortens the soak for a smoke run.

Against any cluster with a kubeconfig that can manage Environments in the
home namespace:

```bash
export KUBECONFIG=~/.kube/staging
export IMAGE_TAG=v0.1.38-beta-lnd-v0.21.6-beta KIND=release
SOAK_MINUTES=10 .github/regression/run.sh all
```

`all` tears down on exit. Phases can also be run one at a time; state is
kept in `WORKDIR` (default `./.regression-run`).

## Aborting a run

Cancel the workflow run; the teardown step runs under `always()` and deletes
the Environment. If a runner is lost, the Environment stays until the
in-cluster sweeper removes any `Environment` in the home namespace older
than three hours. To delete one by hand:

```bash
kubectl -n lnd-regression get environments
kubectl -n lnd-regression delete environment run-<id>-<attempt>
```

## Cluster prerequisites

Set up once on the cluster, outside this repository.

1. **lightning-operator** at a version that ships the `Environment` CRD
   (v0.2.1 or later).
2. **Home namespace** `lnd-regression` with the ServiceAccount
   `lnd-regression-runner`, which each Environment binds inside its sandbox,
   and a `ResourceQuota` capping how many Environments exist at once.
3. **Roles.** The Environment binds the operator's own per-CRD admin roles
   for BitcoinNode, LNDNode, Channel and PaymentGenerator to the runner
   (`ENVIRONMENT_ROLES` in `run.sh`; the operator can only bind roles it
   ships). The scenario also creates the bitcoind RPC `Secret`, and the gate
   reads `pods`, `pods/log` and `pods/proxy`. Those come from a ClusterRole
   bound to the runner in every namespace labelled
   `config.lightning.engineering/environment-namespace=lnd-regression`,
   which the operator puts on each sandbox it creates from the home
   namespace. `setup` waits for that binding before the scenario is applied
   and fails if it does not appear.
4. **CI identity.** An IAM user whose EKS access entry maps to a Kubernetes
   user with a binding in `lnd-regression` allowing
   `environments.config.lightning.engineering` (get, list, watch, create,
   update, patch, delete), `serviceaccounts` get and `serviceaccounts/token`
   create on `lnd-regression-runner`. Its keys are the repository secrets
   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`; the repository secrets
   `AWS_REGION` and `EKS_CLUSTER_NAME` name the cluster (masked in logs).
   The job never holds any other cluster
   credential; inside the sandbox it acts as the runner ServiceAccount
   through a four-hour token.
5. **Nodes.** Sandbox pods cannot carry tolerations (operator admission
   policy), so they need untainted nodes; staging's general node groups
   are.
6. **Sweeper.** A CronJob in the home namespace that deletes any Environment
   there older than three hours, for runs whose GitHub runner was lost
   before its own teardown step ran. The namespace is dedicated to these
   runs, so an Environment created by hand for debugging goes the same way.
7. **Optional.** The repository variable `LND_REGRESSION_GRAFANA_URL` with
   `{namespace}` in it is rendered into the summary as a link to the run's
   dashboards.
