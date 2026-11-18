#!/usr/bin/env bash
# run.sh drives one lnd regression run against a cluster that runs
# lightning-operator with the Environment CRD. The regression workflow calls
# the phases as separate steps; by hand, `run.sh all` runs them in order:
#
#   cluster   write the CI kubeconfig from AWS credentials (CI only)
#   setup     create the Environment, wait for Ready, mint the sandbox token
#   scenario  apply the network manifest and wait until it is healthy
#   bench     throughput benchmark, repeated, median kept
#   soak      continuous load for SOAK_MINUTES with a snapshot every minute
#   gate      judge the run, write result.json, exit 1 on a failure
#   teardown  delete the Environment (always run, also on failure)
#   summary   render result.json into the GitHub job summary
#   baseline  copy a result's performance numbers into baseline.json
#
# Inputs are environment variables; see README.md for the full list. State
# lives in WORKDIR so each phase can run in its own step or shell.

set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$here/lib.sh"

phase=${1:-}
[[ -n "$phase" ]] || die "usage: run.sh <cluster|setup|scenario|bench|soak|gate|teardown|summary|all|baseline <result.json>>"

: "${IMAGE:=lightninglabs/lndinit}"
: "${KIND:=release}"
: "${HOME_NS:=lnd-regression}"
: "${RUNNER_SA:=lnd-regression-runner}"
# ClusterRoles the Environment binds to the runner inside the sandbox: the
# operator's own per-CRD admin roles for what the scenario creates (the
# operator can only bind roles it ships). The bitcoind Secret and the pod
# reads the runner also needs are bound outside the Environment; see README.md.
: "${ENVIRONMENT_ROLES:=lightning-operator-layer1-bitcoinnode-admin-role,lightning-operator-layer2-lndnode-admin-role,lightning-operator-layer2-channel-admin-role,lightning-operator-layer2-paymentgenerator-admin-role}"
: "${RUN_URL:=}"
: "${WORKDIR:=$PWD/.regression-run}"
: "${CI_KUBECONFIG:=${KUBECONFIG:-$HOME/.kube/config}}"
thresholds="$here/thresholds.json"
baseline="$here/baseline.json"
# Fully qualified resource names, so a same-named CRD from another operator
# on a shared cluster can never be picked up.
ENVIRONMENTS=environments.config.lightning.engineering
BITCOINNODES=bitcoinnodes.layer1.lightning.engineering
LNDNODES=lndnodes.layer2.lightning.engineering
CHANNELS=channels.layer2.lightning.engineering
PAYMENTGENERATORS=paymentgenerators.layer2.lightning.engineering
mkdir -p "$WORKDIR/rendered" "$WORKDIR/metrics"
state="$WORKDIR/env.sh"
[[ -f "$state" ]] && source "$state"
SANDBOX_KUBECONFIG="$WORKDIR/sandbox.kubeconfig"

save_state() {
	{
		echo "RUN_NAME=${RUN_NAME}"
		echo "SANDBOX_NS=${SANDBOX_NS}"
		echo "IMAGE_TAG=${IMAGE_TAG}"
		echo "LND_VERSION=${LND_VERSION:-}"
		echo "KIND=${KIND}"
		echo "CI_KUBECONFIG=${CI_KUBECONFIG}"
	} > "$state"
}

threshold() { jq -r "$1" "$thresholds"; }

# lnd_nodes and continuous_generators list what the sandbox holds, so the
# phases follow the manifest rather than naming its objects.
lnd_nodes() { ks get "$LNDNODES" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null; }
continuous_generators() { ks get "$PAYMENTGENERATORS" -o json 2>/dev/null | jq -r '.items[] | select(.spec.mode == "continuous") | .metadata.name'; }

# ---------------------------------------------------------------- cluster
phase_cluster() {
	: "${EKS_CLUSTER_NAME:?}" "${AWS_REGION:?}"
	CI_KUBECONFIG="$WORKDIR/ci.kubeconfig"
	aws eks update-kubeconfig --name "$EKS_CLUSTER_NAME" \
		--region "$AWS_REGION" --kubeconfig "$CI_KUBECONFIG" >/dev/null
	chmod 600 "$CI_KUBECONFIG"
	echo "CI_KUBECONFIG=${CI_KUBECONFIG}" > "$state"
}

# ------------------------------------------------------------------ setup
phase_setup() {
	: "${IMAGE_TAG:?}"
	: "${RUN_NAME:=run-$(date -u +%Y%m%d%H%M%S)}"
	SANDBOX_NS="${HOME_NS}-${RUN_NAME}"
	(( ${#SANDBOX_NS} <= 63 )) || die "sandbox namespace name too long: $SANDBOX_NS"
	local roles
	roles=$(jq -cn --arg r "$ENVIRONMENT_ROLES" '$r | split(",")')
	export RUN_NAME SANDBOX_NS IMAGE_TAG KIND HOME_NS RUNNER_SA ROLES_JSON="$roles"
	render "$here/manifests/environment.yaml" > "$WORKDIR/rendered/environment.yaml"
	kc apply -f "$WORKDIR/rendered/environment.yaml"
	save_state

	env_phase() {
		local p
		p=$(kc get "$ENVIRONMENTS" "$RUN_NAME" -n "$HOME_NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)
		if [[ "$p" == Failed ]]; then
			kc get "$ENVIRONMENTS" "$RUN_NAME" -n "$HOME_NS" -o jsonpath='{.status.conditions}' >&2
			die "Environment $RUN_NAME is Failed"
		fi
		[[ "$p" == Ready ]]
	}
	wait_for 300 "Environment $RUN_NAME to be Ready" env_phase
	log "sandbox namespace $SANDBOX_NS is ready"

	# Act inside the sandbox as the ServiceAccount the Environment bound, so
	# the CI identity itself never needs rights in a namespace that did not
	# exist a minute ago.
	local token server ca
	token=$(kc create token "$RUNNER_SA" -n "$HOME_NS" --duration=4h)
	[[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::add-mask::${token}"
	server=$(kc config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')
	ca=$(kc config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
	umask 077
	cat > "$SANDBOX_KUBECONFIG" <<-KC
	apiVersion: v1
	kind: Config
	clusters:
	- name: sandbox
	  cluster:
	    server: ${server}
	    certificate-authority-data: ${ca}
	users:
	- name: ${RUNNER_SA}
	  user:
	    token: ${token}
	contexts:
	- name: sandbox
	  context:
	    cluster: sandbox
	    user: ${RUNNER_SA}
	    namespace: ${SANDBOX_NS}
	current-context: sandbox
	KC
	ks auth can-i create "$LNDNODES" >/dev/null ||
		die "runner ServiceAccount cannot create LNDNodes in $SANDBOX_NS; check the Environment's roles"
	# The Secret and pod permissions are bound by the cluster's RBAC tooling
	# as the sandbox namespace appears, a moment after the Environment is
	# Ready.
	secrets_ok() { ks auth can-i create secrets >/dev/null 2>&1; }
	wait_for 120 "the sandbox role to be bound (create secrets)" secrets_ok
}

# --------------------------------------------------------------- scenario
phase_scenario() {
	: "${SANDBOX_NS:?run setup first}"
	export BITCOIND_RPC_PASSWORD
	BITCOIND_RPC_PASSWORD=$(openssl rand -hex 16)
	export IMAGE IMAGE_TAG SANDBOX_NS
	render "$here/manifests/scenario.yaml" > "$WORKDIR/rendered/scenario.yaml"
	# The rendered file carries the RPC password; it is only kept locally.
	local t0
	t0=$(now_s)
	ks apply -f "$WORKDIR/rendered/scenario.yaml"
	local ready_timeout=$(( $(threshold .readyTimeoutMinutes) * 60 ))

	# Wait for whatever the manifest declared to converge: every bitcoin
	# node ready, every lnd node Running and synced, every channel Open,
	# every continuous generator Running, which means its first payment
	# settled. Names come from the sandbox, so the manifest can change
	# without this script changing.
	rm -f "$WORKDIR"/ready-*.json
	wait_all "$BITCOINNODES" '.status.ready == true' bitcoind ready
	wait_all "$LNDNODES" '.status.phase == "Running" and .status.syncedToChain == true' lndnode "Running and synced"
	wait_all "$CHANNELS" '.status.phase == "Open"' channel Open
	wait_all "$PAYMENTGENERATORS" '.spec.mode != "continuous" or .status.phase == "Running"' generator Running

	jq -n \
		--arg applied_at "$(date -u -d "@$t0" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$t0" +%Y-%m-%dT%H:%M:%SZ)" \
		--slurpfile btc "$WORKDIR/ready-bitcoind.json" \
		--slurpfile nodes "$WORKDIR/ready-lndnode.json" \
		--slurpfile channels "$WORKDIR/ready-channel.json" \
		--slurpfile generators "$WORKDIR/ready-generator.json" \
		'{applied_at: $applied_at,
		  bitcoind_ready_seconds: ([$btc[0][]] | max),
		  node_startup_seconds: $nodes[0],
		  channel_open_seconds: $channels[0],
		  first_payment_seconds: ([$generators[0][]] | max),
		  soak_running_at: now | todate}' > "$WORKDIR/timings.json"
	log "scenario healthy: $(jq -c . "$WORKDIR/timings.json")"
}

# wait_all <resource> <jq condition> <label> <state> polls the sandbox until
# every object of the resource satisfies the condition, logging each one as
# it gets there and recording its seconds since apply by name in
# $WORKDIR/ready-<label>.json. Uses t0 and ready_timeout of the caller.
wait_all() {
	local resource=$1 cond=$2 label=$3 state_desc=$4
	local ready="$WORKDIR/ready-$label.json" deadline=$(( $(now_s) + ready_timeout ))
	echo '{}' > "$ready"
	while :; do
		local items elapsed n
		items=$(ks get "$resource" -o json 2>/dev/null | jq -c "[.items[] | {name: .metadata.name, ok: (($cond) // false)}]" 2>/dev/null) || items='[]'
		elapsed=$(( $(now_s) - t0 ))
		for n in $(jq -r --slurpfile seen "$ready" '.[] | select(.ok) | .name | select(in($seen[0]) | not)' <<<"$items"); do
			jq --arg n "$n" --argjson s "$elapsed" '.[$n] = $s' "$ready" > "$ready.tmp" && mv "$ready.tmp" "$ready"
			log "$label/$n $state_desc after ${elapsed}s"
		done
		[[ "$(jq 'all(.[]; .ok)' <<<"$items")" == true ]] && return 0
		if (( $(now_s) > deadline )); then
			die "timed out after ${ready_timeout}s waiting for every $label to be $state_desc; not there: $(jq -r '[.[] | select(.ok | not) | .name] | join(", ")' <<<"$items")"
		fi
		sleep 10
	done
}

# ------------------------------------------------------------------ bench
# snapshot_soak prints the counters of the continuous generators, the ones
# that carry the soak, as one JSON object with the counts summed; phase is
# Running only when all of them are. Prints null when nothing can be read,
# so a transient API error never aborts a phase; the gate treats null as
# "no reading".
snapshot_soak() {
	local out
	out=$(ks get "$PAYMENTGENERATORS" -o json 2>/dev/null | jq -c '
		[.items[] | select(.spec.mode == "continuous")] |
		if length == 0 then null else {
		  phase: (if all(.[]; .status.phase == "Running") then "Running" else ([.[] | .status.phase // "unknown"] | unique | join(",")) end),
		  total: ([.[] | .status.totalPayments // 0] | add),
		  succeeded: ([.[] | .status.succeededPayments // 0] | add),
		  failed: ([.[] | .status.failedPayments // 0] | add),
		  lastPaymentTime: ([.[] | .status.lastPaymentTime // empty] | max),
		  flapping: any(.[]; .status.flapping == true),
		  consecutiveFailures: ([.[] | .status.consecutiveFailures // 0] | max),
		  succeeding: all(.[]; .status.succeeding == true),
		  lastError: ([.[] | .status.lastError // empty] | last)} end' 2>/dev/null) || true
	echo "${out:-null}"
}

phase_bench() {
	: "${SANDBOX_NS:?run setup first}"
	local warm=${WARMUP_MINUTES:-$(threshold .warmupMinutes)}
	local soak_at
	soak_at=$(iso_to_s "$(jq -r .soak_running_at "$WORKDIR/timings.json")")
	local until=$(( soak_at + warm * 60 ))
	if (( $(now_s) < until )); then
		log "warm-up: waiting $(( until - $(now_s) ))s"
		sleep $(( until - $(now_s) ))
	fi
	snapshot_soak > "$WORKDIR/soak-at-warmup-end.json"

	local payments concurrency reps timeout
	payments=$(threshold .bench.payments)
	concurrency=$(threshold .bench.concurrency)
	reps=$(threshold .bench.repetitions)
	timeout=$(( $(threshold .bench.timeoutMinutes) * 60 ))
	export SANDBOX_NS BENCH_PAYMENTS=$payments BENCH_CONCURRENCY=$concurrency
	: > "$WORKDIR/bench-runs.jsonl"
	for i in $(seq 1 "$reps"); do
		export BENCH_NAME="bench-$i"
		render "$here/manifests/bench.yaml" > "$WORKDIR/rendered/$BENCH_NAME.yaml"
		# The throughput clock is the runner container's own lifetime,
		# started to finished, read from its pod after it exits: a second
		# of connect overhead, but no scheduling or image pull, and no
		# dependence on when this poll happened to look. The polled
		# first-payment-to-done time is the fallback when the pod cannot
		# be read; the apply-to-done time is kept as well.
		local t0 t1 t_run=0 phase
		t0=$(now_s)
		ks apply -f "$WORKDIR/rendered/$BENCH_NAME.yaml" >/dev/null
		bench_done() {
			phase=$(jget "$PAYMENTGENERATORS/$BENCH_NAME" '{.status.phase}')
			if (( t_run == 0 )) && [[ "$phase" == Running || "$phase" == Completed || "$phase" == Failed ]]; then
				t_run=$(now_s)
			fi
			[[ "$phase" == Completed || "$phase" == Failed ]]
		}
		wait_for "$timeout" "$BENCH_NAME to finish" bench_done
		t1=$(now_s)
		(( t_run > 0 )) || t_run=$t0
		local status
		status=$(ks get "$PAYMENTGENERATORS/$BENCH_NAME" -o json 2>/dev/null | jq -c '.status' 2>/dev/null) || status=null
		local secs=$(( t1 - t_run )) clock=polled
		local run_secs
		run_secs=$(ks get pods -l "app=payment-runner,paymentgenerator=$BENCH_NAME" -o json 2>/dev/null | jq -r '
			[.items[].status.containerStatuses[]? | .state.terminated | select(. != null)] |
			if length == 0 then empty else
			  ((map(.finishedAt | fromdateiso8601) | max) - (map(.startedAt | fromdateiso8601) | min)) end' 2>/dev/null) || run_secs=""
		if [[ "$run_secs" =~ ^[0-9]+$ ]] && (( run_secs > 0 )); then
			secs=$run_secs; clock=runner
		fi
		# Wait for the delete: the sandbox quota counts generators, and a
		# bench still terminating would block the next apply.
		ks delete "$PAYMENTGENERATORS/$BENCH_NAME" --wait=true --timeout=120s >/dev/null ||
			log "WARNING: $BENCH_NAME is still terminating"
		jq -n --arg name "$BENCH_NAME" --arg phase "$phase" --argjson secs "$secs" \
			--arg clock "$clock" --argjson total_secs "$(( t1 - t0 ))" \
			--argjson payments "$payments" --argjson status "${status:-null}" \
			'{name: $name, phase: $phase, seconds: $secs, clock: $clock,
			  apply_to_done_seconds: $total_secs,
			  payments: $payments,
			  payments_per_second: (if $secs > 0 then ($payments / $secs) else null end),
			  status: $status}' >> "$WORKDIR/bench-runs.jsonl"
		log "$BENCH_NAME: $phase, ${secs}s by the $clock clock ($(( t1 - t0 ))s from apply)"
		[[ "$phase" == Completed ]] || log "WARNING: $BENCH_NAME ended $phase: $(jq -r .lastError <<<"$status")"
	done
	local med
	med=$(jq -r 'select(.phase == "Completed") | .payments_per_second' "$WORKDIR/bench-runs.jsonl" | median)
	jq -n --argjson runs "$(jq -s . "$WORKDIR/bench-runs.jsonl")" --argjson med "${med:-null}" \
		--argjson payments "$payments" --argjson concurrency "$concurrency" \
		'{payments: $payments, concurrency: $concurrency, runs: $runs,
		  completed: ($runs | map(select(.phase == "Completed")) | length),
		  throughput_pps_median: $med}' > "$WORKDIR/bench.json"
	log "benchmark: median $(jq -r .throughput_pps_median "$WORKDIR/bench.json") payments/s"
}

# ------------------------------------------------------------------- soak
# snapshot prints one JSON object with the state of the sandbox. Every read
# falls back to null on error so one API blip never ends the soak; the gate
# ignores null readings.
snapshot() {
	local pods=null nodes=null channels=null
	if can_i list pods; then
		pods=$(ks get pods -l app=lnd -o json 2>/dev/null | jq -c '[.items[] | {name: .metadata.name,
			node: .metadata.labels.node,
			restarts: ([.status.containerStatuses[]?.restartCount] | add // 0)}]' 2>/dev/null) || pods=null
	fi
	nodes=$(ks get "$LNDNODES" -o json 2>/dev/null | jq -c '[.items[] | {name: .metadata.name, phase: .status.phase, synced: (.status.syncedToChain // false), channels: (.status.channels // 0)}]' 2>/dev/null) || nodes=null
	channels=$(ks get "$CHANNELS" -o json 2>/dev/null | jq -c '[.items[] | {name: .metadata.name, phase: .status.phase}]' 2>/dev/null) || channels=null
	jq -n --arg ts "$(iso_now)" \
		--argjson nodes "${nodes:-null}" \
		--argjson channels "${channels:-null}" \
		--argjson soak "$(snapshot_soak)" \
		--argjson pods "${pods:-null}" \
		'{ts: $ts, nodes: $nodes, channels: $channels, soak: $soak, pods: $pods}'
}

phase_soak() {
	: "${SANDBOX_NS:?run setup first}"
	local minutes=${SOAK_MINUTES:-}
	[[ -n "$minutes" ]] || minutes=$(threshold .soakMinutes)
	local every
	every=$(threshold .snapshotSeconds)
	local t0=$(now_s) end=$(( $(now_s) + minutes * 60 ))
	log "soaking for ${minutes} minutes"
	: > "$WORKDIR/snapshots.jsonl"
	while :; do
		local s
		if s=$(snapshot) && [[ -n "$s" ]]; then
			echo "$s" >> "$WORKDIR/snapshots.jsonl"
			log "t+$(( ( $(now_s) - t0 ) / 60 ))m $(jq -c '{soak: {ok: .soak.succeeded, failed: .soak.failed, flapping: .soak.flapping}, synced: [(.nodes // [])[].synced], channels: [(.channels // [])[].phase]}' <<<"$s")"
		else
			log "WARNING: snapshot failed; will retry"
		fi
		(( $(now_s) >= end )) && break
		sleep "$every"
	done
	echo "$(iso_now)" > "$WORKDIR/soak-ended-at"

	# End-of-run metrics straight from the pods through the apiserver proxy
	# (no cluster network path needed): lnd's own exporter on 8989 and lndmon
	# on 8990. Skipped, not failed, when the role does not allow pods/proxy.
	local n
	if can_i list pods && can_i get pods/proxy; then
		for n in $(lnd_nodes); do
			local pod
			pod=$(ks get pods -l "app=lnd,node=$n" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | awk '{print $1}')
			[[ -n "$pod" ]] || { log "WARNING: no lnd pod found for $n"; continue; }
			ks get --raw "/api/v1/namespaces/$SANDBOX_NS/pods/$pod:8989/proxy/metrics" > "$WORKDIR/metrics/$n-lnd.prom" 2>/dev/null ||
				log "WARNING: could not scrape lnd metrics from $pod"
			ks get --raw "/api/v1/namespaces/$SANDBOX_NS/pods/$pod:8990/proxy/metrics" > "$WORKDIR/metrics/$n-lndmon.prom" 2>/dev/null ||
				log "WARNING: could not scrape lndmon metrics from $pod"
		done
		# Each continuous generator's runner serves its own metrics on
		# 9090 (lightning-operator v0.2.1 runner and later): payment and
		# invoice latency as the client sees them. An older runner image
		# listens on nothing there, so a failed scrape only means the
		# gate falls back to lnd's gRPC histograms.
		for n in $(continuous_generators); do
			local rpod
			rpod=$(ks get pods -l "app=payment-runner,paymentgenerator=$n" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | awk '{print $1}')
			if [[ -z "$rpod" ]]; then
				log "WARNING: no runner pod found for generator $n"
			elif ks get --raw "/api/v1/namespaces/$SANDBOX_NS/pods/$rpod:9090/proxy/metrics" > "$WORKDIR/metrics/$n-runner.prom" 2>/dev/null; then
				log "scraped runner metrics from $rpod"
			else
				rm -f "$WORKDIR/metrics/$n-runner.prom"
				log "no runner metrics from $rpod (runner image predates them?); latency comes from lnd's exporter"
			fi
		done
	else
		log "metrics scrape skipped: sandbox role lacks pods/proxy"
	fi
	if can_i list pods && can_i get pods/log; then
		for n in $(lnd_nodes); do
			local pod logs
			pod=$(ks get pods -l "app=lnd,node=$n" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | awk '{print $1}')
			[[ -n "$pod" ]] || { log "WARNING: no lnd pod found for $n"; continue; }
			if logs=$(ks logs "$pod" --all-containers --timestamps --request-timeout=10m 2>/dev/null); then
				grep -c 'panic: ' <<<"$logs" > "$WORKDIR/metrics/$n-panics" || true
			else
				log "WARNING: could not read logs of $pod; panic scan skipped for $n"
			fi
		done
	else
		log "panic scan skipped: sandbox role lacks pods/log"
	fi
}

# ------------------------------------------------------------------- gate
phase_gate() {
	: "${SANDBOX_NS:?run setup first}"
	local snaps="$WORKDIR/snapshots.jsonl"
	[[ -s "$snaps" ]] || die "no snapshots; run soak first"
	local f="$thresholds"
	local max_fail_pct min_payments stall_min sync_min max_restarts max_panics
	max_fail_pct=$(threshold .functional.maxPaymentFailurePct)
	min_payments=$(threshold .functional.minPaymentsForFailureRate)
	stall_min=$(threshold .functional.maxPaymentStallMinutes)
	sync_min=$(threshold .functional.maxChainSyncLossMinutes)
	max_restarts=$(threshold .functional.maxPodRestarts)
	max_panics=$(threshold .functional.maxPanics)
	local every
	every=$(threshold .snapshotSeconds)

	# Functional signals from the snapshots.
	# Readings can be null after an API blip, so take the last snapshot that
	# carries the soak counters, and fall back to the first reading when the
	# warm-up marker is missing.
	local first last warm
	first=$(jq -c 'select(.soak != null)' "$snaps" | head -n1)
	last=$(jq -c 'select(.soak != null)' "$snaps" | tail -n1)
	[[ -n "$last" ]] || die "no snapshot carries soak counters"
	warm=$(cat "$WORKDIR/soak-at-warmup-end.json" 2>/dev/null || true)
	[[ -n "$warm" && "$warm" != null ]] || warm=$(jq -c .soak <<<"$first")
	local functional
	functional=$(jq -n --argjson warm "$warm" --argjson last "$last" --argjson every "$every" \
		--slurpfile snaps "$snaps" '
		($snaps | length) as $n |
		(($last.soak.total // 0) - ($warm.total // 0)) as $attempts |
		(($last.soak.failed // 0) - ($warm.failed // 0)) as $failed |
		{
		  payments_after_warmup: $attempts,
		  failed_after_warmup: $failed,
		  failure_pct: (if $attempts > 0 then ($failed * 100 / $attempts) else null end),
		  max_payment_stall_minutes: (
		    [$snaps[] | select(.soak != null and .soak.lastPaymentTime != null) |
		      ((.ts | fromdateiso8601) - (.soak.lastPaymentTime | fromdateiso8601)) / 60] | max // 0),
		  max_chain_sync_loss_minutes: (
		    # longest run of consecutive snapshots with any node unsynced or not Running
		    [$snaps[] | select(.nodes != null) | ([.nodes[] | (.synced | not) or (.phase != "Running")] | any)] |
		    reduce .[] as $bad ({cur: 0, max: 0};
		      if $bad then .cur += 1 | .max = ([.max, .cur] | max) else .cur = 0 end) |
		    .max * $every / 60),
		  bad_channel_phases: ([$snaps[] | (.channels // [])[] | select(.phase == "Force Closed" or .phase == "Breach Closed" or .phase == "Co-op Closed" or .phase == "Closed" or .phase == "Failed" or .phase == "Abandoned" or .phase == "Funding Canceled") | .phase] | unique),
		  inactive_channel_snapshots: ([$snaps[] | select([(.channels // [])[] | .phase == "Inactive"] | any)] | length),
		  pod_restarts: ([$snaps[] | select(.pods != null) | [.pods[].restarts] | max // 0] | max),
		  flapping_at_end: $last.soak.flapping,
		  soak_phase_at_end: $last.soak.phase,
		  snapshots: $n
		}')
	local panics=null
	if ls "$WORKDIR"/metrics/*-panics >/dev/null 2>&1; then
		panics=$(cat "$WORKDIR"/metrics/*-panics | awk '{s+=$1} END {print s+0}')
	fi

	# Performance signals: benchmark, timings, and the end-of-run scrapes.
	# Payment and invoice latency come from the soak runner's own histograms
	# when its image exports them (successful payments, both directions
	# summed), otherwise from lnd's gRPC handling-time histograms, worst
	# node. result.json records which, and the baseline comparison only
	# judges latency against a baseline from the same source.
	local lat_p50=null lat_p95=null inv_p95=null rss=null startup_lndmon=null
	local lat_source=lnd rm="$WORKDIR/runner-metrics.prom"
	# One file for all runners; the quantile sums buckets across them.
	cat "$WORKDIR"/metrics/*-runner.prom > "$rm" 2>/dev/null || : > "$rm"
	if [[ -s "$rm" ]] && grep -q '^lightning_payment_runner_payment_duration_seconds_bucket' "$rm"; then
		lat_source=runner
		lat_p50=$(prom_hist_quantile 0.5 "$rm" '^lightning_payment_runner_payment_duration_seconds_bucket[{][^}]*result="succeeded"')
		lat_p95=$(prom_hist_quantile 0.95 "$rm" '^lightning_payment_runner_payment_duration_seconds_bucket[{][^}]*result="succeeded"')
		inv_p95=$(prom_hist_quantile 0.95 "$rm" '^lightning_payment_runner_invoice_duration_seconds_bucket[{]')
	fi
	local m n
	for m in "$WORKDIR"/metrics/*-lnd.prom; do
		[[ -s "$m" ]] || continue
		n=$(basename "$m" -lnd.prom)
		local r
		r=$(prom_sum "$m" '^process_resident_memory_bytes ')
		rss=$(jq -n --argjson a "$rss" --argjson b "$r" '[$a, $b] | map(select(. != null)) | max')
		if [[ "$lat_source" == lnd ]]; then
			local p50 p95 ip95
			# lnd's handling-time histogram has no grpc_code label, so
			# failed calls are in it too; it is the fallback, not the
			# preferred source.
			p50=$(prom_hist_quantile 0.5 "$m" 'grpc_server_handling_seconds_bucket[{][^}]*grpc_method="SendPaymentV2"')
			p95=$(prom_hist_quantile 0.95 "$m" 'grpc_server_handling_seconds_bucket[{][^}]*grpc_method="SendPaymentV2"')
			ip95=$(prom_hist_quantile 0.95 "$m" 'grpc_server_handling_seconds_bucket[{][^}]*grpc_method="AddInvoice"')
			lat_p50=$(jq -n --argjson a "$lat_p50" --argjson b "$p50" '[$a, $b] | map(select(. != null)) | max')
			lat_p95=$(jq -n --argjson a "$lat_p95" --argjson b "$p95" '[$a, $b] | map(select(. != null)) | max')
			inv_p95=$(jq -n --argjson a "$inv_p95" --argjson b "$ip95" '[$a, $b] | map(select(. != null)) | max')
		fi
		local lm="$WORKDIR/metrics/$n-lndmon.prom"
		if [[ -s "$lm" ]]; then
			local u s
			u=$(prom_sum "$lm" '^lnd_time_to_unlock_millisecs')
			s=$(prom_sum "$lm" '^lnd_time_to_start_millisecs')
			startup_lndmon=$(jq -n --argjson a "$startup_lndmon" --argjson u "$u" --argjson s "$s" \
				'[$a, (if $u == null or $s == null then null else ($u + $s) / 1000 end)] | map(select(. != null)) | max')
		fi
	done
	local performance
	performance=$(jq -n \
		--argjson bench "$(cat "$WORKDIR/bench.json" 2>/dev/null || echo null)" \
		--argjson timings "$(cat "$WORKDIR/timings.json")" \
		--argjson p50 "$lat_p50" --argjson p95 "$lat_p95" --argjson inv "$inv_p95" \
		--arg lat_source "$lat_source" \
		--argjson rss "$rss" --argjson startup_lndmon "$startup_lndmon" '{
		  throughput_pps: ($bench.throughput_pps_median // null),
		  bench_completed: ($bench.completed // 0),
		  payment_latency_p50_seconds: $p50,
		  payment_latency_p95_seconds: $p95,
		  invoice_latency_p95_seconds: $inv,
		  latency_source: $lat_source,
		  rss_bytes_max: $rss,
		  startup_seconds: ([$timings.node_startup_seconds[]] | max),
		  startup_seconds_lndmon: $startup_lndmon,
		  channel_open_seconds: ([$timings.channel_open_seconds[]] | max),
		  first_payment_seconds: $timings.first_payment_seconds
		}')

	# Compare against thresholds and the baseline.
	local base_json perf_limits
	base_json=$(jq -c . "$baseline") || die "$baseline is not valid JSON"
	perf_limits=$(jq -ce ".performance.${KIND}" "$f") ||
		die "no performance limits for KIND=${KIND} in thresholds.json"
	[[ "$perf_limits" != null ]] || die "no performance limits for KIND=${KIND} in thresholds.json"
	local result
	result=$(jq -n \
		--arg status pass \
		--arg image "${IMAGE}:${IMAGE_TAG}" --arg image_tag "$IMAGE_TAG" \
		--arg lnd_version "${LND_VERSION:-}" --arg kind "$KIND" --arg run_key "$RUN_NAME" \
		--arg sandbox "$SANDBOX_NS" --arg run_url "$RUN_URL" \
		--arg started "$(jq -r .applied_at "$WORKDIR/timings.json")" --arg finished "$(iso_now)" \
		--argjson functional "$functional" --argjson performance "$performance" \
		--argjson panics "$panics" \
		--argjson limits "$(jq -c '{functional, warmupMinutes, soakMinutes, bench}' "$f")" \
		--argjson perf_limits "$perf_limits" \
		--argjson baseline "$base_json" \
		--argjson max_fail_pct "$max_fail_pct" --argjson min_payments "$min_payments" \
		--argjson stall_min "$stall_min" --argjson sync_min "$sync_min" \
		--argjson max_restarts "$max_restarts" --argjson max_panics "$max_panics" '
		def breach($check; $value; $limit; $detail): {check: $check, value: $value, limit: $limit, detail: $detail};
		# Percent worse than baseline; positive is a regression either way.
		def regression($cur; $base; $lower_is_worse):
		  if $cur == null or $base == null or $base == 0 then null
		  elif $lower_is_worse then (($base - $cur) * 100 / $base)
		  else (($cur - $base) * 100 / $base) end;
		($functional + {panics: $panics}) as $fn |
		(
		  [
		    (if $fn.payments_after_warmup < 0
		       then breach("counter_reset"; $fn.payments_after_warmup; 0; "soak generator counters went backwards, its runner pod was replaced") else empty end),
		    (if $fn.failure_pct != null and $fn.payments_after_warmup >= $min_payments and $fn.failure_pct > $max_fail_pct
		       then breach("payment_failure_pct"; $fn.failure_pct; $max_fail_pct; "\($fn.failed_after_warmup) of \($fn.payments_after_warmup) payments after warm-up failed") else empty end),
		    (if $fn.max_payment_stall_minutes > $stall_min
		       then breach("payment_stall_minutes"; $fn.max_payment_stall_minutes; $stall_min; "soak generator settled nothing for longer than the limit") else empty end),
		    (if $fn.max_chain_sync_loss_minutes > $sync_min
		       then breach("chain_sync_loss_minutes"; $fn.max_chain_sync_loss_minutes; $sync_min; "a node was out of sync or not Running") else empty end),
		    (if ($fn.bad_channel_phases | length) > 0
		       then breach("channel_phase"; $fn.bad_channel_phases; []; "a channel left the Open phase") else empty end),
		    (if $fn.pod_restarts != null and $fn.pod_restarts > $max_restarts
		       then breach("pod_restarts"; $fn.pod_restarts; $max_restarts; "an lnd pod restarted during the soak") else empty end),
		    (if $fn.panics != null and $fn.panics > $max_panics
		       then breach("panics"; $fn.panics; $max_panics; "panic in lnd logs") else empty end),
		    (if $fn.soak_phase_at_end != "Running"
		       then breach("soak_phase"; $fn.soak_phase_at_end; "Running"; "soak generator was not Running at the end") else empty end)
		  ]
		) as $functional_breaches |
		($baseline.performance // {}) as $base |
		# Latency measured by the runner and by the lnd exporter are close
		# but not the same thing; a baseline from the other source is
		# treated as absent, so the metric is recorded, not judged.
		((($base.latency_source // "lnd") == $performance.latency_source) or ($base | length) == 0) as $same_lat_source |
		{
		  throughput_pps: {cur: $performance.throughput_pps, base: $base.throughput_pps, limit: $perf_limits.maxThroughputRegressionPct, lower_is_worse: true},
		  payment_latency_p95_seconds: {cur: $performance.payment_latency_p95_seconds, base: (if $same_lat_source then $base.payment_latency_p95_seconds else null end), limit: $perf_limits.maxLatencyP95RegressionPct, lower_is_worse: false},
		  invoice_latency_p95_seconds: {cur: $performance.invoice_latency_p95_seconds, base: (if $same_lat_source then $base.invoice_latency_p95_seconds else null end), limit: $perf_limits.maxInvoiceP95RegressionPct, lower_is_worse: false},
		  startup_seconds: {cur: $performance.startup_seconds, base: $base.startup_seconds, limit: $perf_limits.maxStartupRegressionPct, lower_is_worse: false},
		  rss_bytes_max: {cur: $performance.rss_bytes_max, base: $base.rss_bytes_max, limit: $perf_limits.maxRssRegressionPct, lower_is_worse: false}
		} as $cmp |
		($cmp | to_entries | map(
		   .key as $k | .value as $v |
		   regression($v.cur; $v.base; $v.lower_is_worse) as $pct |
		   {key: $k, value: {current: $v.cur, baseline: $v.base, change_pct: $pct, limit_pct: $v.limit,
		     verdict: (if $v.cur == null then "missing" elif $v.base == null then "recorded" elif $pct > $v.limit then "regressed" else "ok" end)}}
		 ) | from_entries) as $perf_cmp |
		($perf_cmp | to_entries | map(select(.value.verdict == "regressed") |
		   breach("performance:\(.key)"; .value.current; .value.baseline; "\(.value.change_pct | . * 10 | round / 10)% worse than baseline, limit \(.value.limit_pct)%"))) as $perf_breaches |
		($perf_cmp | to_entries | map(select(.value.verdict == "missing") | {check: "performance:\(.key)", reason: "no measurement"})
		 + (if $fn.pod_restarts == null then [{check: "pod_restarts", reason: "sandbox role lacks pods"}] else [] end)
		 + (if $fn.panics == null then [{check: "panics", reason: "sandbox role lacks pods/log"}] else [] end)) as $skipped |
		($functional_breaches + $perf_breaches) as $breached |
		{
		  status: (if ($breached | length) > 0 then "fail" else "pass" end),
		  image: $image, image_tag: $image_tag, lnd_version: $lnd_version, kind: $kind, run_key: $run_key,
		  sandbox_namespace: $sandbox, started_at: $started, finished_at: $finished,
		  thresholds: ($limits + {performance: $perf_limits}),
		  baseline: {image_tag: $baseline.image_tag, recorded_at: $baseline.recorded_at},
		  functional: $fn,
		  performance: $performance,
		  performance_vs_baseline: $perf_cmp,
		  breached: $breached,
		  skipped: $skipped,
		  links: {run: $run_url}
		}')
	echo "$result" > "$WORKDIR/result.json"
	jq . "$WORKDIR/result.json"
	[[ "$(jq -r .status "$WORKDIR/result.json")" == pass ]] || {
		log "GATE FAILED: $(jq -c '.breached' "$WORKDIR/result.json")"
		exit 1
	}
	log "gate passed"
}

# --------------------------------------------------------------- teardown
phase_teardown() {
	[[ -n "${RUN_NAME:-}" ]] || { log "nothing to tear down"; return 0; }
	[[ -f "$CI_KUBECONFIG" ]] || { log "no cluster kubeconfig, so no sandbox was created; nothing to tear down"; return 0; }
	if ! kc get "$ENVIRONMENTS" "$RUN_NAME" -n "$HOME_NS" >/dev/null 2>&1; then
		log "Environment $RUN_NAME already gone"
		return 0
	fi
	kc delete "$ENVIRONMENTS" "$RUN_NAME" -n "$HOME_NS" --wait=false
	gone() { ! kc get "$ENVIRONMENTS" "$RUN_NAME" -n "$HOME_NS" >/dev/null 2>&1; }
	# The finalizer holds the Environment until its namespace is deleted, so
	# its disappearance means the sandbox is gone too. Best effort: a sweeper
	# in the cluster deletes anything a lost runner leaves behind.
	if try_wait_for 600 "Environment $RUN_NAME to be deleted" gone; then
		log "sandbox deleted"
	else
		log "WARNING: Environment $RUN_NAME still terminating; the sweeper will finish it"
	fi
}

# ---------------------------------------------------------------- summary
phase_summary() {
	local out=${GITHUB_STEP_SUMMARY:-/dev/stdout}
	local r="$WORKDIR/result.json"
	if [[ ! -s "$r" ]]; then
		{
			echo "## lnd regression: no result"
			echo
			echo "The run ended before the gate produced a result; see the step logs."
			[[ -n "${IMAGE_TAG:-}" ]] && echo "Image: \`${IMAGE}:${IMAGE_TAG}\`"
		} >> "$out"
		return 0
	fi
	{
		echo "## lnd regression: $(jq -r .status "$r")"
		echo
		echo "| | |"
		echo "|---|---|"
		echo "| Image | \`$(jq -r .image "$r")\` |"
		echo "| Kind | $(jq -r .kind "$r") |"
		echo "| Sandbox | \`$(jq -r .sandbox_namespace "$r")\` |"
		echo "| Soak | $(jq -r .thresholds.soakMinutes "$r") min, warm-up $(jq -r .thresholds.warmupMinutes "$r") min |"
		echo "| Baseline | $(jq -r '.baseline.image_tag // "none"' "$r") |"
		echo "| Latency from | $(jq -r 'if .performance.latency_source == "runner" then "payment runner metrics" else "lnd exporter" end' "$r") |"
		echo
		if [[ "$(jq '.breached | length' "$r")" != 0 ]]; then
			echo "### Breached"
			echo
			echo "| Check | Value | Limit | Detail |"
			echo "|---|---|---|---|"
			jq -r '.breached[] | "| \(.check) | \(.value) | \(.limit) | \(.detail) |"' "$r"
			echo
		fi
		echo "### Functional"
		echo
		echo "| Check | Value | Limit |"
		echo "|---|---|---|"
		jq -r '.functional as $f | .thresholds.functional as $t |
			"| Payments after warm-up | \($f.payments_after_warmup) | at least \($t.minPaymentsForFailureRate) for the rate to count |",
			"| Failure rate | \(if $f.failure_pct == null then "n/a" else "\($f.failure_pct * 100 | round / 100)" end)% | \($t.maxPaymentFailurePct)% |",
			"| Longest payment stall | \($f.max_payment_stall_minutes * 10 | round / 10) min | \($t.maxPaymentStallMinutes) min |",
			"| Longest chain sync loss | \($f.max_chain_sync_loss_minutes) min | \($t.maxChainSyncLossMinutes) min |",
			"| Channel phases seen | \(if ($f.bad_channel_phases | length) == 0 then "Open only" else ($f.bad_channel_phases | join(", ")) end) | Open |",
			"| Snapshots with an Inactive channel | \($f.inactive_channel_snapshots) | informational |",
			"| lnd pod restarts | \($f.pod_restarts // "skipped") | \($t.maxPodRestarts) |",
			"| Panics in logs | \($f.panics // "skipped") | \($t.maxPanics) |"' "$r"
		echo
		echo "### Performance"
		echo
		echo "| Metric | This run | Baseline | Change | Limit | Verdict |"
		echo "|---|---|---|---|---|---|"
		jq -r '.performance_vs_baseline | to_entries[] |
			"| \(.key) | \(.value.current // "n/a") | \(.value.baseline // "n/a") | \(if .value.change_pct == null then "n/a" else "\(.value.change_pct * 10 | round / 10)%" end) | \(.value.limit_pct)% | \(.value.verdict) |"' "$r"
		echo
		echo "<details><summary>result.json</summary>"
		echo
		echo '```json'
		jq . "$r"
		echo '```'
		echo "</details>"
	} >> "$out"
}

# --------------------------------------------------------------- baseline
phase_baseline() {
	local r=${2:?usage: run.sh baseline <result.json>}
	# Through a temporary file, so a bad result.json leaves the committed
	# baseline untouched.
	local tmp
	tmp=$(mktemp "$baseline.XXXXXX")
	jq '{_comment: input_filename | "Performance baseline recorded by run.sh baseline from a green run; review in a PR.",
	     image_tag: .image_tag, recorded_at: .finished_at,
	     performance: (.performance | {throughput_pps, payment_latency_p95_seconds, invoice_latency_p95_seconds, latency_source, startup_seconds, rss_bytes_max})}' "$r" > "$tmp" ||
		{ rm -f "$tmp"; die "could not read $r"; }
	mv "$tmp" "$baseline"
	jq . "$baseline"
}

case "$phase" in
	cluster) phase_cluster ;;
	setup) phase_setup ;;
	scenario) phase_scenario ;;
	bench) phase_bench ;;
	soak) phase_soak ;;
	gate) phase_gate ;;
	teardown) phase_teardown ;;
	summary) phase_summary ;;
	baseline) phase_baseline "$@" ;;
	all)
		trap 'phase_teardown' EXIT
		phase_setup
		phase_scenario
		phase_bench
		phase_soak
		phase_gate
		;;
	*) die "unknown phase: $phase" ;;
esac
