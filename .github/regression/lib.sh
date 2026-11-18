#!/usr/bin/env bash
# Helpers shared by run.sh. Sourced, not executed.

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
now_s() { date -u +%s; }
iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# iso_to_s converts an RFC3339 timestamp (as the apiserver writes them) to
# unix seconds. GNU date on the runners, BSD date on macOS.
iso_to_s() {
	local ts=$1
	[[ -z "$ts" || "$ts" == null ]] && { echo 0; return; }
	if date -u -d "$ts" +%s 2>/dev/null; then return; fi
	date -u -j -f %Y-%m-%dT%H:%M:%SZ "$ts" +%s
}

# render substitutes ${VAR} references in a template from the environment and
# fails if any reference is left unset, so a missing variable never reaches
# the cluster as a literal "${...}".
render() {
	local template=$1 out
	out=$(perl -pe 's/\$\{(\w+)\}/exists $ENV{$1} ? $ENV{$1} : "\${$1}"/ge' "$template")
	if grep -n '\${[A-Z_][A-Z0-9_]*}' <<<"$out" >&2; then
		die "unset variable(s) in $template"
	fi
	printf '%s\n' "$out"
}

# kc runs kubectl as the CI identity (cluster kubeconfig). ks runs it as the
# runner ServiceAccount inside the sandbox namespace. Every call carries a
# request timeout: a stalled apiserver connection otherwise hangs a poll
# for as long as the TCP stack allows, and the loops here would rather
# fail the call and ask again. A call that legitimately runs longer passes
# its own --request-timeout, which wins.
kc() { kubectl --kubeconfig "$CI_KUBECONFIG" --request-timeout=60s "$@"; }
ks() { kubectl --kubeconfig "$SANDBOX_KUBECONFIG" -n "$SANDBOX_NS" --request-timeout=60s "$@"; }

# can_i reports whether the sandbox identity may <verb> <resource>; used to
# skip, rather than fail, checks that need permissions the bound roles do not
# grant.
can_i() { ks auth can-i "$1" "$2" >/dev/null 2>&1; }

# try_wait_for <timeout_seconds> <description> <command...> polls until the
# command succeeds, every 10s, and returns 1 on timeout. wait_for is the
# fatal variant.
try_wait_for() {
	local timeout=$1 desc=$2
	shift 2
	local deadline=$(( $(now_s) + timeout ))
	until "$@"; do
		if (( $(now_s) > deadline )); then
			log "timed out after ${timeout}s waiting for ${desc}"
			return 1
		fi
		sleep 10
	done
}
wait_for() { try_wait_for "$@" || exit 1; }

# jget <kind/name> <jsonpath> prints one field of a sandbox object, empty if
# the object or field is missing.
jget() { ks get "$1" -o "jsonpath=$2" 2>/dev/null || true; }

# jq_file <file> <filter> runs jq on a file with -r.
jq_file() { jq -r "$2" "$1"; }

# median prints the median of the numbers on stdin.
median() {
	sort -g | awk '{a[NR]=$1} END {
		if (NR == 0) { print "null"; exit }
		if (NR % 2) print a[(NR+1)/2]; else print (a[NR/2] + a[NR/2+1]) / 2 }'
}

# prom_sum <file> <regex> sums the values of the Prometheus text samples whose
# line matches the regex (use it on non-histogram samples).
prom_sum() {
	awk -v re="$2" '$0 ~ re && $0 !~ /^#/ { s += $NF; n++ } END { print (n ? s : "null") }' "$1"
}

# prom_hist_quantile <q> <file> <regex> computes a quantile from the
# _bucket samples whose line matches the regex, summing buckets across other
# labels the same way histogram_quantile(sum by (le)) does, with linear
# interpolation inside the bucket. Prints "null" when there are no samples.
# Write the regex with [{] rather than \{ : it reaches awk through -v, which
# applies escape processing.
prom_hist_quantile() {
	awk -v q="$1" -v re="$3" '
		$0 ~ re && $0 ~ /_bucket[{]/ && $0 !~ /^#/ {
			if (match($0, /[,{]le="[^"]+"/)) {
				le = substr($0, RSTART + 5, RLENGTH - 6)
				c[le] += $NF
			}
		}
		END {
			n = 0
			for (le in c) { n++; k[n] = le }
			if (n == 0 || !("+Inf" in c) || c["+Inf"] == 0) { print "null"; exit }
			# numeric insertion sort; +Inf last
			for (i = 1; i <= n; i++) v[i] = (k[i] == "+Inf") ? 1e308 : k[i] + 0
			for (i = 2; i <= n; i++) {
				tv = v[i]; tk = k[i]; j = i - 1
				while (j > 0 && v[j] > tv) { v[j+1] = v[j]; k[j+1] = k[j]; j-- }
				v[j+1] = tv; k[j+1] = tk
			}
			total = c["+Inf"]; target = q * total
			prev_le = 0; prev_c = 0
			for (i = 1; i <= n; i++) {
				cur = c[k[i]]
				if (cur >= target) {
					if (k[i] == "+Inf") { print prev_le; exit }
					frac = (cur == prev_c) ? 0 : (target - prev_c) / (cur - prev_c)
					printf "%.6f\n", prev_le + (v[i] - prev_le) * frac
					exit
				}
				prev_le = v[i]; prev_c = cur
			}
			print "null"
		}' "$2"
}
