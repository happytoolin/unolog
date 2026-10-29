#!/usr/bin/env bash
# Run mutation tests with gremlins in every module of this repository.
#
# Usage:
#   scripts/mutate.sh [module-dir ...]
#
# gremlins changes one operator at a time (a comparison, an arithmetic
# operator, a loop control, ...) and runs the tests of the changed
# package. A change that no test fails "lived": the tests do not check
# that line. A module fails when its test efficacy (killed / (killed +
# lived)) is below its floor: 100 percent, or 85 for the root module,
# which keeps equivalent mutants (fast-path choices, capacity hints,
# double-checked locks). MUTATION_EFFICACY sets one floor for every
# module. Set MUTATION_WORKERS to limit the parallel test runs (default:
# CPU count).
#
# Each module runs alone: the root run excludes the nested modules.
# benches and cmd/examples have no library code and are skipped.
set -euo pipefail

usage() {
	sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
}

[ "${1:-}" = -h ] && usage

gremlins=github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
mutators=(
	--invert-assignments --invert-bitwise --invert-bwassign --invert-logical
	--invert-loopctrl --remove-self-assignments
)

# find, not git ls-files: the script must also run in a copy without .git
# (for example a sandbox).
cd "$(dirname "$0")/.."

modules=()
while IFS= read -r mod; do
	modules+=("$mod")
done < <(find . -name go.mod -not -path '*/.*/*' -exec dirname {} \; | sed 's|^\./||' | sort)

dirs=()
if [ "$#" -gt 0 ]; then
	for dir in "$@"; do
		dir="${dir#./}"
		dir="${dir%/}"
		dirs+=("${dir:-.}")
	done
else
	for mod in "${modules[@]}"; do
		case "$mod" in
		benches | cmd/*) ;;
		*) dirs+=("$mod") ;;
		esac
	done
fi

# floor prints the minimum test efficacy, in percent, for module $1.
floor() {
	if [ -n "${MUTATION_EFFICACY:-}" ]; then
		echo "$MUTATION_EFFICACY"
	elif [ "$1" = . ]; then
		echo 85
	else
		echo 100
	fi
}

# efficacy_ok checks gremlins log $1 against floor $2. gremlins v0.6.0
# ignores --threshold-efficacy (the flag value never reaches its check),
# so the script compares the reported efficacy. A module without
# mutants reports none and passes.
efficacy_ok() {
	grep -q '^No results to report\.$' "$1" && return 0
	got="$(sed -n 's/^Test efficacy: \([0-9.]*\)%$/\1/p' "$1")"
	[ -n "$got" ] && awk -v got="$got" -v want="$2" 'BEGIN { exit !(got >= want) }'
}

failures=()
for dir in "${dirs[@]}"; do
	# gremlins walks the directory tree itself, so it must skip the
	# nested modules and the dot directories.
	exclude=(-E '(^|/)\.')
	for mod in "${modules[@]}"; do
		if [ "$dir" = . ] && [ "$mod" != . ]; then
			exclude+=(-E "^${mod}/")
		elif [ "$mod" != "$dir" ] && [[ "$mod" == "$dir"/* ]]; then
			exclude+=(-E "^${mod#"$dir"/}/")
		fi
	done
	workers=()
	[ -n "${MUTATION_WORKERS:-}" ] && workers=(--workers "$MUTATION_WORKERS")

	want="$(floor "$dir")"
	echo "=== $dir (floor ${want}%)"
	# -S lt prints only the lived and timed-out mutants, then the totals.
	log="$(mktemp)"
	if (cd "$dir" && go run "$gremlins" unleash -S lt \
		"${mutators[@]}" ${workers[@]+"${workers[@]}"} "${exclude[@]}" .) | tee "$log" &&
		efficacy_ok "$log" "$want"; then
		echo "PASS $dir"
	else
		echo "FAIL $dir"
		failures+=("$dir")
	fi
	rm -f "$log"
done

if [ "${#failures[@]}" -gt 0 ]; then
	echo
	echo "mutation test failures (efficacy below the floor, or the run failed):"
	printf '  - %s\n' "${failures[@]}"
	exit 1
fi
echo "all modules passed"
