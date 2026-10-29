#!/usr/bin/env bash
# Run every Go fuzz target in every module of this repository.
#
# Usage:
#   scripts/fuzz.sh [-t fuzztime] [package-dir ...]   run targets
#   scripts/fuzz.sh -l [package-dir ...]              list "dir target" pairs
#   scripts/fuzz.sh -j                                print package dirs as JSON
#
# Targets are found by scanning *_test.go files for "func FuzzX(f
# *testing.F)", so a new fuzz target runs in CI, in the nightly job, and in
# `just fuzz` without a list to update. A target that go test does not
# fuzz (for example, one behind a build tag) fails. -t takes any
# `go test -fuzztime` value (default 30s). With package dirs, only those
# directories run. Set FUZZ_FAILURES_FILE to also write the failing targets
# to that file.
#
# Go writes each failing input to <package-dir>/testdata/fuzz/<target>/.
# Commit that file to keep the case as a regression test.
set -euo pipefail

usage() {
	sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
}

fuzztime=30s
mode=run
while getopts 't:ljh' opt; do
	case "$opt" in
	t) fuzztime="$OPTARG" ;;
	l) mode=list ;;
	j) mode=json ;;
	*) usage ;;
	esac
done
shift $((OPTIND - 1))

cd "$(git rev-parse --show-toplevel)"

# A fuzz target declaration. The scan is grep, not `go test -list`: no
# build per package.
fuzz_func='^func Fuzz[A-Za-z0-9_]*\([A-Za-z_][A-Za-z0-9_]* \*testing\.F\)'

# Package directories that declare at least one fuzz target, tracked or new.
all_dirs() {
	git ls-files --cached --others --exclude-standard -- '*_test.go' |
		xargs grep -lE "$fuzz_func" |
		xargs -n1 dirname |
		sort -u
}

targets_in() {
	grep -hoE "$fuzz_func" "$1"/*_test.go | sed -E 's/^func (Fuzz[A-Za-z0-9_]*).*/\1/' | sort -u
}

dirs=()
if [ "$#" -gt 0 ]; then
	for dir in "$@"; do
		dir="${dir#./}"
		dir="${dir%/}"
		dirs+=("${dir:-.}")
	done
else
	while IFS= read -r dir; do
		dirs+=("$dir")
	done < <(all_dirs)
fi

if [ "$mode" = json ]; then
	printf '['
	sep=''
	for dir in "${dirs[@]}"; do
		printf '%s"%s"' "$sep" "$dir"
		sep=','
	done
	printf ']\n'
	exit 0
fi

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

# fuzzed reports whether log $1 shows that the engine ran. go test exits 0
# without fuzzing when -fuzz matches nothing it compiled, or when a build
# tag excludes every test file of the package ("no test files").
fuzzed() {
	grep -q '^fuzz: elapsed:' "$1"
}

# deadline_only reports whether the failed run in log $1 is only the fuzz
# engine's own stop: in Go 1.25 (the fuzz jobs' version) the engine can
# report "context deadline exceeded" when -fuzztime ends
# (go.dev/issue/75804; Go 1.27.1 has the fix). A real failure writes its
# input, and the log names that file.
deadline_only() {
	fuzzed "$1" &&
		! grep -q 'Failing input written to' "$1" &&
		grep -qE '^[[:space:]]+context deadline exceeded$' "$1" &&
		[ "$(grep -c -- '--- FAIL' "$1")" -eq 1 ]
}

failures=()
for dir in "${dirs[@]}"; do
	if ! targets="$(targets_in "$dir")" || [ -z "$targets" ]; then
		echo "no fuzz targets in $dir" >&2
		exit 1
	fi
	for target in $targets; do
		if [ "$mode" = list ]; then
			echo "$dir $target"
			continue
		fi
		echo "=== $dir $target ($fuzztime)"
		if (cd "$dir" && go test . -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzztime") 2>&1 | tee "$log" &&
			fuzzed "$log"; then
			echo "PASS $dir $target"
		elif deadline_only "$log"; then
			echo "PASS $dir $target (the engine's deadline error at the end of -fuzztime, go.dev/issue/75804)"
		else
			echo "FAIL $dir $target"
			failures+=("$dir $target")
		fi
	done
done

[ "$mode" = list ] && exit 0

if [ "${#failures[@]}" -gt 0 ]; then
	echo
	echo "fuzz failures:"
	printf '  - %s\n' "${failures[@]}"
	if [ -n "${FUZZ_FAILURES_FILE:-}" ]; then
		printf '%s\n' "${failures[@]}" >"$FUZZ_FAILURES_FILE"
	fi
	exit 1
fi
echo "all fuzz targets passed"
