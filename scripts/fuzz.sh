#!/usr/bin/env bash
# Run every Go fuzz target in every module of this repository.
#
# Usage:
#   scripts/fuzz.sh [-t fuzztime] [package-dir ...]   run targets
#   scripts/fuzz.sh -l [package-dir ...]              list "dir target" pairs
#   scripts/fuzz.sh -j                                print package dirs as JSON
#
# Targets are found by scanning *_test.go files for "func Fuzz", so a new
# fuzz target runs in CI, in the nightly job, and in `just fuzz` without a
# list to update. -t takes any `go test -fuzztime` value (default 30s).
# With package dirs, only those directories run. Set FUZZ_FAILURES_FILE to
# also write the failing targets to that file.
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

# Package directories that declare at least one fuzz target, tracked or
# new. The scan is grep, not `go test -list`: no build per package.
all_dirs() {
	git ls-files --cached --others --exclude-standard -- '*_test.go' |
		xargs grep -l '^func Fuzz' |
		xargs -n1 dirname |
		sort -u
}

targets_in() {
	grep -ho '^func Fuzz[A-Za-z0-9_]*' "$1"/*_test.go | sed 's/^func //' | sort -u
}

dirs=()
if [ "$#" -gt 0 ]; then
	for dir in "$@"; do
		dir="${dir#./}"
		dirs+=("${dir%/}")
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

failures=()
for dir in "${dirs[@]}"; do
	for target in $(targets_in "$dir"); do
		if [ "$mode" = list ]; then
			echo "$dir $target"
			continue
		fi
		echo "=== $dir $target ($fuzztime)"
		if (cd "$dir" && go test . -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzztime"); then
			echo "PASS $dir $target"
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
