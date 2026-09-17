set shell := ["zsh", "-cu"]

# Usage:
#   just bench                           # run all benchmarks
#   just bench-filter Event              # benchmark names matching Event
#   just bench-save baseline             # save snapshot to .bench/baseline.txt
#   just bench-compare baseline current  # compare snapshots with benchstat

default:
  @just --list

# Maintain clean dependencies across all modules.
tidy:
  while IFS= read -r modfile; do \
    moddir="$(dirname "$modfile")"; \
    echo "== tidying $moddir =="; \
    (cd "$moddir" && go fmt ./... && go fix ./... && go mod tidy && gofumpt -l -w .); \
  done < <(printf '%s\n' go.mod && git ls-files '**/go.mod')


# Lint every module with the strict config (pinned linter version).
lint:
  while IFS= read -r modfile; do \
    moddir="$(dirname "$modfile")"; \
    echo "== linting $moddir =="; \
    (cd "$moddir" && go vet ./... && go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run ./...); \
  done < <(printf '%s\n' go.mod && git ls-files '**/go.mod')

# Scan every module for reachable vulnerabilities.
vuln:
  while IFS= read -r modfile; do \
    moddir="$(dirname "$modfile")"; \
    echo "== govulncheck $moddir =="; \
    (cd "$moddir" && go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...); \
  done < <(printf '%s\n' go.mod && git ls-files '**/go.mod')

test:
  while IFS= read -r modfile; do \
    moddir="$(dirname "$modfile")"; \
    echo "== testing $moddir =="; \
    (cd "$moddir" && go test ./...); \
  done < <(printf '%s\n' go.mod && git ls-files '**/go.mod')

coverage:
  while IFS= read -r modfile; do \
    moddir="$(dirname "$modfile")"; \
    echo "== coverage $moddir =="; \
    (cd "$moddir" && go test ./... -cover); \
  done < <(printf '%s\n' go.mod && git ls-files '**/go.mod')

bench:
  (cd benches && go test -run '^$' -bench . -benchmem ./...)

bench-core:
  (cd benches && go test -run '^$' -bench 'Benchmark(Event|Commit|JSON|NonHTTP|Operation|RateSampler)' -benchmem .)

bench-nonhttp:
  (cd benches && go test -run '^$' -bench '^BenchmarkNonHTTP' -benchmem .)

bench-routers:
  (cd benches && go test -run '^$' -bench '^BenchmarkRouter' -benchmem .)

bench-middleware-overhead:
  (cd benches && go test -run '^$' -bench '^BenchmarkRouter.*/middleware_on_sink_noop$' -benchmem .)

bench-normal-logging:
  (cd benches && go test -run '^$' -bench '^BenchmarkRouter.*/normal_logging_.*' -benchmem .)

bench-router-comparison:
  (cd benches && go test -run '^$' -bench '^BenchmarkRouter.*/(middleware_on_sink_noop|normal_logging_.*)' -benchmem .)

bench-filter name:
  (cd benches && go test -run '^$' -bench '{{name}}' -benchmem ./...)

bench-save name count='10' benchtime='1s':
  mkdir -p .bench
  (cd benches && go test -run '^$' -bench . -benchmem -count {{count}} -benchtime {{benchtime}} ./...) | tee .bench/{{name}}.txt

bench-compare old new:
  go run golang.org/x/perf/cmd/benchstat@latest .bench/{{old}}.txt .bench/{{new}}.txt

bench-adapters:
  (cd benches && go test -run '^$' -bench '^BenchmarkAdapter' -benchmem .)
