# Contributing

Thank you for helping improve unolog.

## Before you start

- Use Go 1.25 or later.
- Open an issue before a large API or behavior change.
- Keep changes small and focused.
- Add or update tests for behavior changes.
- Update the README or migration guide when a public API changes.

## Check your change

Run the repository checks from the project root:

```bash
just test
just lint
just vuln
```

Run `just tidy` after a dependency or module-file change. Review its output
before you commit it because this repository contains multiple Go modules.

## Fuzz your change

Run the fuzz targets when you change the encoder, the WAL, the lifecycle, an
adapter, or an integration:

```bash
just fuzz            # every target, 30s each
just fuzz 2m .       # only the root package, 2 minutes each
just fuzz-list       # list every target
```

`scripts/fuzz.sh` finds every `Fuzz` function in every module, so a new
target runs in CI and in the nightly job without a workflow change. When a
target fails, Go writes the input to `<package>/testdata/fuzz/<target>/`.
Commit that file with the fix: it then runs as a regression test in
`go test`.

## Pull requests

Describe the problem and the chosen solution. Link the related issue when one
exists. CI must pass before the change can merge.

Release Please updates the changelog and release version. Do not edit release
versions by hand unless the release automation itself needs repair.
