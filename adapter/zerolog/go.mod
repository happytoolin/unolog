module github.com/happytoolin/unolog/adapter/zerolog

go 1.25.0

require (
	github.com/happytoolin/unolog v1.1.0 // x-release-please-version
	github.com/rs/zerolog v1.35.1
)

require (
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	go.uber.org/goleak v1.3.0
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/happytoolin/unolog => ../..
