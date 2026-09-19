module github.com/happytoolin/unolog/integration/std

go 1.25.0

require (
	github.com/happytoolin/unolog v1.1.0 // x-release-please-version
	go.uber.org/goleak v1.3.0
)

replace github.com/happytoolin/unolog => ../..
