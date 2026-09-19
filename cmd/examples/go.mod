module github.com/happytoolin/unolog/cmd/examples

go 1.25.0

require (
	github.com/gin-gonic/gin v1.12.0
	github.com/gofiber/fiber/v2 v2.52.15
	github.com/gofiber/fiber/v3 v3.5.0
	github.com/happytoolin/unolog v1.1.0
	github.com/happytoolin/unolog/adapter/slog v1.0.1
	github.com/happytoolin/unolog/adapter/zap v1.0.1
	github.com/happytoolin/unolog/adapter/zerolog v1.0.1
	github.com/happytoolin/unolog/integration/echo v1.0.1
	github.com/happytoolin/unolog/integration/fiber v1.0.1
	github.com/happytoolin/unolog/integration/fiberv3 v1.0.1
	github.com/happytoolin/unolog/integration/gin v1.0.1
	github.com/happytoolin/unolog/integration/std v1.0.1
	github.com/happytoolin/unolog/integration/worker v1.0.1
	github.com/labstack/echo/v4 v4.15.4
	github.com/rs/zerolog v1.35.1
	go.uber.org/zap v1.28.0
)

replace github.com/happytoolin/unolog => ../..

replace github.com/happytoolin/unolog/adapter/slog => ../../adapter/slog

replace github.com/happytoolin/unolog/adapter/zap => ../../adapter/zap

replace github.com/happytoolin/unolog/adapter/zerolog => ../../adapter/zerolog

replace github.com/happytoolin/unolog/integration/echo => ../../integration/echo

replace github.com/happytoolin/unolog/integration/fiber => ../../integration/fiber

replace github.com/happytoolin/unolog/integration/fiberv3 => ../../integration/fiberv3

replace github.com/happytoolin/unolog/integration/gin => ../../integration/gin

replace github.com/happytoolin/unolog/integration/std => ../../integration/std

replace github.com/happytoolin/unolog/integration/worker => ../../integration/worker

require (
	github.com/bytedance/gopkg v0.1.4 // indirect
	github.com/bytedance/sonic v1.15.4 // indirect
	github.com/bytedance/sonic/loader v0.5.2 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/cloudwego/base64x v0.1.7 // indirect
	github.com/gabriel-vasile/mimetype v1.4.15 // indirect
	github.com/gin-contrib/sse v1.1.2 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.4 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/gofiber/schema v1.8.6 // indirect
	github.com/gofiber/utils/v2 v2.5.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/labstack/gommon v0.5.0 // indirect
	github.com/leodido/go-urn v1.5.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mattn/go-runewidth v0.0.30 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/molecule-man/go-brrr v1.1.0 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.61.0 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.3.2 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.74.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	go.mongodb.org/mongo-driver/v2 v2.9.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/arch v0.30.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/tools v0.49.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
