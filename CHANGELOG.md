# Changelog

## Unreleased

## [1.2.0](https://github.com/happytoolin/unolog/compare/v1.1.0...v1.2.0) (2026-09-19)


### Features

* add CONTRIBUTING.md and SECURITY.md for improved guidelines ([33ef396](https://github.com/happytoolin/unolog/commit/33ef3969418e46c0c4cc0ab5866ca2a10a832c68))


### Performance Improvements

* add realistic route benchmarks ([90dbbff](https://github.com/happytoolin/unolog/commit/90dbbff67b5d6e6a8bc45271b98d574696621402))

## [1.1.0](https://github.com/happytoolin/unolog/compare/v1.0.1...v1.1.0) (2026-09-19)


### Features

* add unolog agent skill ([c3af501](https://github.com/happytoolin/unolog/commit/c3af501715df7cbe723e317109943f2e78846078))
* publish unolog agent skill ([e425a11](https://github.com/happytoolin/unolog/commit/e425a117acfbaa02fc8033a86b46ec1c62299131))


### Bug Fixes

* **deps:** update dependencies and clear all reachable vulnerabilities ([06e1545](https://github.com/happytoolin/unolog/commit/06e15459d0377e9291ab5f1028c6f327dae51961))
* **deps:** update dependencies, clear vulnerabilities, and add strict linting ([408bd14](https://github.com/happytoolin/unolog/commit/408bd14a178e65a24558eb7e9973b86715119eb3))
* harden logging lifecycle and hot paths ([6c9c498](https://github.com/happytoolin/unolog/commit/6c9c4981048809b80e0f8fa153c536086be3fb0e))


### Performance Improvements

* harden logging hot paths ([b334fbb](https://github.com/happytoolin/unolog/commit/b334fbb8d67992c7decd58307a99fc5f10a3b45a))

## [1.0.1](https://github.com/happytoolin/unolog/compare/v1.0.0...v1.0.1) (2026-09-15)


### Performance Improvements

* cut ~15% off the canonical encoder path ([0d24e2f](https://github.com/happytoolin/unolog/commit/0d24e2f826006f3a212ec1cbf7ef70ba3365be29))
* cut ~15% off the canonical encoder path ([d388306](https://github.com/happytoolin/unolog/commit/d388306e85bea4ee6eb2bcdc860442a2e01f41be))

## [1.0.0](https://github.com/happytoolin/unolog/compare/v0.5.0...v1.0.0) (2026-09-15)


### ⚠ BREAKING CHANGES

* module path moved from github.com/happytoolin/happycontext to github.com/happytoolin/unolog.
* remove the raw-JSON surface — AddRawJSON, KindRaw, Field.Raw()
* The entire v0 public API is replaced. See MIGRATION.md for the complete v0 -> v1 symbol map.

### Features

* add first-party JSON sink with SWAR encoder ([#23](https://github.com/happytoolin/unolog/issues/23)) ([7919d17](https://github.com/happytoolin/unolog/commit/7919d1711553292954bc417885ef0967d3537e73))
* remove the raw-JSON surface — AddRawJSON, KindRaw, Field.Raw() ([a683c53](https://github.com/happytoolin/unolog/commit/a683c5339f0c345d2560fe1e989714df21b8479f))
* v2 record core (W3-W9) + DST hardening ([#29](https://github.com/happytoolin/unolog/issues/29)) ([07b8293](https://github.com/happytoolin/unolog/commit/07b82938720bca7d84ce30db8a913300ab671f5f))


### Bug Fixes

* **ci:** point release-please at v2 with the input that exists ([04519dc](https://github.com/happytoolin/unolog/commit/04519dca0804194439f63c0c3b9d1e7891ac12f3))
* **ci:** point release-please at v2 with the input that exists ([b8139cb](https://github.com/happytoolin/unolog/commit/b8139cbda3f089027a83d628d01efdd5f6646864))
* **core:** GLM whole-branch audit — 1 defect + 2 probable-defects + API-freeze polish ([5850c19](https://github.com/happytoolin/unolog/commit/5850c1953d625851f3e9fdcb6929a50c193203c9))
* **v2:** apply the final pre-1.0 quality-audit fixes ([1a4e91d](https://github.com/happytoolin/unolog/commit/1a4e91d3b8423bf2f157e40d550bc24c4334f29f))
* **v2:** apply the final pre-1.0 quality-audit fixes ([1ed5f45](https://github.com/happytoolin/unolog/commit/1ed5f450d50c68356d6e292601862be0be1fc738))
* **v2:** close the GLM-5.3 review findings ([1c8d236](https://github.com/happytoolin/unolog/commit/1c8d2361b6410fd7648b0674a64a0615626ca9d5))
* **v2:** close the post-End write race with always-guarded events ([deb20d8](https://github.com/happytoolin/unolog/commit/deb20d8e93e6e4ef27e83ae334f69dac52c66c6c))
* **v2:** close the post-End write race with always-guarded events ([7ad1dac](https://github.com/happytoolin/unolog/commit/7ad1dac8c1cd2dc256a30e4900af4b96352cd342))
* **v2:** mirror the retry sampling rule in the lifecycle fuzz model ([f2c15bf](https://github.com/happytoolin/unolog/commit/f2c15bfe7dd64718247514892b9be632d0ddb004))
* **v2:** resolve the 1.0.0 release-review findings ([6e2d549](https://github.com/happytoolin/unolog/commit/6e2d54926b7ec6b5ffa3b4396cd1296788d3b046))
* **v2:** serialize reset with stale watchdog snapshots ([3bc0066](https://github.com/happytoolin/unolog/commit/3bc0066a9917340841b2014394bf5c8b59d30500))


### Performance Improvements

* deliver the optimization the review caught as half-implemented ([6cbafe3](https://github.com/happytoolin/unolog/commit/6cbafe3fb7a1bfc6e006602aabfb7ec5bf4e987a))
* hot-path wins — EndDrop −18%, encode −10%, batched post-seal ([5f3ce19](https://github.com/happytoolin/unolog/commit/5f3ce196cf6dfec493dea516d13b27b0a728c529))
* hot-path wins — EndDrop −18%, encode −10%, batched post-seal ([388ed12](https://github.com/happytoolin/unolog/commit/388ed1253880e5b6a3e5e50f097173fa360e56eb))
* v2 optimizations — zerolog Encoded(), Record pooling, lazy start fields, fast-path sampling ([4f725f4](https://github.com/happytoolin/unolog/commit/4f725f4accc965f7460b8280a1669154fb5c3fcd))


### Code Refactoring

* point the module path at github.com/happytoolin/unolog ([bbd06cb](https://github.com/happytoolin/unolog/commit/bbd06cb458ecec98bfca3bdd5ef018448f1c4439))

## [0.5.0](https://github.com/happytoolin/happycontext/compare/v0.4.0...v0.5.0) (2026-08-30)


### ⚠ BREAKING CHANGES

* remove DeterministicOrder sorting from adapters ([#21](https://github.com/happytoolin/happycontext/issues/21)). Pre-1.0 semver: breaking changes may land in 0.x minor releases. Default output is unchanged; `SinkOptions.DeterministicOrder` was opt-in. Migrate stable-byte assertions to `hc.TestSink`, or pin the v0.4 adapter modules (the `Sink` interface is unchanged).

### Code Refactoring

* remove DeterministicOrder sorting from adapters ([#21](https://github.com/happytoolin/happycontext/issues/21)) ([b4f62dc](https://github.com/happytoolin/happycontext/commit/b4f62dc2ffdead732119a9f665a7ee0b77dd8dcb))

## [0.4.0](https://github.com/happytoolin/happycontext/compare/v0.3.0...v0.4.0) (2026-08-27)


### Performance Improvements

* reduce hot-path allocations in core, slog adapter, and middleware ([040c221](https://github.com/happytoolin/happycontext/commit/040c221cbffbc4b0aee1d5ba9b97ca9c9a09176d))
* skip unnecessary lifecycle and adapter work ([#19](https://github.com/happytoolin/happycontext/issues/19)) ([e213a7e](https://github.com/happytoolin/happycontext/commit/e213a7ee79d4a88d40d17c6be20597297c149991))

## [0.3.0](https://github.com/happytoolin/happycontext/compare/v0.2.4...v0.3.0) (2026-06-06)

### Breaking Changes

* `EventFields` now returns a shallow top-level copy. Nested maps/slices are shared references.
* Public API simplified:
  * Removed `AddMap`
  * Removed `Add2`
  * `Add` now accepts variadic key/value pairs: `Add(ctx, "k1", v1, "k2", v2, ...)`

### Features

* add generic operation lifecycle and worker integration ([eec5979](https://github.com/happytoolin/happycontext/commit/eec597988a8a1bd25c6815b83f1f56bd8795923d))
* add stateful and deferred operation handle APIs ([5043b2a](https://github.com/happytoolin/happycontext/commit/5043b2a27a549fe2a994325c3f9ff258a812f070), [0a551a3](https://github.com/happytoolin/happycontext/commit/0a551a3aa23af91ab6e7fed5c2b1515a657fe161))
* add operation policies, structured lifecycle metadata, and event accessors ([3da301a](https://github.com/happytoolin/happycontext/commit/3da301a16d73b3082da4e8d41bd81b9d6976bdbe), [6754177](https://github.com/happytoolin/happycontext/commit/6754177ca1a505c5945401b4e7dec80cc1838200))

### Bug Fixes

* harden lifecycle edge cases ([5b811f2](https://github.com/happytoolin/happycontext/commit/5b811f271d221517735bb7a5ab59924e156c37d1))
* preserve package compatibility ([f5963f1](https://github.com/happytoolin/happycontext/commit/f5963f109bfd7cd88b579e30be50caef4f909f93))
* fix operation sampling precedence ([ad9afba](https://github.com/happytoolin/happycontext/commit/ad9afbadb607d25b4f8fad961829f82b8810af87))

## [0.2.4](https://github.com/happytoolin/happycontext/compare/v0.2.3...v0.2.4) (2026-04-04)


### Miscellaneous Chores

* release 0.2.4 ([4719b58](https://github.com/happytoolin/happycontext/commit/4719b58222ec6b3f38a2a361bb3b48193ae6357f))

## [0.2.3](https://github.com/happytoolin/happycontext/compare/v0.2.2...v0.2.3) (2026-04-04)


### Bug Fixes

* repair root release-please tag matching ([590dcce](https://github.com/happytoolin/happycontext/commit/590dccec72e91c5c444c46c310cc5158c37817fa))
* stop root package-name from shadowing root tags ([68fb797](https://github.com/happytoolin/happycontext/commit/68fb7973f126eb268f3268af22fc4a382b77960f))

## [0.2.2](https://github.com/happytoolin/happycontext/compare/v0.2.1...v0.2.2) (2026-04-04)


### Bug Fixes

* align workspace module requirements ([50f82b3](https://github.com/happytoolin/happycontext/commit/50f82b36d02769e80445c6d1a7919cb086929a4c))

## [0.2.1](https://github.com/happytoolin/happycontext/compare/v0.2.0...v0.2.1) (2026-04-04)


### Miscellaneous Chores

* fix go module release tagging ([8e318cb](https://github.com/happytoolin/happycontext/commit/8e318cbc4d544eec2da3a012b005ea7ebe967533))

## [0.2.0](https://github.com/happytoolin/happycontext/compare/happycontext-v0.1.0...happycontext-v0.2.0) (2026-04-03)


### Features

* document per-request message overrides ([1b78d4f](https://github.com/happytoolin/happycontext/commit/1b78d4f8da42dc5f5e5c4e08aad8a51b3476852a))

## [0.1.0](https://github.com/happytoolin/happycontext/compare/happycontext-v0.0.1...happycontext-v0.1.0) (2026-02-10)


### Features

* Introduce advanced sampling options with per-level rates and custom samplers, and ensure TestSink deep copies event fields. ([537ed99](https://github.com/happytoolin/happycontext/commit/537ed994ee4a437f8d9d6531db1b212ed9e6ca9e))

## [0.0.1](https://github.com/happytoolin/happycontext/compare/happycontext-v0.0.1...happycontext-v0.0.1) (2026-02-09)


### Features

* add comprehensive benchmarking for logging adapters and integrations ([5d0f607](https://github.com/happytoolin/happycontext/commit/5d0f6078c137f98fca1a240821712b74b430002a))
* enhance event handling and middleware logging ([0180f08](https://github.com/happytoolin/happycontext/commit/0180f08307d69b4f6ad4a036783b4f09864765f4))
* opensourcing ([c97d378](https://github.com/happytoolin/happycontext/commit/c97d3787dcac19bdb716bdf35fa3020bf0a7775a))


### Miscellaneous Chores

* prepare v0.0.1 release ([9ea1198](https://github.com/happytoolin/happycontext/commit/9ea119821fbb72a88cc75b9affdf8ca87cb01e6b))

## Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.
