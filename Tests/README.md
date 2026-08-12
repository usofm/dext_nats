# Dext.Nats test layout

The test executable remains a single Dext.Testing runner, but focused fixtures are organized by responsibility so humans and coding agents do not need to load the historical mega-unit for every change.

```text
Tests/
  Core/          client lifecycle, drain, dispatch, request/reply
  Protocol/      wire parsing/encoding parity and protocol correctness
  Internal/      non-public buffer/dispatcher primitives
  JetStream/     JetStream administration and delivery
  KeyValue/      KV behavior
  ObjectStore/   object-store behavior
  Services/      NATS Services API
  Security/      TLS/NKey/credentials
  DI/            Dext dependency-injection integration
  Observability/ logging/metrics/health checks
  Stress/        explicit stress fixtures
  Benchmarks/    explicit repeatable performance fixtures
```

## Migration rule

`Dext.Net.Nats.Tests.pas` is legacy consolidation debt. Do not add new feature tests to it. When touching an existing fixture there, prefer moving that fixture into its matching feature folder as part of the same change, provided behavior stays unchanged.

The root runner `Dext.Net.Nats.Tests.dpr` is the composition point and should explicitly reference each fixture path. This keeps test discovery deterministic while allowing the implementation to be decomposed gradually.

## Performance refactor

Parser V2 correctness lives under `Protocol/`; V1/V2 performance comparison lives under `Benchmarks/`; bounded-dispatch behavior lives under `Core/`; internal buffer/channel mechanics live under `Internal/`.

A hot-path optimization is not considered complete until it has correctness coverage and, where relevant, a repeatable benchmark.
