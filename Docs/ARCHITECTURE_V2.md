# Dext.Nats Architecture V2

This document defines the refactor path for Dext.Nats after the 1.0 feature-complete baseline.

The primary rule is **public API stability**: existing consumer units such as `Dext.Net.Nats`, `Dext.Net.Nats.JetStream`, `Dext.Net.Nats.KeyValue`, and `Dext.Net.Nats.ObjectStore` remain the supported public surface. Internal implementation can be decomposed without forcing application code to change.

## Goals

1. Reduce oversized units and AI/context cost.
2. Move reusable hot-path mechanics into small internal units.
3. Use Dext-native primitives before introducing custom equivalents.
4. Remove receive-thread head-of-line blocking through an optional bounded dispatcher.
5. Replace parser tail-shifting with read/write cursors.
6. Preserve safe owned-message APIs while adding opt-in high-performance borrowed/Span paths later.
7. Keep benchmarks repeatable and separate from correctness tests.

## Target source layout

```text
Source/
  Dext.Net.Nats.pas
  Dext.Net.Nats.Protocol.pas
  Dext.Net.Nats.NKeys.pas
  Dext.Net.Nats.JetStream.pas
  Dext.Net.Nats.KeyValue.pas
  Dext.Net.Nats.ObjectStore.pas
  Dext.Net.Nats.Services.pas
  Dext.Net.Nats.DependencyInjection.pas
  Dext.Net.Nats.HealthChecks.pas

  Internal/
    Dext.Net.Nats.Internal.Buffer.pas
    Dext.Net.Nats.Internal.Dispatcher.pas

  Protocol/
    Dext.Net.Nats.Protocol.Parser.pas
    Dext.Net.Nats.Protocol.Writer.pas
    Dext.Net.Nats.Protocol.Headers.pas
    Dext.Net.Nats.Protocol.Info.pas

  JetStream/
    Dext.Net.Nats.JetStream.Models.pas
    Dext.Net.Nats.JetStream.Streams.pas
    Dext.Net.Nats.JetStream.Consumers.pas
    Dext.Net.Nats.JetStream.Ordered.pas
    Dext.Net.Nats.JetStream.Json.pas

  KeyValue/
    Dext.Net.Nats.KeyValue.Models.pas
    Dext.Net.Nats.KeyValue.Watcher.pas

  ObjectStore/
    Dext.Net.Nats.ObjectStore.Models.pas
    Dext.Net.Nats.ObjectStore.Reader.pas
    Dext.Net.Nats.ObjectStore.Watcher.pas

  Services/
    Dext.Net.Nats.Services.Endpoint.pas
    Dext.Net.Nats.Services.Group.pas
```

The top-level public units stay as facades/composition roots. Internal units may move over time, but existing consumer `uses` clauses must remain valid.

## Naming rules

- `TDextNats*`: framework-facing classes and services.
- `TNats*`: protocol/config/value records.
- `EDextNats*`: exceptions rooted in the library exception hierarchy.
- `Dext.Net.Nats.Internal.*`: non-public infrastructure that applications should not depend on.
- Feature implementation units use the existing namespace followed by the responsibility, for example `Dext.Net.Nats.JetStream.Consumers`.

Avoid generic names such as `Utils`, `Helpers`, or `Common` when a responsibility-specific name is possible.

## Phase 1 — internal performance primitives

Status: **started**.

- `TDextNatsReadBuffer`: read/write cursor buffer; no per-frame tail shift.
- `TDextNatsBoundedDispatcher<T>`: bounded Dext Channel worker pool with explicit backpressure.
- Dedicated tests in `Dext.Net.Nats.Internal.Tests.pas`.
- Vendored NATS server archive removed; reproducible download script added.

These primitives are introduced and tested before being connected to the client/parser hot paths.

## Phase 2 — parser migration

Replace `TDextNatsFrameParser`'s `FBufferLen + ShiftBuffer` model with `TDextNatsReadBuffer`.

Acceptance criteria:

- framing behavior unchanged;
- incremental fragments still pass;
- multiple frames per receive still pass;
- malformed and oversized frames behave identically;
- benchmark reports parser throughput/allocation delta.

## Phase 3 — bounded message dispatch

Add an opt-in dispatch configuration while preserving inline delivery as the compatibility default.

Proposed modes:

```text
Inline          receive thread invokes handler directly
BoundedWorkers  receive thread enqueues typed work into a bounded Dext Channel
```

The bounded mode must expose capacity/worker settings and a deterministic overflow policy. The implementation must not create a thread per message and should not allocate an anonymous closure per message.

## Phase 4 — split large implementation units

Decompose JetStream, KeyValue, ObjectStore, Services, and the protocol implementation behind stable public facades.

The first candidates are:

1. JetStream models + JSON.
2. JetStream consumer logic.
3. ObjectStore reader/watcher.
4. KeyValue watcher.
5. Protocol parser/writer.

## Phase 5 — split tests by feature

The current mega test unit is intentionally treated as technical debt. Target layout:

```text
Tests/
  Protocol/
  Core/
  JetStream/
  KeyValue/
  ObjectStore/
  Services/
  Security/
  DI/
  Observability/
  Stress/
  Benchmarks/
```

The test runner remains a single Dext.Testing executable while fixture units become small and feature-oriented.

## Phase 6 — advanced performance API

Only after safe APIs are stable and benchmarked:

- borrowed/Span message callback with explicitly bounded lifetime;
- pooled encode buffers;
- optimized header parser/writer;
- possible vectored-write support if added to `Dext.Net.Tcp`;
- allocation and throughput baselines for owned vs borrowed modes.

## Performance rule

No optimization is accepted only because it looks faster. A hot-path change must have:

1. a correctness test,
2. a stress test where relevant,
3. a repeatable benchmark,
4. documented ownership/lifetime semantics.
