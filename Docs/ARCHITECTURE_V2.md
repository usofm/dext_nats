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
    Dext.Net.Nats.Internal.Parser.pas

  JetStream/
    Dext.Net.Nats.JetStream.Models.pas
    Dext.Net.Nats.JetStream.Json.pas
    Dext.Net.Nats.JetStream.Codecs.pas
    Dext.Net.Nats.JetStream.Parsers.pas
    Dext.Net.Nats.JetStream.Transport.pas
    Dext.Net.Nats.JetStream.Streams.pas
    Dext.Net.Nats.JetStream.Consumers.pas
    Dext.Net.Nats.JetStream.Ordered.pas

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

Status: **implemented on the V2 refactor branch**.

- `TDextNatsReadBuffer`: read/write cursor buffer; no per-frame tail shift.
- `TDextNatsBoundedDispatcher<T>`: bounded Dext Channel worker pool with explicit backpressure.
- Dedicated tests under `Tests/Internal/`.
- `CompactionCount` instrumentation proves when a physical unread-tail move actually occurs.
- Vendored NATS server archive removed; reproducible download script added.

## Phase 2 — parser migration

Status: **parity implementation ready; runtime cut-over pending Delphi compile/test**.

`TDextNatsFrameParserV2` in `Dext.Net.Nats.Internal.Parser.pas` uses `TDextNatsReadBuffer` and returns the same owned `TNatsFrame` contract as the current parser.

Parity coverage is isolated in `Tests/Protocol/Dext.Net.Nats.ParserV2.Tests.pas` and compares V1/V2 behavior for PING/PONG/+OK/-ERR/INFO, MSG, fragmented MSG, multi-frame input, HMSG, Clear, and max-frame rejection.

`Tests/Benchmarks/Dext.Net.Nats.ParserV2.Benchmarks.pas` provides an explicit V1-vs-V2 throughput benchmark and a non-explicit assertion that multi-frame consumption performs zero physical compactions.

Acceptance criteria before runtime cut-over:

- Delphi build succeeds;
- parity fixture passes;
- existing protocol/integration fixtures still pass after cut-over;
- benchmark reports V1/V2 throughput and does not show a material regression;
- multi-frame batch consumption does not compact per frame.

## Phase 3 — bounded message dispatch

Status: **opt-in implementation ready; legacy inline Subscribe remains unchanged**.

`Dext.Net.Nats.Dispatching.pas` adds a dispatched subscription backed by `TDextNatsBoundedDispatcher<TNatsMsg>` and `Dext.Collections.Channels`.

The compatibility model remains:

```text
Inline          existing TDextNatsClient.Subscribe behavior
BoundedWorkers  opt-in dispatched subscription with bounded Dext Channel
```

The bounded mode exposes capacity/worker settings and deterministic full-queue behavior. It does not create a thread per message.

## Phase 4 — split large implementation units

Status: **JetStream extraction in progress**.

Extracted JetStream implementation units now include:

```text
Source/JetStream/
  Dext.Net.Nats.JetStream.Json.pas
  Dext.Net.Nats.JetStream.Codecs.pas
  Dext.Net.Nats.JetStream.Parsers.pas
  Dext.Net.Nats.JetStream.Transport.pas
  Dext.Net.Nats.JetStream.Streams.pas
```

Responsibilities:

- `Json`: allocation-conscious UTF-8 sink and small request-body builders.
- `Codecs`: stream/consumer/purge serialization with byte-for-byte parity tests against the existing public records.
- `Parsers`: StreamInfo, ConsumerInfo, StoredMsg, PublishAck and success/error response parsing with parity/error-semantics tests.
- `Transport`: the small `INatsJetStreamApiTransport` request/reply boundary plus production `TDextNatsClient` adapter.
- `Streams`: extracted stream administration core for create/update/info/exists/delete/purge and stored-message get operations.

`TDextNatsJetStreamStreams` is intentionally tested through a fake transport. This verifies `$JS.API.*` subject suffixes, JSON request bodies, timeout forwarding and response/error parsing without requiring a live server.

`ListStreams` and `ListStreamNames` remain temporarily in the facade until paged response parsing is extracted. After that, the complete stream administration surface can delegate to `Streams.pas`.

During this transition the historical facade remains authoritative until Delphi 13 compile/tests confirm the extracted implementation. After that gate, existing `TDextNatsJetStreamContext` methods will delegate to the extracted services and duplicate private helpers will be removed.

Next extraction order:

1. paged stream-name / stream-info parsing and list operations;
2. consumer administration + Fetch;
3. ordered consumer implementation;
4. ObjectStore reader/watcher;
5. KeyValue watcher;
6. Services and protocol writer/parser separation.

No public unit rename is allowed during this phase.

## Phase 5 — split tests by feature

Status: **in progress**.

Focused tests are now organized as:

```text
Tests/
  Core/
    Dext.Net.Nats.Drain.Tests.pas
    Dext.Net.Nats.Dispatching.Tests.pas
  Internal/
    Dext.Net.Nats.Internal.Tests.pas
  Protocol/
    Dext.Net.Nats.ParserV2.Tests.pas
  JetStream/
    Dext.Net.Nats.JetStream.Json.Tests.pas
    Dext.Net.Nats.JetStream.Streams.Tests.pas
  Benchmarks/
    Dext.Net.Nats.ParserV2.Benchmarks.pas
```

`Tests/README.md` defines the complete target layout and migration rule. The historical `Dext.Net.Nats.Tests.pas` mega-unit remains temporary technical debt; new tests must not be added to it. Existing fixtures will move out feature-by-feature when touched.

The root Dext.Testing runner remains the single composition point and explicitly references the feature-folder paths.

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

## Merge gate for Architecture V2

Before the branch is merged to `main`:

1. hosted structure/hygiene CI must be green;
2. `scripts/build-tests.ps1` must compile the Dext.Testing project on Delphi 13;
3. parser parity tests must pass;
4. dispatched-subscription tests must pass;
5. JetStream extracted JSON/codec/parser/stream-admin tests must pass;
6. existing integration tests should run against a local `nats-server -js`;
7. the parser benchmark must be captured for V1 and V2;
8. public API compatibility must be reviewed from the PR diff.
