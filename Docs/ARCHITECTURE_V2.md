# Dext.Nats Architecture V2

This document defines the refactor path for Dext.Nats after the 1.0 feature-complete baseline.

The primary rule is **public API stability**: existing consumer units such as `Dext.Net.Nats`, `Dext.Net.Nats.JetStream`, `Dext.Net.Nats.KeyValue`, `Dext.Net.Nats.ObjectStore`, and `Dext.Net.Nats.Services` remain the supported public surface while implementation is decomposed behind them.

## Goals

1. Reduce oversized units and AI/context cost.
2. Move reusable hot-path mechanics into small internal units.
3. Use Dext-native primitives before custom equivalents.
4. Remove receive-thread head-of-line blocking through optional bounded dispatch.
5. Replace parser tail-shifting with read/write cursors.
6. Preserve safe owned-message APIs while preparing opt-in borrowed/Span paths.
7. Keep benchmarks repeatable and separate from correctness tests.
8. Keep the refactor reversible until Delphi 13 compile/integration validation passes.

## Current source layout

```text
Source/
  Dext.Net.Nats.pas                     # public facade/client
  Dext.Net.Nats.Protocol.pas            # public compatibility facade
  Dext.Net.Nats.JetStream.pas           # public compatibility facade
  Dext.Net.Nats.KeyValue.pas            # public compatibility facade
  Dext.Net.Nats.ObjectStore.pas         # public compatibility facade
  Dext.Net.Nats.Services.pas            # public compatibility facade

  Internal/
    Dext.Net.Nats.Internal.Buffer.pas
    Dext.Net.Nats.Internal.Dispatcher.pas
    Dext.Net.Nats.Internal.Parser.pas

  Protocol/
    Dext.Net.Nats.Protocol.Headers.pas
    Dext.Net.Nats.Protocol.Control.pas

  JetStream/
    Dext.Net.Nats.JetStream.Json.pas
    Dext.Net.Nats.JetStream.Codecs.pas
    Dext.Net.Nats.JetStream.Parsers.pas
    Dext.Net.Nats.JetStream.Paging.pas
    Dext.Net.Nats.JetStream.ObjectPaging.pas
    Dext.Net.Nats.JetStream.Transport.pas
    Dext.Net.Nats.JetStream.Streams.pas
    Dext.Net.Nats.JetStream.Consumers.pas
    Dext.Net.Nats.JetStream.Fetch.pas
    Dext.Net.Nats.JetStream.Push.pas
    Dext.Net.Nats.JetStream.Ordered.pas

  KeyValue/
    Dext.Net.Nats.KeyValue.Subjects.pas

  ObjectStore/
    Dext.Net.Nats.ObjectStore.Subjects.pas
    Dext.Net.Nats.ObjectStore.Crypto.pas

  Services/
    Dext.Net.Nats.Services.Subjects.pas
```

## Naming rules

- `TDextNats*`: framework-facing classes/services.
- `TNats*`: protocol/config/value records.
- `EDextNats*`: library exception hierarchy.
- `Dext.Net.Nats.Internal.*`: non-public infrastructure.
- Feature implementation units use responsibility-specific suffixes such as `.Streams`, `.Consumers`, `.Fetch`, `.Subjects`, `.Headers`.
- Avoid generic `Utils`, `Helpers`, or `Common` units.

## Phase 1 — internal performance primitives

Status: **implemented**.

- `TDextNatsReadBuffer`: cursor-based read/write buffer.
- `CompactionCount`: proves physical compaction frequency.
- `TDextNatsBoundedDispatcher<T>`: bounded `Dext.Collections.Channels` worker pool with explicit backpressure.
- Vendored NATS server archive removed; reproducible download script added.

## Phase 2 — parser migration

Status: **V2 implementation + parity + benchmark ready; runtime cut-over gated by Delphi compile/integration**.

`TDextNatsFrameParserV2` uses `TDextNatsReadBuffer` and preserves the owned `TNatsFrame` contract. Parity tests cover control frames, INFO, MSG/HMSG, fragmented input, multi-frame input, Clear and max-frame rejection. A V1/V2 benchmark and compaction assertion are included.

The old parser remains the runtime parser until the self-hosted Delphi 13 gate passes. This is intentional rollback safety, not forgotten work.

## Phase 3 — bounded message dispatch

Status: **opt-in implementation ready**.

`Dext.Net.Nats.Dispatching.pas` adds bounded worker dispatch backed by Dext Channels. Existing inline `Subscribe` behavior remains unchanged for compatibility.

```text
Inline          existing TDextNatsClient.Subscribe behavior
BoundedWorkers  opt-in channel-backed worker dispatch
```

## Phase 4 — JetStream decomposition

Status: **all major implementation seams extracted; facade delegation/cut-over gated by Delphi 13**.

### Serialization and parsing

- `Json`: UTF-8 sink and small request builders.
- `Codecs`: stream/consumer/purge serialization with parity tests.
- `Parsers`: StreamInfo, ConsumerInfo, StoredMsg, PublishAck and success/error parsing.
- `Paging`: paged name responses.
- `ObjectPaging`: paged StreamInfo/ConsumerInfo responses while reusing canonical parsers.

### API/service boundaries

- `Transport`: `INatsJetStreamApiTransport` plus `TDextNatsClient` adapter.
- `Streams`: create/update/info/exists/delete/purge/list names/list objects/get stored message.
- `Consumers`: create/info/delete/list names/list objects.
- `Fetch`: pull-consumer inbox/batch/timeout lifecycle.
- `Push`: push subscription orchestration and consumer deliver-subject resolution.
- `Ordered`: extracted ordered-consumer engine/state machine prepared for facade cut-over.

The historical `TDextNatsJetStreamContext` remains the public ABI/API facade until the Windows Delphi gate validates all extracted units. At cut-over, facade methods become thin delegators and duplicate implementations can be removed.

## Phase 5 — large non-JetStream units

Status: **first extraction seams implemented**.

### KeyValue

`Dext.Net.Nats.KeyValue.Subjects.pas` owns bucket/key/search-key validation, stream/subject mapping, TTL validation and operation-header decoding. This is shared by Get/Put/History/Watch paths and is independently tested.

Next physical move after compile gate: watcher lifecycle and entry mapping out of the public facade.

### ObjectStore

- `Dext.Net.Nats.ObjectStore.Subjects.pas`: OBJ stream name, Base64URL object-name mapping, metadata/chunk subjects and bucket-character contract.
- `Dext.Net.Nats.ObjectStore.Crypto.pas`: NUID and SHA-256 wire digest helpers.

Next physical move after compile gate: lazy reader and watcher lifecycle.

### Services

`Dext.Net.Nats.Services.Subjects.pas` owns ADR-32 discovery subjects, prefix joining and service-name validation. Endpoint/group lifecycle remains in the public facade until compile validation.

### Protocol

- `Dext.Net.Nats.Protocol.Headers.pas`: extracted header codec with parity fixture.
- `Dext.Net.Nats.Protocol.Control.pas`: extracted PING/PONG/SUB/UNSUB writer with parity fixture.
- Parser V2 already lives in `Internal.Parser`.

The remaining PUB/HPUB/CONNECT writer will be migrated after the extracted units compile on Delphi 13, because that path is throughput-sensitive and should be benchmarked before replacing the current byte writer.

## Phase 6 — feature-oriented tests

Status: **active and enforced by CI**.

New/refactored tests now live under:

```text
Tests/
  Core/
  Internal/
  Protocol/
  JetStream/
  KeyValue/
  ObjectStore/
  Services/
  Benchmarks/
```

The historical `Tests/Dext.Net.Nats.Tests.pas` mega-unit remains temporary debt. New tests must not be added to it. Existing fixtures migrate out when the corresponding production feature is touched.

## Phase 7 — advanced performance API

Status: **not cut over yet; design prepared**.

After compile/integration stability:

1. borrowed/Span callback with explicit lifetime contract;
2. pooled encode buffers;
3. Span-oriented header parser/writer;
4. optional vectored write if `Dext.Net.Tcp` exposes a suitable primitive;
5. owned-vs-borrowed allocation/throughput baselines;
6. evaluate making bounded dispatch the recommended server-workload mode while preserving inline compatibility.

## Performance rule

No optimization is accepted because it merely looks faster. A hot-path change requires:

1. correctness/parity test;
2. stress test where relevant;
3. repeatable benchmark;
4. documented ownership/lifetime semantics;
5. rollback path until runtime validation succeeds.

## Remaining compile/runtime gate

Hosted Linux GitHub Actions performs structural/hygiene checks only. It does **not** prove Delphi compilation.

Before merging Architecture V2 to `main`:

1. run `scripts/build-tests.ps1` on Windows with Delphi 13;
2. compile every extracted unit through `Tests/Dext.Net.Nats.Tests.dproj`;
3. run focused and legacy tests;
4. run integration tests against `nats-server -js`;
5. capture Parser V1/V2 benchmark output;
6. resolve any Delphi language/API mismatches;
7. cut over `TDextNatsClient` to Parser V2 behind a reversible define first;
8. convert JetStream public methods into thin delegates to extracted services;
9. repeat integration/stress tests;
10. only then remove duplicated old implementations and merge the PR.
