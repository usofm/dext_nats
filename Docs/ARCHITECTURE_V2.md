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
    Dext.Net.Nats.Protocol.Writer.pas

  Dext.Net.Nats.ParserRuntime.pas         # TDextNatsRuntimeFrameParser = V2

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
    Dext.Net.Nats.JetStream.Runtime.pas    # compiled; not used by façade yet

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

Status: **V2 is the production runtime parser.**

`TDextNatsFrameParserV2` uses `TDextNatsReadBuffer` and preserves the owned `TNatsFrame` contract. `Dext.Net.Nats.ParserRuntime` aliases `TDextNatsRuntimeFrameParser = TDextNatsFrameParserV2`. The old contiguous/`ShiftBuffer` parser is gone. Parity tests cover control frames, INFO, MSG/HMSG, fragmented input, multi-frame input, Clear and max-frame rejection. A V2 benchmark and compaction assertion are included. Delphi 13 compile/integration of this cut-over is still an unrun gate.

## Phase 3 — bounded message dispatch

Status: **native default on `TDextNatsClient` (1 worker, bounded queue, block/backpressure).**

`Source/Internal/Dext.Net.Nats.Internal.Dispatcher.pas` is the only callback dispatch layer. The obsolete `Dext.Net.Nats.Dispatching.pas` adapter was removed from production. Public `Subscribe` is worker-dispatched. Internal completion subscriptions (`Request` inbox, Fetch inbox via `TDextNatsJetStreamFetcher.SubscribeInline`) use `SubscribeCore(..., InlineDelivery=True)` so nested synchronous waits do not deadlock the sole worker.

**P0 Fetch (source):** `TDextNatsJetStreamContext.Fetch` delegates to `TDextNatsJetStreamFetcher`. Wait-state is an interface gate; `Unsubscribe(sid, 0)` always runs before dropping it. Compiler/live validation still pending.

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

The historical `TDextNatsJetStreamContext` remains the public ABI/API facade and **still contains duplicate implementations** except Fetch, which now delegates to `TDextNatsJetStreamFetcher`. `TDextNatsJetStreamRuntime` is compiled but unused by the façade (P1). At cut-over, remaining facade methods become thin delegators and duplicate implementations can be removed.

**P0 Fetch (source):** façade `Fetch` uses the extracted inline collector. Wait-state is an interface gate; `Unsubscribe(sid, 0)` always runs before the gate is released so a late MSG cannot Enter/SetEvent on freed sync objects.

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

PUB/HPUB/CONNECT live in `Dext.Net.Nats.Protocol.Writer.pas` (`NatsV2Encode*`). PING/PONG/SUB/UNSUB live in `Protocol.Control`. The old `NatsEncode*` duplicates are gone.

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

Status: **partial**. Non-breaking hot-path items below are in source. Remaining items are skipped (breaking API or missing Dext primitive) or wait on compile/live measurement.

1. borrowed/Span MSG callback with explicit lifetime contract — **skipped** (breaking public `TNatsMsgHandler` API);
2. [x] pooled encode buffers — `TDextPool<TDextNatsEncodeScratch>` / `TDextNatsControlScratch` wrap the record writers (`TDextPool<T>` requires `class, constructor`; public `NatsV2Encode*` / `NatsControl*` still return owned `TBytes`; Publish API unchanged). Exhaustion falls back to a stack writer (`AcquireTimeoutMs=0`) so publish never blocks on the pool;
3. [x] RecvLoop `Receive(TByteSpan)` into `TDextNatsReadBuffer.WritableSpan` via `PrepareReceive`/`CommitReceive` (plaintext TCP and TLS plaintext). RecvLoop still owns Receive; TLS encrypted path still uses `FTLSNetworkBuffer`;
4. [x] Span-oriented header parser/writer — `NatsEncodeHeaderBlock` / `NatsDecodeHeaderBlock(TByteSpan)` in `Protocol.Headers`; public `TNatsHeaders` remains `TArray<TPair<string,string>>`. Parser V2 decodes HMSG from `DataSpan.Slice` (no `GetString`). `TNatsHeadersHelper.Encode` and `NatsParseHeaderBlock` delegate to the span codec;
5. optional vectored write — **skipped** (`Dext.Net.Tcp` has no `writev` / vectored send);
6. owned-vs-borrowed allocation/throughput baselines — **open** (needs a compile + `DEXT_NATS_RUN_BENCH=1` run);
7. bounded dispatch is already the default; keep `InlineDelivery` reserved for internal completion subscriptions only.

Also closed this round (not originally numbered):

- [x] JetStream admin JSON without string↔`GetBytes` into `TUtf8JsonReader` when the payload is already `TBytes` (`INatsJetStreamApiTransport.Request` / façade `ApiRequest` return `TBytes`; `NatsJsParse*` and `TNatsStreamInfo.Parse` etc. have `TBytes` overloads; public `Parse(string)` remains);
- [x] `TDextNatsReadBuffer.IndexOfCrLf` via `TDextSimd.IndexOfByte` (CR then LF check; SIMD unit already falls back to Pascal).

Skipped: FastPath/MapFast (N/A — this library is Net, not HTTP).

## Performance rule

No optimization is accepted because it merely looks faster. A hot-path change requires:

1. correctness/parity test;
2. stress test where relevant;
3. repeatable benchmark;
4. documented ownership/lifetime semantics;
5. rollback path until runtime validation succeeds.

## Remaining compile/runtime gate

Hosted Linux GitHub Actions performs structural/hygiene checks only. It does **not** prove Delphi compilation.

Parser V2 and native dispatch are already on `main`. Remaining before treating Architecture V2 as validated:

1. P0 Fetch deadlock/UAF are fixed in source; nested Request (100×) and nested Fetch (50×) tests added (`HandlerWorkerCount = 1`). Compiler/live validation still pending.
2. Run `scripts/build-tests.ps1` on Windows with Delphi 13 (compiler 37.0 is installed; this agent session did not run it).
3. Compile every extracted unit through `Tests/Dext.Net.Nats.Tests.dproj`.
4. Run focused and legacy tests; then `nats-server -js` live tests.
5. Capture Parser V2 benchmark output (expect 0 per-frame compactions on the batch PING case).
6. Convert remaining JetStream public methods into thin delegates to extracted Runtime/services. **Fetch** delegates to an owned `TDextNatsJetStreamFetcher`. Streams/Consumers/Push/Ordered stay duplicated (extracted admin adds empty-name checks; ordered engine is not a drop-in for `TDextNatsOrderedConsumer`).
7. Repeat integration/stress tests (`scripts/validate-parser-cutover.ps1 -Config Release -Platform Win32 -LiveNats -Benchmark`).
8. Apply the same façade/service split to KeyValue, ObjectStore, and Services.
