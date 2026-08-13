# Dext.Nats — Architecture Change Report & Codex Test Plan

> Repository: `usofm/dext_nats`  
> Working branch at writing: `perf/parser-v2-cutover` (merged to `main` in PR #2 / `7f510e7`)  
> Current `main` HEAD (this execution): `8b19853` (Phase 7 compile-fix; see git log for span-headers / SIMD / docs / Fetch-reuse)  
> Fetch-inline commit still in history: `51f3d3d3d28287dc7e518bf8acddc89eef6fbb32`  
> Target compiler: **Delphi 13 / RAD Studio** (compiler 37.0 is installed on this machine)  
> Target NATS runtime: NATS Server with JetStream (`nats-server` **v2.14.5** at `.tools/nats-server-v2.14.5-windows-amd64`; v2.12.3 also present under Comp12 `nats.server`)  
> Status: **NOT READY TO MERGE** (full matrix / Release / Nested Request 100× / stress / bench still open). P0 Fetch deadlock + UAF are **fixed in source** and **nested Fetch 50× passed live** on nats-server 2.14.5 `-js`. Debug Win32 rebuild **succeeded** (`scripts/build-tests.ps1 -Config Debug -Platform Win32`). OrderedConsumer + PurgeDeletes live tests also passed on 2.14.5.

**Agent paths (this machine):** canonical Dext source is `C:\apps_delphi\Comp12\dext` (not `C:\dev\comp\dext`). Dext AI Coding Pack is `C:\apps_delphi\Comp12\DEXT_AI_CODING_PACK` (`v2026.08.12-r3-dext-412ed292`). This library is Net/protocol — pack ORM/web skills do not apply. See `AGENTS.md` for which pack files to load. Fetch P0 deadlock/UAF are fixed in source and nested-Fetch live passed on 2.14.5. Remaining merge blockers: Release/Win64 rebuild, Nested Request 100×, remaining live matrix, stress/bench, and the P1 façade→Runtime cut-over (Streams/Consumers/Push/Ordered still duplicated).

---

## 1. Purpose of this document

This document records the architectural and performance-oriented changes made to `Dext.Nats`, explains **why** each change was introduced, and defines the exact test plan that Codex should execute on a Windows machine with Delphi 13 installed.

The primary goals of the refactor are:

- remove duplicate protocol implementations;
- remove per-frame parser buffer copying;
- keep socket/TLS receive work lightweight;
- move user callbacks away from the NATS receive loop;
- preserve message ordering and backpressure semantics;
- reduce lock contention around network I/O;
- split very large units into focused feature units;
- make the project easier to maintain and safer for AI-assisted development;
- retain exact NATS wire behavior while improving runtime architecture.

Because the library has no production downstream consumer yet, this branch intentionally prefers a cleaner first public baseline over carrying compatibility debt.

---

# Part A — Change Report

## 2. Parser V2 became the only production parser

### Previous design

The original protocol parser used a contiguous `TBytes` buffer together with a `ShiftBuffer`-style design. After consuming a frame, unread bytes could be moved toward the start of the buffer.

This creates an unnecessary memory-copy cost under sustained traffic, especially when many frames are already buffered.

### New design

The new parser uses:

- `TDextNatsReadBuffer`
- `FReadPos`
- `FWritePos`
- cursor-based consumption
- compaction only when necessary

Main implementation units:

```text
Source/Internal/Dext.Net.Nats.Internal.Buffer.pas
Source/Internal/Dext.Net.Nats.Internal.Parser.pas
Source/Dext.Net.Nats.ParserRuntime.pas
```

The original `TDextNatsFrameParser` path was removed. The runtime parser facade now permanently resolves to `TDextNatsFrameParserV2`.

### Why this change was necessary

The old architecture could perform a tail `Move()` after normal frame consumption. That makes parser cost depend not only on the frame being consumed but also on the amount of unread data behind it.

The V2 cursor model makes normal frame consumption effectively O(1) with respect to unread tail data.

### Important regression guard

The following must never return:

```text
TDextNatsFrameParser   // old parser class
ShiftBuffer
FBufferLen             // old parser length architecture
```

### Tests added/updated

```text
Tests/Protocol/Dext.Net.Nats.ParserV2.Tests.pas
Tests/Protocol/Dext.Net.Nats.ParserRuntime.Tests.pas
Tests/Benchmarks/Dext.Net.Nats.ParserV2.Benchmarks.pas
```

The tests now validate the production parser directly instead of comparing it against a retired parser implementation.

---

## 3. Protocol encoding was split into specialized units

### Previous design

`Dext.Net.Nats.Protocol.pas` contained both protocol contracts/types and outbound frame encoder implementations.

During refactoring, specialized protocol units were introduced but duplicate encoders still existed temporarily in the original protocol unit.

### New design

Outbound encoding is now separated into:

```text
Source/Protocol/Dext.Net.Nats.Protocol.Writer.pas
Source/Protocol/Dext.Net.Nats.Protocol.Control.pas
Source/Protocol/Dext.Net.Nats.Protocol.Headers.pas
```

Runtime usage is now:

```text
CONNECT -> NatsV2EncodeConnect
PUB     -> NatsV2EncodePub
HPUB    -> NatsV2EncodeHPub
SUB     -> NatsControlSub
UNSUB   -> NatsControlUnsub
PING    -> NatsControlPing
PONG    -> NatsControlPong
```

The old duplicate functions were removed:

```text
NatsEncodeConnect
NatsEncodePub
NatsEncodeHPub
NatsEncodeSub
NatsEncodeUnsub
NatsEncodePing
NatsEncodePong
```

### Why this change was necessary

Maintaining two implementations of the same wire protocol is dangerous:

- fixes can be applied to one path but not the other;
- tests may accidentally validate a non-production implementation;
- AI/code search has to load more unrelated context;
- performance work becomes harder to reason about.

The specialized writer/control units now define the canonical outbound wire path.

### PING/PONG optimization

`PING\r\n` and `PONG\r\n` are cached frames so keepalive traffic does not allocate a new byte array for every call.

---

## 4. `FlushOutbox` no longer performs network I/O while holding `FLock`

### Previous design problem

The shared client state lock could remain held while queued outbound frames were sent to the socket/TLS layer.

Network operations can stall unpredictably. Holding a broad state lock during I/O expands the lock critical section from microseconds to potentially milliseconds or more.

### New behavior

`FlushOutbox` now follows this sequence:

```text
FLock.Enter
    dequeue one frame
    update pending byte counters
FLock.Leave

SendRaw(frame)
```

The loop then repeats.

### Why this change was necessary

This prevents network latency from blocking unrelated state operations such as subscription bookkeeping, connection-state reads, or other client operations guarded by `FLock`.

FIFO ordering is retained because the outbox is still dequeued in order.

### Required regression rule

No `SendRaw()` call should be introduced inside the `FLock` critical section of `FlushOutbox`.

---

## 5. Subscription application callbacks were moved off `RecvLoop`

### Previous design

The receive thread parsed a frame and then directly invoked:

```pascal
handler(msg);
```

This means a slow application callback blocked:

```text
socket receive
-> protocol parsing
-> PING/PONG handling
-> all following message delivery
```

One slow callback therefore caused head-of-line blocking for the entire connection.

### New architecture

The client now owns a bounded dispatcher backed by Dext Channels:

```text
Socket / TLS
    |
    v
RecvLoop
    |
    v
Parser V2
    |
    v
TNatsMsg
    |
    v
Bounded Dext Channel
    |
    v
Handler Worker
    |
    v
Application callback
```

Important implementation unit:

```text
Source/Internal/Dext.Net.Nats.Internal.Dispatcher.pas
```

The public client owns:

```text
TDextNatsBoundedDispatcher<TNatsHandlerDispatchItem>
```

### Current defaults

```text
HandlerWorkerCount   = 1
HandlerQueueCapacity = 8192
Overflow policy      = block/backpressure
```

### Why one worker by default

A single callback worker preserves global callback ordering without running user code on the socket receive thread.

More workers can increase parallel callback throughput but change completion ordering, so concurrency should be an explicit decision rather than the default.

### Why blocking backpressure is the default

Silent message loss is unacceptable as a baseline client behavior.

When the application cannot keep up and the bounded queue becomes full, RecvLoop is allowed to slow down. TCP/NATS then naturally propagates backpressure instead of dropping already accepted messages.

---

## 6. Dispatcher shutdown now drains accepted work

### Previous risk

Closing a dispatcher incorrectly can terminate workers before already accepted queue items are executed.

### New semantics

Shutdown is intended to:

1. close the channel for new writes;
2. continue consuming already accepted queue items;
3. finish active callbacks;
4. join worker threads;
5. only then free dispatcher resources.

### Why this change was necessary

A graceful NATS `Drain` must distinguish between:

- no new subscription interest;
- messages already received/queued;
- callbacks still executing.

Already accepted callback work should not vanish during shutdown.

### Relevant test

The internal dispatcher test suite includes drain-oriented verification for accepted work.

---

## 7. `FInFlightHandlers` now represents queued + executing callbacks

### Previous meaning

It originally represented callbacks executing directly on the receive thread.

### New meaning

The counter is incremented before a callback work item is accepted by the dispatcher and decremented after callback completion.

Therefore:

```text
FInFlightHandlers = queued callbacks + executing callbacks
```

### Why this matters

`TDextNatsClient.Drain()` can now wait for the complete callback pipeline instead of merely checking whether a worker happens to be executing at that instant.

---

## 8. Nested synchronous `Request()` deadlock was prevented

### Deadlock discovered during self-review

With one callback worker, consider:

```text
Application callback starts on worker #1
    -> callback calls Client.Request()
       -> Request waits for reply
       -> reply is received by RecvLoop
       -> reply callback is queued to worker #1
       -> worker #1 is blocked waiting for reply
```

This is a classic self-deadlock.

### Fix

The low-level subscription implementation has an `InlineDelivery` capability for **internal completion subscriptions only**.

The synchronous `Request()` private inbox completion handler is allowed to execute inline on RecvLoop because it performs only completion mechanics such as:

```text
claim gate
copy reply record
event.SetEvent
```

It is not an application callback.

Public `Subscribe()` still uses worker dispatch.

### Why this design was chosen

It preserves both goals:

- user code never blocks RecvLoop;
- synchronous request/reply can safely be invoked from a user callback running on the sole worker.

---

## 9. JetStream `Fetch()` received the same deadlock protection

### Problem

JetStream Fetch creates a temporary inbox subscription and blocks waiting for one or more messages.

If Fetch is called from the sole callback worker and its private inbox is also worker-dispatched, the same self-deadlock can occur.

### Intended fix (only half-applied)

Commit `51f3d3d` extracted `TDextNatsJetStreamFetcher` with a cracker `SubscribeInline` → `SubscribeCore(..., InlineDelivery=True)` so the private inbox collector can complete on RecvLoop.

**P0 — public façade does not use that path.** `TDextNatsJetStreamContext.Fetch` in `Source/Dext.Net.Nats.JetStream.pas` still calls public `FClient.Subscribe` (dispatcher, default 1 worker, `dopBlock`). Nested Fetch / KV / Object Store from a user handler on that worker deadlocks. `TDextNatsJetStreamRuntime` owns a `TDextNatsJetStreamFetcher` but the façade never constructs or delegates to Runtime.

**P0 — use-after-free on both Fetch implementations.** On `wrSignaled`, neither `JetStream.pas` Fetch nor `JetStream.Fetch.pas` unsubscribes the inbox before the `finally` that frees `Lock` / `Done`. `Unsubscribe(sid, batch+5)` can leave the SUB live; a late MSG/HMSG can still enter the anonymous handler and touch freed sync objects.

### Required invariant

Only framework/internal completion subscriptions may request inline delivery.

Normal user-visible subscriptions must remain dispatcher-based.

Public `TDextNatsJetStreamContext.Fetch` (and any KV/OS path that calls it) must use the inline completion collector, then unsubscribe/drain before freeing handler-captured `TEvent` / `TCriticalSection`.

---

## 10. Obsolete double-dispatch adapter was removed

An earlier architecture introduced:

```text
Dext.Net.Nats.Dispatching.pas
TDextNatsDispatchedSubscription
```

That was useful while the main client still invoked handlers directly on RecvLoop.

Once bounded callback dispatch became native to `TDextNatsClient`, retaining the adapter would create:

```text
RecvLoop
 -> core dispatcher
    -> adapter dispatcher
       -> user callback
```

That means unnecessary queues, workers, allocations, latency, shutdown complexity, and duplicated backpressure semantics.

Therefore the opt-in adapter was removed from the production source graph (`Source/Dext.Net.Nats.Dispatching.pas` is gone; not referenced from `Tests/Dext.Net.Nats.Tests.dpr`).

`Tests/Dext.Net.Nats.Dispatching.Tests.pas` was deleted (it was not in the `.dpr` and still `uses` the removed unit). Duplicate Drain/Internal units may still remain at `Tests/` root beside the feature-folder copies the `.dpr` actually compiles.

The canonical architecture now has exactly one callback dispatch layer (`TDextNatsBoundedDispatcher` in `Internal.Dispatcher`).

---

## 11. Test structure was reorganized by feature

The project is being moved away from a giant single test unit.

Current direction:

```text
Tests/
├── Core/
├── Internal/
├── Protocol/
├── JetStream/
├── KeyValue/
├── ObjectStore/
├── Services/
└── Benchmarks/
```

### Why this change was necessary

Large monolithic test units cause:

- large AI context loads;
- poor test ownership;
- difficult navigation;
- higher merge-conflict probability;
- harder incremental refactoring.

Feature-focused test units make it easier for both developers and Codex to load only the relevant context.

The old mega test unit still exists temporarily for tests not yet migrated. New tests should go into feature folders.

---

## 12. JetStream decomposition has started

The original `Dext.Net.Nats.JetStream.pas` is very large. Focused implementation units have already been introduced, including:

```text
Source/JetStream/Dext.Net.Nats.JetStream.Json.pas
Source/JetStream/Dext.Net.Nats.JetStream.Codecs.pas
Source/JetStream/Dext.Net.Nats.JetStream.Parsers.pas
Source/JetStream/Dext.Net.Nats.JetStream.Paging.pas
Source/JetStream/Dext.Net.Nats.JetStream.ObjectPaging.pas
Source/JetStream/Dext.Net.Nats.JetStream.Transport.pas
Source/JetStream/Dext.Net.Nats.JetStream.Streams.pas
Source/JetStream/Dext.Net.Nats.JetStream.Consumers.pas
Source/JetStream/Dext.Net.Nats.JetStream.Fetch.pas
Source/JetStream/Dext.Net.Nats.JetStream.Push.pas
Source/JetStream/Dext.Net.Nats.JetStream.Ordered.pas
Source/JetStream/Dext.Net.Nats.JetStream.Runtime.pas
```

### Why this is not finished yet

**P1 — Architecture V2 extract unused by the public façade.** `TDextNatsJetStreamContext` still contains the full Fetch/Streams/Consumers implementations. `TDextNatsJetStreamRuntime` is compiled into the test project but never constructed by the façade. Nested Fetch therefore cannot pick up `SubscribeInline` until the façade delegates (or inlines the same collector).

Many extracted implementation units still consume public types declared in the giant facade unit.

If the facade directly imports `Runtime`, while Runtime/Streams/Consumers import the facade for model types, a circular unit dependency is created.

### Planned next architecture

```text
Dext.Net.Nats.JetStream.Types / Models
        ^
        |
   +----+--------------------+
   |                         |
Facade                  Runtime/services
```

The public types/models must be extracted first. Then both facade and runtime services can depend on the types unit without circular dependency.

After that, the giant facade can become primarily a public compatibility/API surface and delegate actual behavior to focused services.

---

## 13. KeyValue, ObjectStore and Services decomposition

Focused helper units have also started in these areas:

```text
Source/KeyValue/
Source/ObjectStore/
Source/Services/
```

Examples include subject generation, watcher gates, routing, validation, crypto, and reader functionality.

The same architectural rule applies:

> Large public facade units should eventually coordinate focused services rather than contain every implementation detail themselves.

---

## 14. Repository hygiene and build tooling

Binary NATS tooling is no longer committed into the repository.

The repository uses scripts to obtain tools when needed.

Relevant tooling includes:

```text
scripts/build-tests.ps1
scripts/validate-parser-cutover.ps1
scripts/download-nats-server.ps1
.github/workflows/delphi-build.yml
```

The hosted Linux CI performs structural checks only.

A real compiler gate requires Windows + RAD Studio / Delphi 13.

---

# Part B — Required Codex Test Plan

## 15. Instructions to Codex

Codex must treat this document as the acceptance checklist for the branch.

### Hard rules

1. **Do not weaken tests simply to obtain a green build.**
2. Fix the implementation when a valid test exposes a defect.
3. Do not reintroduce Parser V1 or legacy protocol encoders.
4. Do not move user callbacks back onto RecvLoop.
5. Do not silently drop callback messages to make stress tests pass.
6. Do not introduce network I/O while holding `FLock`.
7. Keep Dext-native primitives where they already exist (`Dext.Collections.Channels`, Dext testing infrastructure, etc.).
8. Run a clean rebuild, not an incremental compile.
9. Record every compiler/test failure and the exact fix.
10. If a fix changes architecture or semantics, update this document and `Docs/ARCHITECTURE_V2.md`.

---

## 16. Phase 0 — Environment verification

Codex must first report:

```text
Windows version
Delphi/RAD Studio version
BDS path
MSBuild path
Win32 compiler availability
Win64 compiler availability
NATS server version
Git branch
Git commit SHA
```

Expected branch:

```text
perf/parser-v2-cutover
```

Do not continue on the wrong branch.

---

## 17. Phase 1 — Static architecture checks

Before compiling, verify these files exist:

```text
Source/Internal/Dext.Net.Nats.Internal.Buffer.pas
Source/Internal/Dext.Net.Nats.Internal.Parser.pas
Source/Internal/Dext.Net.Nats.Internal.Dispatcher.pas
Source/Dext.Net.Nats.ParserRuntime.pas
Source/Protocol/Dext.Net.Nats.Protocol.Writer.pas
Source/Protocol/Dext.Net.Nats.Protocol.Control.pas
Source/Protocol/Dext.Net.Nats.Protocol.Headers.pas
```

Verify the old adapter does **not** exist:

```text
Source/Dext.Net.Nats.Dispatching.pas
Tests/Core/Dext.Net.Nats.Dispatching.Tests.pas
```

Search the repository and fail the phase if any production source contains:

```text
TDextNatsFrameParser      // exact legacy class, not V2
ShiftBuffer
FBufferLen
NatsEncodeConnect
NatsEncodePub
NatsEncodeHPub
NatsEncodeSub
NatsEncodeUnsub
NatsEncodePing
NatsEncodePong
```

Note: do not treat unrelated identifiers such as `NatsEncodePublicKey` as protocol encoder regressions.

---

## 18. Phase 2 — Clean Delphi compilation

Run the project using the provided build script first:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-tests.ps1 -Config Debug -Platform Win32
```

Then:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-tests.ps1 -Config Release -Platform Win32
```

If Win64 is supported by all dependencies, also run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-tests.ps1 -Config Release -Platform Win64
```

### Required result

```text
0 compiler errors
0 linker errors
```

Warnings should be reviewed. Do not ignore new warnings caused by this branch, especially:

```text
unsafe casts
uninitialized managed records
implicit string conversions
possible data loss
unused local variables caused by incomplete refactor
```

### Special compiler-risk areas to inspect

Codex should pay close attention to:

- generic anonymous-method compatibility in `TDextNatsBoundedDispatcher<T>`;
- protected/internal subscription access used by JetStream Fetch;
- record fields containing managed types (`string`, `TBytes`, anonymous methods);
- `TInterlocked` overload resolution;
- `IChannel<T>.Read` behavior after channel close;
- test framework fixture registration;
- `.dpr` and `.dproj` paths after feature test migration.

---

## 19. Phase 3 — Unit tests without live NATS

Run the complete non-explicit/non-live test suite.

Required categories include at least:

```text
Protocol
Parser
ParserRuntime
Internal Buffer
Internal Dispatcher
Drain unit tests
JetStream JSON/codecs/parsers
JetStream paging
KeyValue helpers
ObjectStore helpers
Services helpers
```

### Parser acceptance tests

Verify:

- `PING`
- `PONG`
- `+OK`
- `-ERR`
- `INFO`
- `MSG`
- `HMSG`
- fragmented control lines
- fragmented payloads
- multiple frames in one buffer
- `Clear()` behavior
- malformed control frames
- maximum frame size enforcement

### Buffer acceptance tests

Verify:

- consume advances cursor without corrupting unread bytes;
- append after consume reuses capacity when possible;
- normal batched frame consumption does not compact per frame;
- compaction preserves unread data exactly.

### Dispatcher acceptance tests

Verify:

- accepted items execute;
- FIFO order with one worker;
- bounded `TryDispatch` reports full queue correctly;
- blocking dispatch resumes when capacity becomes available;
- callback exception does not terminate the worker;
- `Stop()` drains accepted items;
- writes after close/stopped state are rejected;
- repeated `Stop()` is safe.

---

## 20. Phase 4 — Local NATS integration environment

Start a local NATS server with JetStream enabled.

Use the project script when possible:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\download-nats-server.ps1
```

Start server approximately as:

```text
nats-server -js
```

Use an isolated test data directory if supported by the script/environment.

Verify:

```text
client port reachable
JetStream enabled
no production NATS instance is used
```

---

## 21. Phase 5 — Core live NATS tests

### 21.1 Connect / disconnect

Test:

```text
Connect
INFO parse
CONNECT handshake
PING/PONG handshake
Disconnect
Reconnect
```

Repeat connect/disconnect at least 100 times and check for:

```text
AV
thread leaks
socket leaks
hangs
stale FConnected state
```

### 21.2 Publish / subscribe

Test:

```text
PUB no payload
PUB UTF-8 payload
PUB binary payload
PUB reply-to
SUB
UNSUB
UNSUB max messages
queue groups
HPUB headers
```

Validate exact payload and header round-trip.

### 21.3 Request / reply

Test:

```text
Request success
Request timeout
No responders / status 503
RequestWithHeaders
RequestAsync
many concurrent requests
```

### Critical nested-request test

This test is mandatory.

Scenario:

1. Configure `HandlerWorkerCount = 1`.
2. Subscribe to `outer.request`.
3. Inside that user callback call synchronous `Client.Request('inner.request', ...)`.
4. Another responder replies to `inner.request`.
5. Ensure the nested Request completes successfully.

Required result:

```text
No deadlock
No timeout caused by callback queue starvation
outer handler eventually completes
```

Repeat at least 100 iterations.

---

## 22. Phase 6 — Callback dispatcher behavioral tests

### 22.1 RecvLoop isolation

Create a subscription whose handler deliberately sleeps, for example 500 ms.

At the same time send enough traffic to require continued receive/parsing activity.

Verify that:

- RecvLoop remains able to receive and parse until queue capacity is reached;
- PING/PONG/control frame handling continues;
- slow user code is not directly executing on the receive thread.

### 22.2 Ordering

With:

```text
HandlerWorkerCount = 1
```

publish sequential payloads:

```text
1..10000
```

Require callback-observed order to be exactly:

```text
1..10000
```

No duplicates and no missing values.

### 22.3 Backpressure

Use a very small queue, for example:

```text
HandlerQueueCapacity = 4
HandlerWorkerCount = 1
```

Use a deliberately blocked/slow callback and produce messages faster than they are consumed.

Verify:

- no silent drops;
- no corruption;
- RecvLoop eventually blocks through bounded-channel backpressure;
- processing resumes after worker capacity becomes available;
- connection remains valid unless the test intentionally exceeds server/network limits.

### 22.4 Multi-worker mode

Test:

```text
HandlerWorkerCount = 4
```

Verify:

- callbacks can execute concurrently;
- all messages are delivered exactly once at the client callback layer;
- completion order is not assumed;
- no race in `FInFlightHandlers`.

---

## 23. Phase 7 — Drain semantics

Test graceful drain while messages are:

```text
already received
queued in callback dispatcher
actively executing
still arriving before UNSUB takes effect
```

Required behavior:

1. new publishes/subscriptions are rejected according to drain semantics;
2. UNSUB is sent;
3. server flush barrier is completed when possible;
4. queued/active callbacks complete within timeout;
5. connection closes;
6. `FInFlightHandlers` reaches zero;
7. no accepted callback item disappears.

Also test timeout behavior with a callback intentionally blocked longer than the drain timeout.

Required result:

```text
EDextNatsTimeoutError
connection still ends closed
no AV or use-after-free
```

---

## 24. Phase 8 — Reconnect and outbox tests

### 24.1 Outbox during reconnect

1. Connect client.
2. Force server/socket interruption.
3. Publish while reconnect is in progress.
4. Restore server.
5. Verify buffered messages flush in FIFO order.

### 24.2 Pending buffer limit

Configure a very small:

```text
MaxPendingBufferBytes
```

Verify publishes are rejected once the configured reconnect buffer capacity is exhausted.

### 24.3 Lock-contention regression

Instrument or stress the client while FlushOutbox sends many buffered frames.

Verify state operations are not blocked for the full duration of socket sends because `FLock` is released before `SendRaw`.

Codex should inspect `FlushOutbox` source after any fix and confirm the invariant remains true.

---

## 25. Phase 9 — JetStream tests

Run all current JetStream tests against a fresh JetStream-enabled server.

Cover:

```text
CreateStream
UpdateStream
GetStreamInfo
StreamExists
DeleteStream
PurgeStream
ListStreams
ListStreamNames
CreateConsumer
GetConsumerInfo
DeleteConsumer
ListConsumers
ListConsumerNames
Fetch
Push consumer
Ordered consumer
Ack
Nak
Term
InProgress
PublishAck
message deduplication / MsgId
```

### Critical nested-Fetch test

Mandatory because of the callback dispatcher architecture.

Scenario:

1. Set `HandlerWorkerCount = 1`.
2. Execute a normal application subscription callback on the sole worker.
3. Inside that callback call JetStream `Fetch()` for a pull consumer.
4. Ensure Fetch's private inbox completion path does not depend on the blocked worker.

Required result:

```text
Fetch completes
No self-deadlock
No artificial timeout from callback starvation
Fetched messages are correct
```

Repeat at least 50–100 times.

---

## 26. Phase 10 — KeyValue tests

Test current KeyValue operations including, where implemented:

```text
bucket create/open
Put
Create/CAS semantics
Update
Get
Delete
Purge
history
watcher
filtered watcher
watcher stop during activity
```

Pay special attention to watcher callbacks now running through core client callback dispatch.

Verify no watcher assumes direct RecvLoop execution.

---

## 27. Phase 11 — ObjectStore tests

Test current ObjectStore operations including:

```text
put object
get object
large object/chunk sequence
metadata
watch
reader
delete
crypto paths if enabled
```

Verify large transfers do not introduce callback deadlocks or corrupt ordering.

---

## 28. Phase 12 — Services tests

Test current NATS Services support:

```text
PING discovery
INFO discovery
STATS discovery
endpoint routing
queue selection
validation
request/reply endpoint behavior
```

Ensure service handlers behave correctly with worker-dispatched application callbacks.

---

## 29. Phase 13 — TLS tests

If the repository integration fixtures support TLS, run:

```text
TLS connect
certificate verification
TLS publish/subscribe
TLS request/reply
TLS reconnect
TLS JetStream requests
```

Stress simultaneous read/write activity because TLS engine access is separately serialized.

Verify no deadlock between:

```text
FTlsIoLock
FSendLock
FLock
callback workers
```

---

## 30. Phase 14 — Stress tests

Minimum recommended stress matrix:

### Test A — raw throughput

```text
1 publisher
1 subscriber
1 worker
100k+ small messages
```

Validate count and ordering.

### Test B — slow consumer

```text
1 publisher
1 subscriber
queue capacity = 64
handler delay = 5–10ms
10k+ messages
```

Validate backpressure and zero silent loss.

### Test C — many subscriptions

```text
100–1000 subscriptions
mixed subjects
high message fan-out
```

Validate routing and subscription map thread safety.

### Test D — concurrent publishers

```text
8+ publishing threads
single client
```

Validate frame integrity and send locking.

### Test E — concurrent Request

```text
100+ simultaneous request/reply operations
```

Validate unique inbox behavior, timeout gates and reply isolation.

### Test F — reconnect storm

Repeatedly interrupt and restore NATS while publishers and subscribers remain active.

Validate:

```text
single reconnect owner
subscription replay
outbox replay
no duplicate callback infrastructure
no socket/thread leaks
```

---

## 31. Phase 15 — Performance benchmarks

Run explicit benchmark tests with release optimization.

At minimum capture:

```text
Parser frames/sec
Parser buffer compaction count
PUB encoding throughput
HPUB encoding throughput
callback dispatch throughput
request/reply throughput
```

### Parser acceptance criterion

The parser benchmark that preloads many small frames should not compact the cursor buffer once per frame.

For the existing batch PING benchmark the expected invariant is effectively:

```text
BufferCompactions = 0
```

for the normal preloaded batch scenario.

Do not optimize by weakening parser validation.

---

## 32. Phase 16 — Memory and lifecycle checks

Enable Delphi memory leak reporting or the project's preferred memory manager diagnostics when available.

Test repeated cycles of:

```text
Create client
Connect
Subscribe
Publish/request
Unsubscribe
Disconnect
Free client
```

Minimum 100–1000 cycles.

Look for leaks involving:

```text
TThread
TEvent
TCriticalSection
IChannel<T>
managed records
anonymous methods
TNatsMsg payload arrays
subscription objects
JetStream helper objects
```

Also test freeing a client after callbacks have been queued and after a graceful Drain.

---

## 33. Phase 17 — Race/deadlock review

Codex must manually inspect these lock/thread interactions even if tests are green:

```text
RecvLoop ownership of socket read/reconnect
PingLoop socket-close behavior
FLock usage
FSendLock usage
FTlsIoLock usage
Handler dispatcher worker lifecycle
FInFlightHandlers increment/decrement balance
Disconnect while dispatcher callbacks are executing
Drain while callbacks are queued
Request timeout racing reply
Fetch timeout racing final message
Unsubscribe racing queued callback
subscription auto-remove / MaxMsgs
```

For every potential race found, add a deterministic regression test before fixing the implementation whenever practical.

---

## 34. Phase 18 — Full validation command

After individual failures have been fixed, run the repository's full validation entrypoint:

```powershell
.\scripts\validate-parser-cutover.ps1 -Config Release -Platform Win32 -LiveNats -Benchmark
```

If Win64 is officially supported by every dependency, repeat with Win64.

---

# Part C — Codex Execution Workflow

## 35. Codex should work in this order

Use the following exact workflow:

```text
1. Confirm branch + commit.
2. Read this file completely.
3. Read Docs/ARCHITECTURE_V2.md.
4. Inspect AGENTS.md.
5. Run static architecture checks.
6. Clean compile Debug Win32.
7. Fix compiler errors one by one.
8. Clean compile Release Win32.
9. Run non-live unit tests.
10. Start isolated local NATS + JetStream.
11. Run core live integration tests.
12. Run nested Request test.
13. Run callback ordering/backpressure tests.
14. Run Drain tests.
15. Run reconnect/outbox tests.
16. Run JetStream tests.
17. Run nested Fetch test.
18. Run KV/ObjectStore/Services tests.
19. Run TLS tests if environment supports them.
20. Run stress tests.
21. Run performance benchmarks.
22. Run lifecycle/leak checks.
23. Run final validate-parser-cutover.ps1 command.
24. Update the result section below.
25. Commit implementation fixes separately from test-only changes where practical.
```

---

## 36. Rules for Codex fixes

When a test fails, Codex should follow this decision tree:

### Compiler failure

```text
Is code invalid Delphi 13 syntax?
    -> fix syntax/type/lifetime issue without changing architecture.

Does Dext API differ from assumption?
    -> inspect installed/current Dext source and adapt to native API.

Does a unit cycle exist?
    -> fix dependency direction; do not solve by merging everything back into giant units.
```

### Runtime failure

```text
Protocol mismatch?
    -> verify NATS wire format and parser/writer contract.

Threading failure?
    -> identify owner thread + lock order + queue lifecycle.

Timeout only under nested Request/Fetch?
    -> verify internal completion subscription is not queued behind blocked worker.

Message loss?
    -> never hide it by increasing queue size only; find ownership/backpressure bug.
```

### Performance regression

```text
First prove allocation/copy/lock source with benchmark or instrumentation.
Then optimize.
Do not introduce unsafe borrowed lifetimes without dedicated tests.
```

---

# Part D — Test Result Template

## 37. Codex must fill this section after execution

```markdown
## Codex Validation Result

Date: 2026-08-13
Machine: MACBOOKPRO16-US
Windows: Microsoft Windows NT 10.0.19045.0
Delphi/RAD Studio: Delphi 13 Florence — `C:\Program Files (x86)\Embarcadero\Studio\37.0` (`dcc32`/`dcc64` 37.0, `rsvars.bat` present). Also Studio 22.0 / 23.0 on disk; build script would pick 37.0.
BDS path: `C:\Program Files (x86)\Embarcadero\Studio\37.0`
MSBuild path: `C:\Windows\Microsoft.NET\Framework64\v4.0.30319\MSBuild.exe`
Win32 compiler: `...\Studio\37.0\bin\dcc32.EXE` present
Win64 compiler: `...\Studio\37.0\bin\dcc64.exe` present
NATS Server: `nats-server: v2.12.3` at `C:\apps_delphi\Comp12\nats.server\server\nats-server.exe`
Branch: `main` (cutover branch `perf/parser-v2-cutover` exists locally/remotely; already merged via PR #2 / `7f510e7`. Continuing on `main` is correct.)
Commit tested: `3d935f61025e88c19610b8db7794ec0972ba09fa` (static review only; no compile/test exe run)

### Phase 0 / 1 — executed

Required V2 units exist (Buffer, Parser, Dispatcher, ParserRuntime, Writer, Control, Headers).
Production parser is `TDextNatsRuntimeFrameParser = TDextNatsFrameParserV2`.
`FlushOutbox` releases `FLock` before `SendRaw`.
Legacy identifiers `ShiftBuffer`, `FBufferLen`, `NatsEncodeConnect`/`Pub`/`HPub`/`Sub`/`Unsub`/`Ping`/`Pong` are absent from production source (`NatsEncodePublicKey` in NKeys is unrelated).
`Source/Dext.Net.Nats.Dispatching.pas` is absent. Orphan `Tests/Dext.Net.Nats.Dispatching.Tests.pas` was deleted.
`Docs/ARCHITECTURE_V2.md` was stale vs code (claimed V1 still runtime / opt-in Dispatching adapter); updated in this pass.

### Build
- [x] Debug Win32 clean rebuild — **passed** (`scripts/build-tests.ps1 -Config Debug -Platform Win32` → `Output\Win32\Debug\Dext.Net.Nats.Tests.exe`). Compile fixes: duplicate `SysUtils` in Paging/Transport implementation; `IndexOfCrLf` arity in Internal tests (`8b19853`).
- [ ] Release Win32 clean rebuild — **not run**
- [ ] Release Win64 clean rebuild (if supported) — **not run**

Commands recorded, not executed:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-tests.ps1 -Config Debug -Platform Win32
powershell -ExecutionPolicy Bypass -File .\scripts\build-tests.ps1 -Config Release -Platform Win32
powershell -ExecutionPolicy Bypass -File .\scripts\build-tests.ps1 -Config Release -Platform Win64
```

Compiler errors fixed:
1. Debug Win32: duplicate `System.SysUtils` in `JetStream.Paging` / `JetStream.Transport` implementation uses; `Should(FBuffer.IndexOfCrLf)` needed `IndexOfCrLf(0)` (E2035). Delphi 13 (`Studio\37.0`).

Compiler warnings reviewed:
1. Pre-existing: Dispatcher `Dispatch` hides `TObject.Dispatch`; unused locals; PUBLISHED RTTI on DI settings. Not treated as blockers.

### Unit Tests
- [x] Parser — `TDextNatsParserV2Tests` **8/8 passed** (Debug Win32, `DEXT_NATS_SKIP_LIVE=1`)
- [x] Protocol Writer/Control/Headers — `TDextNatsProtocolV2Tests` **8/8 passed** (includes span header decode + UTF-8 round-trip)
- [x] Internal Buffer — `TDextNatsInternalTests` **8/8 passed** (includes SIMD `IndexOfCrLf` fallback)
- [x] Internal Dispatcher — same Internal fixture **passed**
- [ ] Core client — mega-unit + Drain tests exist; **not executed** (full fixture)
- [ ] Drain — `Tests/Core/Dext.Net.Nats.Drain.Tests.pas` (duplicate also at `Tests/Dext.Net.Nats.Drain.Tests.pas`); **not executed**
- [x] JetStream JSON unit tests — `TDextNatsJetStreamJsonTests` **14/14 passed** (includes `TBytes` parse without string round-trip). Streams/Consumers/Fetch/ObjectPaging fixtures exist; **not executed** this pass
- [ ] KeyValue unit tests — Subjects/WatcherGate only; **not executed**
- [ ] ObjectStore unit tests — Subjects/WatcherGate only; **not executed**
- [ ] Services unit tests — Subjects/Validation/Routing; **not executed**

### Live NATS
- [ ] Connect/Disconnect — **not run**
- [ ] Pub/Sub — **not run**
- [ ] Headers — **not run**
- [ ] Queue groups — **not run**
- [ ] Request/Reply — **not run**
- [ ] Request timeout — **not run**
- [ ] No responders — **not run**
- [ ] Reconnect — **not run**
- [ ] Outbox replay — **not run**

Start command when executing locally: `nats-server -js` (or `scripts/download-nats-server.ps1` then isolated data dir). Do not point tests at production.

### Critical Concurrency
- [ ] Nested synchronous Request from one-worker callback — **source + test added** (`TDextNatsIntegrationTests.NestedRequest_FromOneWorkerHandler_ShouldComplete`, 100×, second-client responder, `HandlerWorkerCount = 1`); **not executed**
- [x] Nested JetStream Fetch from one-worker callback — **P0 source fix + live passed** on nats-server **v2.14.5** `-js` (`NestedFetch_FromOneWorkerHandler_ShouldComplete`, 50×, 214ms)
- [ ] Callback FIFO with one worker — **not run**
- [ ] Bounded queue backpressure — **not run**
- [ ] Multi-worker callback stress — **not run**
- [ ] Drain queued callbacks — tests exist; **not executed**
- [ ] Disconnect during active callbacks — **not run**

### JetStream
- [ ] Stream CRUD/list/purge — **not run**
- [ ] Consumer CRUD/list — **not run**
- [x] Fetch — **P0 source fix** + **nested-Fetch live passed** on nats-server v2.14.5 (`TDextNatsJetStreamFetcher` inline SUB + Unsubscribe(0) before releasing wait-state). Broader Fetch live matrix still open
- [ ] Push — **not run**
- [x] Ordered consumer — `OrderedConsumer_ShouldDeliverInOrder` **passed** on nats-server v2.14.5 (231ms)
- [ ] Ack/Nak/Term/WPI — **not run**
- [ ] Publish Ack / dedup — **not run**

### Additional Modules
- [ ] KeyValue live tests — **partial**: `PurgeDeletes_ShouldRemoveMarkersWhenForced` **passed** on nats-server v2.14.5 (422ms); rest of KV live matrix **not run** (KV Get/Watch can nest Fetch)
- [ ] ObjectStore live tests — **not run** (chunk Fetch can nest)
- [ ] Services live tests — **not run**
- [ ] TLS tests — **not run**

### Stress / Performance
- [ ] 100k+ message stress — **not run**
- [ ] concurrent publishers — **not run**
- [ ] concurrent requests — **not run**
- [ ] reconnect storm — **not run**
- [ ] parser benchmark — **not run** (`DEXT_NATS_RUN_BENCH=1`)
- [ ] dispatcher throughput — **not run**
- [ ] memory/lifecycle checks — **not run**

### Final Gate
Command:
`.\scripts\validate-parser-cutover.ps1 -Config Release -Platform Win32 -LiveNats -Benchmark`

Result:
FAIL (full `validate-parser-cutover.ps1 -LiveNats -Benchmark` not run; Release config not built)

Remaining defects:
1. **P0 deadlock:** **fixed in source and live-proven.** Nested Fetch 50× passed on nats-server v2.14.5 (`TDextNatsJetStreamFetcher` / `SubscribeInline`).
2. **P0 UAF:** **fixed in source.** Same Fetch body (`INatsFetchGate`, `Unsubscribe(sid, 0)` after `Gate.Stop`). Nested-Fetch live pass is the runtime evidence; dedicated UAF stress still not run.
3. **P1:** `TDextNatsJetStreamRuntime` / extracted Streams/Consumers/Push/Ordered are still unused by the façade. **Fetch** delegates to an owned `TDextNatsJetStreamFetcher`. Remaining admin/push/ordered methods still duplicate the extracted units (empty-name checks / parallel ordered engine — not wired to avoid behavior change).
4. Hygiene: orphan Dispatching test unit deleted; duplicate Drain/Internal units at `Tests/` root may remain.
5. Nested Request (100×) **not executed**. Nested Fetch (50×) **passed** on 2.14.5.
6. Release Win32/Win64, remaining live matrix, stress, and `DEXT_NATS_RUN_BENCH=1` still required.

Performance numbers:
- Parser frames/sec: not measured
- Parser compactions: not measured (code invariant: V2 cursor; batch PING benchmark expects 0 compactions — unproven this session)
- Publish throughput: not measured
- Callback throughput: not measured
- Request/reply throughput: not measured

Final recommendation:
NOT READY TO MERGE
```

---

# Part E — Merge Criteria

## 38. PR must remain unmerged until all critical gates pass

Minimum merge requirements:

- Delphi 13 Release Win32 clean compile succeeds;
- non-live tests pass;
- live core NATS tests pass;
- JetStream core tests pass;
- nested synchronous Request test passes;
- nested JetStream Fetch test passes;
- callback ordering/backpressure tests pass;
- Drain does not lose accepted work;
- reconnect/outbox tests pass;
- no known deadlock or use-after-free remains;
- parser benchmark confirms no per-frame compaction regression;
- hosted GitHub CI remains green.

Recommended additional gates before first production release:

- Win64 clean compile;
- TLS integration suite;
- long-running reconnect stress;
- memory/leak run;
- throughput baseline archived in documentation.

---

## 39. Current architectural direction after validation

P0 Fetch deadlock (façade now uses inline completion via `TDextNatsJetStreamFetcher`) and Fetch UAF (stop + `Unsubscribe(sid, 0)` before releasing handler-captured wait-state) are fixed in source; nested Request/Fetch regression tests are added. Compiler/runtime validation is still required.

Once this branch is compiler/runtime validated, continue in this order:

```text
1. Extract JetStream Types/Models from giant facade.
2. Remove circular dependencies from Streams/Consumers/Fetch/Push/Ordered.
3. Make remaining `TDextNatsJetStreamContext` methods delegate to focused Runtime/services (Fetch already delegates to `TDextNatsJetStreamFetcher`).
4. Delete duplicate JetStream implementations from giant facade.
5. Apply same facade/service decomposition to KeyValue.
6. Apply same decomposition to ObjectStore.
7. Apply same decomposition to Services.
8. Re-run the complete test plan after each major module cut-over.
```

The objective is not merely smaller files. The objective is a dependency graph where public API, models, protocol/runtime mechanics, and feature services have clear ownership and can be tested independently.
