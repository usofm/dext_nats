# AGENTS.md — Guide for AI Coding Agents

This file orients AI agents (and humans) working on **Dext.Nats**, a native Delphi
client for the [NATS](https://nats.io) messaging system, built on the **Dext
Framework**. Read this before making changes.

## What this project is

A dependency-light NATS client library, structured the way Dext's own networking
units are structured (see "Reference material" below). It is not a standalone
socket library — it deliberately reuses `Dext.Net.Tcp` for the transport and
`Dext.Core.Span` / `Dext.Collections` for buffers and collections, so it looks
and feels like a first-party Dext component.

## Repository layout

```
Source/
  Dext.Net.Nats.Protocol.pas   Pure wire-protocol layer: constants, INFO/CONNECT
                                JSON, headers, TNatsFrame, TDextNatsFrameParser
                                (incremental parser), frame encoders. NO socket I/O.
  Dext.Net.Nats.pas             TDextNatsClient: the public API. Socket I/O,
                                threading, reconnection, pub/sub, request/reply.
  Dext.Net.Nats.JetStream.pas   TDextNatsJetStreamContext: stream admin
                                (create/update/info/delete), dedup'd publish,
                                pull-consumer admin, Fetch, Ack/Nak/Term/
                                InProgress via $JS.API.*.
Demo/
  JetStreamSmokeTest/           Interactive console program that manually verifies
                                stream/consumer paths and dedup'd publish
                                against a local `nats-server -js`. Not a
                                Dext.Testing suite — see its own header comment.
Tests/                          Dext.Testing-based unit + integration tests.
LICENSE                         Apache 2.0.
```

Keep this separation: protocol/parsing code must stay free of sockets and
threading so it can be unit-tested in isolation; all I/O and concurrency lives
in `Dext.Net.Nats.pas`.

`Dext.Net.Nats.JetStream.pas` depends on `Dext.Net.Nats` — never the reverse.
`TDextNatsJetStreamContext` wraps an already-connected `TDextNatsClient` by
composition (it does not inherit from or modify the client, and does not own
its lifetime) and reuses the `NatsJsonGetStr/Int/Int64/Bool`/`NatsBoolStr`/
`NatsJsonEscape` helpers already exported from `Dext.Net.Nats.Protocol`'s
interface section instead of re-implementing JSON parsing.

## Reference material — read before extending

There is no Delphi compiler available in this environment (no `dcc32`/`dcc64`
on PATH), so **there is no build/test loop**. Changes are reviewed by careful
manual reading against known-good Dext code. Always cross-check new code
against these existing, compiling sources rather than guessing at APIs:

- `C:\dev\comp\dext\Sources\Net\Dext.Net.Tcp.pas` — the `TDextTcpClient` this
  library wraps. Check exact method signatures here before calling them
  (`Connect`, `Disconnect`, `Send`, `Receive`, `Connected`).
- `C:\dev\comp\dext\Sources\Net\Dext.Net.Mqtt.pas` and
  `Dext.Net.Mqtt.Parser.pas` — closest architectural sibling: a text/binary
  protocol client with a receive thread, a ping thread, and a stateful
  incremental parser. Mirror its patterns (thread creation, buffering,
  reconnection) rather than inventing new ones.
- `C:\dev\comp\dext\Sources\Net\Dext.Net.RestClient.pas`,
  `Dext.Net.ConnectionPool.pas`, `Dext.Net.Engine.pas` — general Dext.Net
  coding style (doc comments, error types, options-record pattern).
- `C:\dev\comp\dext\Sources\Core\Base\Dext.Core.Span.pas` — `TByteSpan` API
  used for zero-copy buffer append in the parser.
- `C:\dev\comp\dext\Sources\Core\Dext.Collections*.pas` — the collections
  actually used (see below); the factory is `Dext.Collections.Factory` /
  `TCollections`, interfaces live in `Dext.Collections.pas` and
  `Dext.Collections.Dict.pas`.
- `C:\dev\comp\indy_nats\Source\` — a separate Indy-based NATS client, useful
  purely as a NATS-protocol reference (framing, CONNECT options, inbox
  generation), not as a Delphi-style reference.

When in doubt about a Dext API's exact signature, **grep the real source file
above** instead of assuming RTL-equivalent behavior.

## Coding conventions used in this repo

- License header block (see top of existing `.pas` files) on every new unit.
- `/// <summary>...</summary>` XML doc comments on public types/members; no
  redundant inline comments that just restate the code.
- Naming: `TDextNats*` for classes, `EDextNats*` for exceptions (all inherit
  from `EDextNatsException`), `TNats*` for plain protocol data records.
- Options/config are plain records with a `class function CreateDefault: T;
  static;` factory (see `TDextNatsOptions`, `TNatsConnectOptions`), not classes.
- **Collections: use `Dext.Collections` only — never `System.Generics.Collections`.**
  Prefer interface types (`IDictionary<K,V>`, `IQueue<T>`, `IList<T>`)
  constructed via `TCollections.CreateDictionary<K,V>(...)` /
  `.CreateQueue<T>` / `.CreateList<T>` so cleanup is automatic (no manual
  `.Free`). For `TPair<K,V>` (including `TNatsHeader`), `uses
  Dext.Collections.Dict` (where `TPair` is declared) alongside
  `Dext.Collections`; do not mix in the RTL `TPair`.
- Byte buffers: use `TBytes`/`TByteSpan`, not `TMemoryStream`, on hot paths.

## Threading model — read carefully before touching `Dext.Net.Nats.pas`

`TDextNatsClient` runs two background threads once connected:

- **`RecvLoop`** — the *only* thread allowed to call `FTcpClient.Receive` or
  drive a reconnect (`HandleConnectionLost` → `TryReconnect`). It owns the
  parser (`FParser`) exclusively.
- **`PingLoop`** — sends keepalive PINGs. If it detects a stale connection
  (too many un-answered pings) it must **only close the socket**
  (`FTcpClient.Disconnect`) and let `RecvLoop` notice the failure and
  reconnect itself. `TDextTcpClient` is not safe to `Connect`/`Receive`
  concurrently from two threads — never call `TryReconnect`/`HandleConnectionLost`
  from `PingLoop`.

Other rules:

- All shared mutable state (`FConnected`, `FSubscriptions`, `FPongWaiters`,
  `FPendingOutbox`, etc.) is guarded by `FLock`. All actual socket writes go
  through `SendRaw`, which serializes on `FSendLock` (separate from `FLock`
  so a slow send doesn't block state reads).
- Request/reply (`Request`, `RequestAsync`) must use a one-shot "claim gate"
  (see `INatsRequestGate`/`TNatsRequestGate`) so that a reply arriving at the
  exact moment a timeout fires can never race with freeing the waiting
  `TEvent`. Don't free a `TEvent` that a concurrent handler might still call
  `SetEvent` on — claim ownership first, then free.
- `TThread.CreateAnonymousThread(SomeInstanceMethod)` (passing a
  `procedure of object` where a `TProc` is expected) is an established Dext
  pattern (see `Dext.Net.Mqtt.pas`) and works fine — no need to wrap it in an
  extra anonymous closure.

## NATS protocol notes

Text protocol, CRLF-terminated control lines, binary-safe payloads announced
by explicit byte counts: `INFO`, `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`,
`PING`, `PONG`, `MSG`, `HMSG`, `+OK`, `-ERR`. `TDextNatsFrameParser` in
`Dext.Net.Nats.Protocol.pas` is the single source of truth for framing rules
(including the `NATS_MAX_FRAME_BYTES` safety ceiling) — extend it rather than
parsing frames anywhere else.

## Current status / pending work

- [x] Protocol layer (`Dext.Net.Nats.Protocol.pas`)
- [x] Client (`Dext.Net.Nats.pas`): connect, pub/sub, request/reply, reconnect,
      keepalive — now using `Dext.Collections` throughout
- [x] JetStream (`Dext.Net.Nats.JetStream.pas`): stream admin
 (create/update/info/delete), dedup'd publish with a `Nats-Msg-Id`
 header, pull-consumer admin, Fetch, and Ack/Nak/Term/InProgress;
 push consumers are not implemented
- [x] Unit/integration tests in `Tests/Dext.Net.Nats.Tests.pas` (use `Dext.Testing`)
- [x] Console demo project (`.dpr`/`.dproj`): `Demo/JetStreamSmokeTest/` is the
      first one, covering the new JetStream layer; more demos may follow
- [x] TLS on `TDextNatsClient` via `TDextTLSOptions` / `IDextTLSEngine`
      (upgrade after cleartext INFO when `tls_required` or `Options.TLS.Enabled`)
- [x] `README.md` with usage examples
- [ ] NKey/JWT auth, a DI extension, observability (health checks/metrics/
      structured logging), and push consumers are deferred to a later pass

## Working style expected of an agent here

- Since there's no compiler in this environment, be conservative: re-read
  the exact method you're calling in its source unit before using it, and
  re-read your own diff afterwards looking for the kind of concurrency bugs
  described above (they don't show up as compile errors).
- Prefer extending the existing unit structure (protocol / client / JetStream)
  over adding new units unless a new concern (e.g. tests, TLS) genuinely needs
  its own file.
