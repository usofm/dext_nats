# Changelog — Dext.Nats

All notable releases of this library are documented here.  
Format inspired by [Keep a Changelog](https://keepachangelog.com/); versioning follows [SemVer](https://semver.org/).

## [1.0.0] — 2026-08-11

First production-oriented release of the native NATS client for the Dext Framework (Delphi 12 / Studio 23.0).

### Supported

- **Core NATS:** connect/disconnect, pub/sub, queue groups, headers (HPUB/HMSG), request/reply (incl. no-responders), reconnect + subscription replay + `connect_urls`, keepalive, `Drain` / `DrainAsync` / `IsDraining`
- **TLS:** upgrade after cleartext INFO via `TDextTLSOptions` / `IDextTLSEngine`
- **Auth:** user/password/token, NKey/JWT, `.creds`
- **Async:** `RequestAsync` / `FlushAsync` (`TAsyncBuilder`)
- **DI / ops:** `AddNatsClient` / config bind / `AddNatsJetStream`, optional `ILogger`, opt-in metrics, `TNatsHealthCheck`
- **JetStream:** stream/consumer admin (`ListStreams` / `ListConsumers`, …), Fetch, push `SubscribePush`, Ack/Nak/Term/InProgress, compression (`s2`) + placement on stream config
- **Key-Value:** Put/Get/Delete/Purge, Keys, History, Watch (EndOfInitial, MetaOnly/UpdatesOnly, IncludeHistory, IgnoreDeletes, ResumeFromRevision), CAS Create/Update, per-key TTL (server 2.11+)
- **Object Store:** Create/Update/Delete store, Put/Get/Delete, List/Keys, Watch, UpdateMeta, Seal, links, streaming Put/Get + PutFile/GetFile
- **DX:** unit + live tests (`Dext.Testing`), console E2E demos, VCLDemo, `Dext.Nats.groupproj`

### Not supported / deferred (not blocking 1.0)

- Object Store show-deleted options; lazy on-demand ObjectResult reader
- NATS Services API (`$SRV.*`); ordered-consumer helper
- Full account INFO surface beyond current `TNatsServerInfo`
- B2B Agent / `TNatsManager` product (design only: `Docs/NATS_B2B_AGENT_PLAN.md`)
- MQTT feature port (architecture notes only: `Docs/MQTT_VS_NATS.md`)
- Hosted CI with Embarcadero compilers (local reproduce documented in README; self-hosted runner TBD)

### Requirements

- Delphi 12 Athens (Studio 23.0)
- Dext Framework sources on the search path (`Dext.Net.Tcp`, Collections, Json.Utf8, …)
- Runtime: `nats-server` for live tests/demos (JetStream: `nats-server -js`)

### Docs

- `README.md` — usage
- `Docs/NATS_DEXT_ROADMAP.md` — completed integration roadmap
- `Docs/NATS_FUTURE_PLAN.md` — post-1.0 priorities
- `Docs/TEST_PLAN.md` — test matrix
- `Docs/MQTT_VS_NATS.md` — MQTT vs NATS architecture
- `AGENTS.md` — threading contracts for contributors

[1.0.0]: CHANGELOG.md#100--2026-08-11
