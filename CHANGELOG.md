# Changelog — Dext.Nats

All notable releases of this library are documented here.  
Format inspired by [Keep a Changelog](https://keepachangelog.com/); versioning follows [SemVer](https://semver.org/).

## [Unreleased]

### Added

- **NATS Services StatsHandler:** `TNatsServiceConfig.StatsHandler` / `TNatsServiceStatsHandler` / `TNatsServiceEndpointInfo` — optional per-endpoint STATS `data` JSON enrichment (nats.go micro StatsHandler). Empty return omits `data`; handler exceptions fire `ErrorHandler` and omit `data`. Connection-closed auto-stop still deferred (`OnDisconnected` ≠ permanent ClosedHandler). Unit test.
- **NATS Services DoneHandler / ErrorHandler + drain-on-Stop:** `TNatsServiceConfig.DoneHandler` / `ErrorHandler` (`TNatsServiceDoneHandler`, `TNatsServiceErrorHandler`, `TNatsServiceErrorInfo`) — Done fires once after Stop; Error on `RespondError`, handler exceptions, and discovery publish failures. `Stop` UNSUBs service SIDs then best-effort `Flush` when connected (does not call `client.Drain` / disconnect). Unit + live soft-skip tests.
- **NATS Services AddGroup:** `TDextNatsService.AddGroup` / `TDextNatsServiceGroup` (nats.go micro subject-prefix groups) — nested `AddGroup`, group `AddEndpoint` joins `Prefix.endpoint` (or `Prefix.Subject`), queue inherit/override via `TNatsGroupConfig.QueueGroup` / `QueueGroupDisabled`. Groups owned by the service. Unit + live soft-skip tests.
- **JetStream stream Mirror / Sources / RePublish:** `TNatsStreamConfig` gains `Mirror` / `Sources` / `RePublish` / `MirrorDirect` (`TNatsStreamSource`, `TNatsRePublish`, `TNatsExternalStream`, `TNatsSubjectTransform`) with Dext.Json.Utf8 ToJson/Parse round-trip for STREAM.CREATE/UPDATE/INFO. KV bucket create/Config pass these through (mirror clears subjects + sets `mirror_direct`; no auto `KV_` rename / source transform injection). Unit serialize/parse + live soft-skip RePublish/Mirror create tests.
- **KV ListKeysFiltered / multi-filter Keys:** `ListKeysFiltered` and `Keys(filters)` list live PUT keys matching search patterns (`*` / trailing `>`; multi-filter via consumer `filter_subjects`); empty filters = all keys. Reuses `ValidateSearchKey` from `WatchFiltered` and the existing last_per_subject pull path.
- **KV WatchFiltered + bucket Config():** `WatchFiltered` (key/subject wildcards `*` / trailing `>`; multi-filter via consumer `filter_subjects`); `Watch` accepts the same search-key rules. `TNatsKeyValueConfig.FromStreamConfig` + `TDextNatsKeyValue.Config` / `TNatsKeyValueStatus.Config` read back bucket settings from STREAM.INFO (incl. Mirror/Sources/RePublish). Consumer config gains `FilterSubjects`.
- **KV PurgeDeletes + JetStream STREAM.PURGE:** `TDextNatsJetStreamContext.PurgeStream` / `TNatsStreamPurgeRequest` (`subject` / `seq` / `keep`). KV `PurgeDeletes` / `TNatsKeyValuePurgeDeletesOptions.DeleteMarkersOlderThan` (0 → 30m default, negative → remove all markers; nats.go parity).
- **Object Store Watch IgnoreDeletes / IncludeHistory:** `TNatsObjectStoreWatchOptions` gains `IgnoreDeletes` (skip `Deleted` meta, still counts toward EndOfInitial) and `IncludeHistory` (`deliver_policy=all`; conflicts with `UpdatesOnly`).
- **KV GetRevision + bucket Compression/Placement:** `GetRevision` / `TryGetRevision` (STREAM.MSG.GET; includes DEL/PURGE; wrong-subject → not found, nats.go parity). `TNatsKeyValueConfig.Compression` / `Placement` map onto KV_* `STREAM.CREATE` (`scS2` ≡ nats.go `Compression: true`).
- **Opt-in throughput benchmarks:** `TDextNatsBenchmarkTests` — formal encode (`Encode_Throughput_ShouldReportOpsPerSec`) and live pub/sub (`PubSub_Throughput_ShouldReportMsgsPerSec`) harness next to `Encode_MicroBenchmark_*`; gated with Explicit + `DEXT_NATS_RUN_BENCH=1`; soft-skips without `nats-server` on the live path; prints ops/sec / msgs/sec (soft floors only — not a CI perf gate). `Encode_MicroBenchmark_PubAndCachedPing` now also prints ops/sec.
- **Health check Flush probe (HLTH P2b):** `TNatsHealthCheckOptions` (`FlushTimeoutMs`, `CreateDefault` / `CreateWithFlush`) + `AddNatsHealthCheck(..., AOptions)`. Default remains Connected-only; when `FlushTimeoutMs > 0`, healthy = connected and short PING/PONG `Flush` within that budget (timeout/error → Unhealthy, never raises; does not fall back to `RequestTimeoutMs`).
- **Borrowed-span MSG (inline only):** Parser V2 no longer `CopyTo` owned `TBytes` for MSG/HMSG (`TNatsFrame.PayloadSpan` into the read buffer). Default `Subscribe` still copies before the dispatcher queue. Opt-in `SubscribeInline` delivers a borrowed `TNatsMsg.PayloadSpan` on RecvLoop (valid until the handler returns; `Payload` empty). `Request`/`Fetch` complete inline and `CloneOwned`/`CopyPayload` so returned messages keep owned bytes. `TNatsMsgHandler` / `Payload: TBytes` unchanged.
- **Object Store show-deleted:** `TNatsObjectStoreGetOptions.ShowDeleted` for `Get` / `GetInfo` / `GetFile`; `TNatsObjectStoreListOptions.ShowDeleted` alongside existing `List`/`ListObjects(AIncludeDeleted)` (nats.go `*ShowDeleted` parity). Put has no public flag — re-Put after Delete already sees tombstones.
- **Object Store lazy ObjectResult:** `TDextNatsObjectResult` (`TStream`) + `GetResult` — chunk Fetch on `Read`, SHA-256/size verified at EOF (nats.go `ObjectResult`). Eager `Get(TStream)` / `Get(TBytes)` / `GetFile` drain via `GetResult`.
- **JetStream ordered consumer:** `SubscribeOrdered` / `TDextNatsOrderedConsumer` + `TNatsOrderedConsumerOptions` (ADR-17 push: ephemeral, `ack_policy=none`, flow_control + idle heartbeats, mem_storage, recreate on consumer-sequence gap / missed HB). Consumer config gains `FlowControl` / `IdleHeartbeat` / `InactiveThreshold` / `MemoryStorage` / `NumReplicas`.
- **NATS Services API (MVP):** `Source/Dext.Net.Nats.Services.pas` — `TDextNatsService.AddService` / `AddEndpoint` / `Stop` / `Reset`, auto `$SRV.PING|INFO|STATS` (all / kind / instance), `Respond` / `RespondError`, default queue `q`. Composition over `TDextNatsClient` (does not own it).
- **Richer server INFO:** `TNatsServerInfo` parses additional wire fields (`git_commit`, `ip`, `tls_verify`, `api_lvl`, `cluster`, `cluster_dynamic`, `domain`, `remote_account`, `acc_is_sys`, `ldm`, `ws_connect_urls`) and exposes them via existing `TDextNatsClient.ServerInfo` (updated on handshake and async INFO).

## [1.0.0] — 2026-08-11

First production-oriented release of the native NATS client for the Dext Framework (Delphi 12 / Studio 23.0).

### Supported

- **Core NATS:** connect/disconnect, pub/sub, queue groups, headers (HPUB/HMSG), request/reply (incl. no-responders), reconnect + subscription replay + `connect_urls`, keepalive, `Drain` / `DrainAsync` / `IsDraining`
- **TLS:** upgrade after cleartext INFO via `TDextTLSOptions` / `IDextTLSEngine`
- **Auth:** user/password/token, NKey/JWT, `.creds`
- **Async:** `RequestAsync` / `FlushAsync` (`TAsyncBuilder`)
- **DI / ops:** `AddNatsClient` / config bind / `AddNatsJetStream`, optional `ILogger`, opt-in metrics, `TNatsHealthCheck`
- **JetStream:** stream/consumer admin (`ListStreams` / `ListConsumers`, …), Fetch, push `SubscribePush`, ordered `SubscribeOrdered`, Ack/Nak/Term/InProgress, compression (`s2`) + placement on stream config
- **Key-Value:** Put/Get/Delete/Purge, Keys/ListKeysFiltered, History, Watch/WatchFiltered (EndOfInitial, wildcards, MetaOnly/UpdatesOnly, IncludeHistory, IgnoreDeletes, ResumeFromRevision), Config() stream read-back, CAS Create/Update, per-key TTL (server 2.11+)
- **Object Store:** Create/Update/Delete store, Put/Get/Delete, List/Keys, Watch, UpdateMeta, Seal, links, streaming Put/Get + PutFile/GetFile, lazy `GetResult` / `TDextNatsObjectResult`
- **DX:** unit + live tests (`Dext.Testing`), console E2E demos, VCLDemo, `Dext.Nats.groupproj`

### Not supported / deferred (not blocking 1.0)

- Full nats.go micro parity beyond Services (schema depth, connection-closed auto-stop, per-sub Drain) — see `Docs/NATS_FUTURE_PLAN.md`
- Account JWT/claims limit objects and dedicated `OnLameDuckMode` (INFO `ldm` is on `ServerInfo.LameDuckMode`; full account limits are not on INFO wire)
- Modern nats.go pull `jetstream.OrderedConsumer` / multi-filter / `OptStartTime` (push ADR-17 helper is present)
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
