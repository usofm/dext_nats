# برنامهٔ آینده Dext.Nats — پس از نقشهٔ راه یکپارچه‌سازی

> **وضعیت:** زنده / به‌روز (2026-08-11)  
> **دامنه:** فقط `Dext.Net.Nats*` — MQTT خارج از محدوده است  
> **مرجع نقشهٔ راه تکمیل‌شده:** [`NATS_DEXT_ROADMAP.md`](NATS_DEXT_ROADMAP.md) (P0–P3 / PERF / DI / AUTH / PUSH / KV)  
> **مرجع تست:** [`TEST_PLAN.md`](TEST_PLAN.md)  
> **مرجع معماری:** [`../AGENTS.md`](../AGENTS.md) · [`../README.md`](../README.md)

این سند **جلونگر** است: آنچه امروز در `main` (committed) آمادهٔ استفاده است، و آنچه برای ریلیز 1.0 و بعد از آن اولویت دارد. تیک‌ها فقط برای کار **مرتب‌شده در git** زده می‌شوند — تغییرات working-tree هنوز `[ ]` می‌مانند.

---

## ۱. هدف / وضعیت امروز

`Dext.Nats` برای **دامنهٔ اصلی** production-ready است:

| لایه | وضعیت committed |
|------|-----------------|
| Core NATS | Connect / pub-sub / request-reply / headers / queue / reconnect / Drain / TLS / NKey-JWT |
| JetStream | Stream/consumer admin + `ListStreams`/`ListConsumers` + Fetch + push `SubscribePush` + Ack\* |
| Key-Value | CreateBucket / Put/Get/Delete/Purge / Keys / History / Watch\* / CAS / per-key TTL |
| Object Store | Create/Update/Delete store, Put/Get/List/Keys, Watch\*, UpdateMeta, Seal, links, streaming Put/Get |
| DX | DI + HealthChecks + Logger/Metrics، README، demos کنسول + VCL، `Dext.Nats.groupproj` |

قراردادهای threading (`RecvLoop` / `PingLoop` / claim-gate) از [`AGENTS.md`](../AGENTS.md) همچنان تغییرناپذیرند.

---

## ۲. فازهای انجام‌شده (committed)

ترتیب تقریبی تکمیل؛ همه با `- [x]`.

### ۲.۱ نقشهٔ راه Dext (`NATS_DEXT_ROADMAP`)

- [x] **P0 PERF** — `TNatsByteWriter`، parse کنترل‌لاین روی بایت، INFO/CONNECT + JetStream admin JSON با `Dext.Json.Utf8`؛ حذف `System.JSON` از Protocol/JetStream
- [x] **P1 DI + LOG** — `AddNatsClient` / `AddNatsClientAndConnect` / `AddNatsJetStream` / `BindNatsOptions`؛ `ILogger` اختیاری
- [x] **P2 MET + HLTH + ASYNC** — `EnableMetrics` / `TNatsClientMetrics`؛ `TNatsHealthCheck`؛ `RequestAsync`/`FlushAsync` با `TAsyncBuilder`
- [x] **P3 AUTH + PUSH** — NKey/JWT + `.creds`؛ JetStream `SubscribePush`
- [x] **SPEC-KV-01** — `Dext.Net.Nats.KeyValue.pas` (شامل Watch EndOfInitial، MetaOnly/UpdatesOnly، CAS، per-key TTL)
- [x] **SPEC-DOC-01** — `README.md` با مثال‌های Connect / DI / TLS / KV / OS

### ۲.۲ محصول فراتر از roadmap اولیه

- [x] **Drain** — `Drain` / `DrainAsync` / `IsDraining` (UNSUB → flush → disconnect)
- [x] **JetStream List\*** — `ListStreams` / `ListStreamNames` / `ListConsumers` / `ListConsumerNames` (+ UI در VCLDemo)
- [x] **Object Store (ADR-20)** — CreateStore / OpenStore / UpdateStore / DeleteStore، Put/Get/Delete، List/Keys، Watch/WatchAll، UpdateMeta، Seal، AddLink/AddBucketLink، streaming `TStream` + PutFile/GetFile
- [x] **DI teardown** — آزادسازی `DefaultProvider` بعد از تست‌های DI (exit 216)
- [x] **Demos** — PubSub / RequestReply / QueueGroup / Headers / Tls / NKey / JetStreamSmoke / KeyValueE2E / ObjectStoreE2E / VCLDemo
- [x] **Tests suite** — Unit + Integration + JetStream + KV + OS + DI + Observability (`Dext.Testing`؛ live soft-skip)
- [x] **Project group** — `Dext.Nats.groupproj`

---

## ۳. فازهای آینده (اولویت‌بندی‌شده)

### P0 — پایداری قبل از برچسب 1.0

- [x] **Shutdown / WSAEINTR 10004** — landed: dext_nats `afa22e5` (FClosing skip + clean RecvLoop join; `Disconnect_ShouldJoinThreadsCleanly`) + Dext Tcp `7061f861`.
- [x] **Stream compression + placement** — landed: `ca3d4f5` (`TNatsStoreCompression` / `TNatsPlacement` on `TNatsStreamConfig` + Object Store map; soft-skip live S2 coverage).
- [x] **یادداشت معماری MQTT vs NATS** — [`MQTT_VS_NATS.md`](MQTT_VS_NATS.md)؛ MQTT همچنان غیرهدف پیاده‌سازی است.
- [ ] **B2B Agent / TNatsManager** — پیش‌نویس در [`NATS_B2B_AGENT_PLAN.md`](NATS_B2B_AGENT_PLAN.md) (مسیر legacy Agent + اپ جدید + امنیت RouteId)؛ پیاده‌سازی تا قطعی شدن Outbox/DB معوق.

### P1 — هم‌ترازی با nats.go (شکاف‌های اعلام‌شده)

- [x] **Object Store show-deleted** — `TNatsObjectStoreGetOptions.ShowDeleted` روی Get/GetInfo/GetFile؛ `List`/`ListObjects` با `AIncludeDeleted` یا `TNatsObjectStoreListOptions.ShowDeleted`؛ Put بدون گزینهٔ عمومی (مثل nats.go، tombstone را داخلی می‌بیند)
- [x] **Lazy ObjectResult reader** — `TDextNatsObjectResult` / `GetResult`: Fetch تنبل روی `Read`، verify digest در EOF (nats.go `ObjectResult`)؛ `Get(TStream)` از روی آن drain می‌کند
- [x] **Account / server INFO غنی‌تر** — `TNatsServerInfo` + `TDextNatsClient.ServerInfo` (handshake و async INFO): `git_commit` / `ip` / `tls_verify` / `api_lvl` / `cluster` / `cluster_dynamic` / `domain` / `remote_account` / `acc_is_sys` / `ldm` / `ws_connect_urls` علاوه بر فیلدهای قبلی (`max_payload`, `client_id`, …). **Gaps:** محدودیت‌های حساب JWT/claims (max_connections و غیره) روی INFO نیستند؛ بدون `OnLameDuckMode` جدا (فقط snapshot/`ldm`)؛ بدون `$SYS` account monitoring
- [x] **Ordered consumer** — `SubscribeOrdered` / `TDextNatsOrderedConsumer` (ADR-17 push: ephemeral, ack_none, flow_control + idle HB, mem_storage, recreate on gap / missed HB). **Gaps vs nats.go:** classic push API only (not modern `jetstream.OrderedConsumer` pull); single `FilterSubject` (no multi-filter); no `OptStartTime`; no Messages/Fetch iterator surface — callback delivery only.
- [x] **NATS Services API** — در `Dext.Net.Nats.Services.pas`: `AddService` / `AddEndpoint` / `AddGroup` (`TDextNatsServiceGroup`، prefix تو در تو + queue inherit/override) / `Stop` / `Reset`، پاسخ خودکار `$SRV.PING|INFO|STATS` (all/kind/instance)، queue پیش‌فرض `q`، `Respond` / `RespondError`. **Gaps vs nats.go micro:** `StatsHandler` / schema JSON عمیق، `DoneHandler` / `ErrorHandler`، Drain روی Stop (اینجا Unsubscribe)، auto-stop روی connection closed، metadata immutability سخت‌گیرانه

### P2 — کیفیت ریلیز و عملیات

- [x] **Versioning** — `CHANGELOG.md` + semver **1.0.0** + git tag `v1.0.0` (local; push tag when publishing).
- [x] **CI** — `.github/workflows/ci.yml` structure checks on hosted runners; full Delphi suite remains **local / self-hosted** (reproduce commands in README). Live/`DEXT_NATS_REQUIRE_LIVE` not in hosted CI.
- [x] **پوشش تست باز از TEST_PLAN** — صف/headers/reconnect/outbox، JS Nak/Term/InProgress، stress Explicit، و gapهای عملی §۲.۳ در suite بسته شده‌اند (live همچنان soft-skip بدون سرور؛ Stress/`Explicit` با `DEXT_NATS_RUN_STRESS=1`؛ TLS/NKey env-gated). ConnectUrls failover چندسروره هنوز بدون تست یکپارچه است.
- [ ] **Push / publish ریلیز** — پکیج یا subtree برای مصرف‌کننده‌های Dext؛ `git push` + `git push --tags` وقتی آمادهٔ انتشار؛ بدون force-push به `main`.
  **Local note (2026-08-11):** `main` is ahead of `origin/main` with post-1.0 commits; tag `v1.0.0` exists **locally** (at release commit) and must not be force-moved. Agents must **not** push until the user explicitly says push / push tags.

### P3 — بعد از 1.0 (اختیاری)

- [x] Payload view با `TByteSpan` عمر مشخص (PERF-04 اختیاری از roadmap) — `TNatsMsg.PayloadSpan` / `TNatsJsMsg.PayloadSpan`؛ `Payload: TBytes` پایدار می‌ماند
- [x] Health check عمیق‌تر با `Flush` کوتاه (HLTH P2b) — `TNatsHealthCheckOptions.FlushTimeoutMs` / `CreateWithFlush`; default Connected-only
- [ ] **KV/OS parity (remaining vs nats.go)** — landed: KV `Compression`/`Placement`, `GetRevision`/`TryGetRevision`, **`PurgeDeletes`**, **`WatchFiltered`** / wildcard `Watch` (`*` / `>`), **`ListKeysFiltered`** / multi-filter `Keys` (last_per_subject pull + `ValidateSearchKey`), bucket **`Config()`** / `Status.Config` (STREAM.INFO read-back), JetStream stream **`Mirror` / `Sources` / `RePublish` / `MirrorDirect`** (ToJson/Parse + KV create pass-through; no auto KV_ name rewrite / source subject-transform injection), OS Watch **`IgnoreDeletes` / `IncludeHistory`**, Services **`AddGroup`**. Still deferred: remaining micro Services gaps (StatsHandler / Done/Error / Drain / auto-stop — listed under P1 Services); KV mirror/source helper transforms beyond pass-through.
- [x] Benchmark رسمی throughput کنار `Encode_MicroBenchmark_*` — `TDextNatsBenchmarkTests` (`Encode_Throughput_*` / `PubSub_Throughput_*`); Explicit + `DEXT_NATS_RUN_BENCH=1`; live soft-skip بدون سرور؛ گزارش ops/sec و msgs/sec (نه CI gate)

---

## ۴. تعریف Done برای ریلیز 1.0

ریلیز **1.0** وقتی Done است که:

1. همهٔ آیتم‌های **§۲** همچنان سبز روی suite پیش‌فرض بمانند (بدون رگرسیون).
2. **P0 §۳** committed باشد (shutdown 10004، compression/placement، یادداشت [`MQTT_VS_NATS.md`](MQTT_VS_NATS.md)). B2B Agent فقط سند است و برای برچسب 1.0 کتابخانه اجباری نیست.
3. [`TEST_PLAN.md`](TEST_PLAN.md): مسیرهای Integration اصلی (Disconnect تمیز، pub/sub، request، JS Fetch+Ack، KV Put/Get، OS Put/Get) بدون fail روی `nats-server -js` محلی.
4. README و `AGENTS.md` با API واقعی هم‌خوان باشند (بدون وعدهٔ قابلیت uncommitted).
5. برچسب git `v1.0.0` + [`CHANGELOG.md`](../CHANGELOG.md) («چه پشتیبانی می‌شود / چه پشتیبانی نمی‌شود»).
6. CI: workflow ساختار در `.github/workflows/ci.yml` + دستور بازتولید Delphi در README (build کامل روی self-hosted هنوز اختیاری است).

آیتم‌های P1 (show-deleted، lazy ObjectResult، Services، ordered) **الزامی برای 1.0 نیستند** مگر صریحاً به این لیست اضافه شوند.

---

## ۵. غیرهدف‌ها (Non-goals)

| مورد | دلیل |
|------|------|
| بهبود / port به `Dext.Net.Mqtt` | خارج از اولویت محصول؛ فقط مقایسهٔ معماری مجاز است |
| نوشتن NATS server داخل Delphi | سرور رسمی `nats-server` مرجع است |
| تعویض `TDextTcpClient` با stack دیگر | Tcp فعلی بخشی از هویت Dext است |
| شکستن API عمومی پایدار بدون deprecate | overload / unit جدید؛ حذف فقط با اعلام صریح |
| کامل بودن ۱۰۰٪ nats.go قبل از 1.0 | parity انتخابی (P1)؛ MVP فعلی کافی است |
| WebSocket / leafnode client اختصاصی | خارج از دامنهٔ فعلی |

---

## ۶. پیوندها و نگهداری

| سند | نقش |
|-----|-----|
| [`NATS_DEXT_ROADMAP.md`](NATS_DEXT_ROADMAP.md) | نقشهٔ یکپارچه‌سازی Dext — **تکمیل‌شده**؛ تاریخچهٔ SPEC |
| [`TEST_PLAN.md`](TEST_PLAN.md) | ماتریس تست و gapها |
| [`../AGENTS.md`](../AGENTS.md) | قرارداد معماری + موجودی واحدها |
| این فایل | اولویت‌های پس از roadmap تا 1.0 و بعد |

هنگام commit هر آیتم باز: همان خط را `[x]` کن و در صورت نیاز یک خط در `AGENTS.md` Current status به‌روز شود.
