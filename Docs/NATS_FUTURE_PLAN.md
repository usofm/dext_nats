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
- [ ] **یادداشت معماری MQTT vs NATS** — پیش‌نویس `Dext.Net.Mqtt vs Dext.Net.Nats.md` (untracked) را پالایش و در `Docs/` ثبت کن؛ MQTT همچنان غیرهدف پیاده‌سازی است.
- [ ] **B2B Agent / TNatsManager** — پیش‌نویس در [`NATS_B2B_AGENT_PLAN.md`](NATS_B2B_AGENT_PLAN.md) (مسیر legacy Agent + اپ جدید + امنیت RouteId)؛ پیاده‌سازی تا قطعی شدن Outbox/DB معوق.

### P1 — هم‌ترازی با nats.go (شکاف‌های اعلام‌شده)

- [ ] **Object Store show-deleted** — گزینه‌های Get/Put/List برای آبجکت‌های حذف‌شده (مثل nats.go)
- [ ] **Lazy ObjectResult reader** — خواندن تنبل chunkها به‌جای بارگذاری یک‌جا در Get حجیم
- [ ] **Account / server INFO غنی‌تر** — سطح API برای فیلدهای INFO حساب / محدودیت‌ها در صورت نیاز اپ‌های multi-tenant
- [ ] **Ordered consumer** — helper JetStream برای consumer مرتب (ordered) روی یک subject/stream
- [ ] **NATS Services API** — ثبت/کشف microservice (`$SRV.*`) در حد MVP اگر تقاضا باشد

### P2 — کیفیت ریلیز و عملیات

- [ ] **Versioning** — برچسب semver (`v1.0.0`)، changelog کوتاه، هم‌خوانی با bump Dext در صورت نیاز
- [ ] **CI** — job build Delphi 12 + suite پیش‌فرض؛ اختیاری service container `nats-server -js` با `DEXT_NATS_REQUIRE_LIVE=1`؛ stress فقط env-gated (طبق [`TEST_PLAN.md`](TEST_PLAN.md) §۷.۴)
- [ ] **پوشش تست باز از TEST_PLAN** — صف/headers/reconnect/outbox، JS Nak/Term/InProgress، stress Explicit؛ بستن gapهای §۲.۳ که هنوز soft-skip یا غایب‌اند
- [ ] **Push / publish ریلیز** — پکیج یا subtree برای مصرف‌کننده‌های Dext؛ README نصب؛ بدون force-push به `main`

### P3 — بعد از 1.0 (اختیاری)

- [ ] Payload view با `TByteSpan` عمر مشخص (PERF-04 اختیاری از roadmap)
- [ ] Health check عمیق‌تر با `Flush` کوتاه (HLTH P2b)
- [ ] KV/OS parity بیشتر با nats.go در صورت ADR جدید سرور
- [ ] Benchmark رسمی throughput کنار `Encode_MicroBenchmark_*`

---

## ۴. تعریف Done برای ریلیز 1.0

ریلیز **1.0** وقتی Done است که:

1. همهٔ آیتم‌های **§۲** همچنان سبز روی suite پیش‌فرض بمانند (بدون رگرسیون).
2. **P0 §۳** committed باشد (shutdown 10004: `afa22e5` / Tcp `7061f861`؛ compression/placement: `ca3d4f5`)؛ آیتم MQTT-vs-NATS note همچنان باز است.
3. [`TEST_PLAN.md`](TEST_PLAN.md): مسیرهای Integration اصلی (Disconnect تمیز، pub/sub، request، JS Fetch+Ack، KV Put/Get، OS Put/Get) بدون fail روی `nats-server -js` محلی.
4. README و `AGENTS.md` با API واقعی هم‌خوان باشند (بدون وعدهٔ قابلیت uncommitted).
5. برچسب git `v1.0.0` + یادداشت کوتاه «چه پشتیبانی می‌شود / چه پشتیبانی نمی‌شود».
6. CI حداقلی (build + unit؛ live اختیاری) یا دستور بازتولید مستند در README.

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
