# نقشهٔ راه Dext.Nats — یکپارچه‌سازی و پرفورمنس روی Dext

> **وضعیت:** پیشنهادی / پذیرفته‌شده برای اجرا (2026-08-10)  
> **دامنه:** فقط `Dext.Net.Nats*` — MQTT خارج از محدوده است  
> **مرجع تست:** [`TEST_PLAN.md`](TEST_PLAN.md)  
> **مرجع معماری:** [`../AGENTS.md`](../AGENTS.md)

---

## ۱. هدف

`Dext.Nats` را به یک کلاینت messaging **first-party** برای Dext تبدیل کنیم که:

1. روی **داغ‌مسیر پروتکل** حداقل تخصیص داشته باشد (`TByteSpan`، `Dext.Json.Utf8`).
2. مثل Redis در اپ‌های Dext قابل ثبت باشد (`Dext.DI`).
3. observability سبک و استاندارد داشته باشد (`Dext.Logging`، `Dext.Telemetry.Metrics`، `Dext.HealthChecks`).
4. API async هم‌سبک Redis داشته باشد (`Dext.Threading.Async` / `TAsyncBuilder`).
5. قابلیت‌های ناقص پروتکل/محصول (NKey/JWT، JetStream push) را بدون شکستن قرارداد threading فعلی اضافه کند.

### غیرهدف‌ها (Non-goals)

| مورد | دلیل |
|------|------|
| بهبود / port به `Dext.Net.Mqtt` | خارج از اولویت محصول |
| نوشتن NATS server داخل Delphi | سرور رسمی `nats-server` مرجع است |
| تعویض `TDextTcpClient` با stack دیگر | Tcp فعلی بخشی از هویت Dext است |
| شکستن API عمومی پایدار بدون دلیل | overload / unit جدید؛ حذف فقط با deprecate صریح |

---

## ۲. وضعیت امروز (Baseline)

| لایه | فایل | وابستگی Dext | گلوگاه شناخته‌شده |
|------|------|--------------|-------------------|
| Protocol | `Source/Dext.Net.Nats.Protocol.pas` | `Span`, `Collections`, `Json.Utf8` | PERF-01..05 done (byte writer, control-line span, Utf8 INFO/CONNECT) |
| Client | `Source/Dext.Net.Nats.pas` | `Tcp`, `Security`, `Collections`, `Span`, Logging/Metrics, `Threading.Async` | ASYNC `RequestAsync`/`FlushAsync` → `TAsyncBuilder` done |
| JetStream | `Source/Dext.Net.Nats.JetStream.pas` | Client + `Json.Utf8` | PERF-03b Utf8 admin JSON؛ push `SubscribePush` done |
| Tests | `Tests/Dext.Net.Nats.Tests.*` | `Dext.Testing` | پوشش خوب؛ پلن گسترش در `TEST_PLAN.md` |

**قراردادهای تغییرناپذیر** (از `AGENTS.md`):

- فقط `RecvLoop` حق `Receive` و `TryReconnect` دارد.
- `PingLoop` فقط `Disconnect` می‌کند.
- state روی `FLock`؛ write روی `FSendLock`؛ TLS I/O روی `FTlsIoLock`.
- request/reply با `INatsRequestGate` / claim قبل از free کردن `TEvent`.
- فقط `Dext.Collections` — نه `System.Generics.Collections`.

---

## ۳. فازها و اولویت

| فاز | کد | عنوان | اولویت | واحد(های) اصلی |
|-----|----|--------|--------|----------------|
| 0 | **PERF** | داغ‌مسیر Protocol (Span + Utf8 JSON + encode تک‌بافر) | P0 | `Protocol` (+ تست unit) |
| 1 | **DI** | ثبت DI + Options binding | P1 | unit جدید `Dext.Net.Nats.DependencyInjection.pas` |
| 1 | **LOG** | `ILogger` اختیاری روی Client | P1 | `Nats.pas` |
| 2 | **MET** | Metrics شمارنده‌ها / histogram | P2 | `Nats.pas` |
| 2 | **HLTH** | `IHealthCheck` برای Connected | P2 | DI unit یا `Dext.Net.Nats.HealthChecks.pas` |
| 2 | **ASYNC** | `Request` / `Flush` روی `TAsyncBuilder` | P2 | `Nats.pas` |
| 3 | **AUTH** | NKey / JWT | P3 | `Protocol` + `Nats.pas` |
| 3 | **PUSH** | JetStream push consumer | P3 | `JetStream.pas` |
| DX | **DOC** | `README.md` + مثال DI/Logging | موازی بعد از P1 | ریشهٔ repo |

ترتیب اجرا پیشنهادی: **PERF → DI → LOG → MET/HLTH/ASYNC → AUTH/PUSH → DOC**.

---

## ۴. Specها

هر spec شناسهٔ پایدار دارد (`SPEC-PERF-01` …). Acceptance = شرط Done.

### فاز PERF — پرفورمنس پروتکل

#### SPEC-PERF-01 — Encode تک‌بافر بدون `NatsConcatBytes` روی مسیر داغ

**مشکل:** `NatsEncodePub` / `NatsEncodeHPub` چند `TBytes` می‌سازند و با `Move` ادغام می‌کنند.

**الزامات:**

1. یک helper داخلی (مثلاً `TNatsByteWriter` record در implementation یا interface محدود) با:
   - `Reset` / `EnsureCapacity`
   - `WriteBytes` / `WriteAscii` / `WriteCrLf` / `WriteIntDec`
   - `ToBytes` یا `AsSpan` در پایان
2. `NatsEncodePub`, `NatsEncodeHPub`, `NatsEncodeSub`, `NatsEncodeUnsub`, `NatsEncodeConnect` از writer استفاده کنند.
3. `PING`/`PONG` می‌توانند ثابت از پیش‌ساخته (`const`/`class var` بایت) باشند — بدون تخصیص در هر فراخوانی.
4. امضای عمومی encodeها (`function …: TBytes`) حفظ شود مگر overloadهای Span اضافه شوند.

**Acceptance:**

- [x] تست‌های U-06 و encode موجود سبز بمانند.
- [x] مسیر `PUB` روی hot path حداکثر **یک** `SetLength` نهایی برای خروجی داشته باشد (نه N کپی میانی).
- [x] بنچمارک ساده (اختیاری در تست/دمو): ≥ همان throughput قبلی؛ هدف: کاهش تخصیص‌های میانی به صفر.
  (`Encode_MicroBenchmark_PubAndCachedPing` — PING/PONG reference-stable؛ 40k× PUB زیر سقف زمانی)

#### SPEC-PERF-02 — Parse کنترل‌لاین روی `TByteSpan` بدون `GetString` اجباری

**مشکل:** هر فریم کنترل‌لاین را با `TEncoding.UTF8.GetString` به `string` تبدیل می‌کند.

**الزامات:**

1. `ParseControlLine` (یا معادل) روی بایت کار کند: تشخیص opcode با مقایسهٔ بایت (`INFO`/`MSG`/`HMSG`/`PING`/`PONG`/`+OK`/`-ERR`).
2. Subject / reply-to / sid / اندازهٔ payload از span استخراج شوند؛ `string` فقط وقتی برای API عمومی لازم است ساخته شود (`TNatsFrame.Subject` و غیره).
3. مقایسه opcode بدون allocate.

**Acceptance:**

- [x] همهٔ تست‌های parser (U-01…U-05 و موارد TEST_PLAN بعدی) سبز.
- [x] رفتار قاب‌های ناقص / سقف `NATS_MAX_FRAME_BYTES` بدون تغییر معنایی.

#### SPEC-PERF-03 — INFO / CONNECT / JS JSON با `Dext.Json.Utf8`

**مشکل:** `System.JSON` (`TJSONObject.ParseJSONValue`) برای INFO و helperهای JSON.

**الزامات:**

1. `TNatsServerInfo.Parse` از `TUtf8JsonReader` روی `TByteSpan` / UTF-8 bytes استفاده کند.
2. `TNatsConnectOptions.ToJson` با `TUtf8JsonWriter` (sink مستقیم به بافر) یا writer معادل نوشته شود — خروجی بایت یا string پایدار برای wire.
3. JetStream `ToJson` / `Parse`های admin همان الگو را بگیرند (PERF-03b: PubAck، Stream/Consumer info، error object، Delete success).
4. توابع `NatsJsonGetStr/Int/Int64/Bool` حذف شوند وقتی JetStream دیگر به `System.JSON` وابسته نیست.

**مرجع API Dext:**

```text
Dext.Json.Utf8.pas
  TUtf8JsonReader.Create(TByteSpan)
  Read / TokenType / ValueSpan / ValueSpanEquals / GetString / GetInt64 / GetBoolean
  TUtf8JsonWriter.Create(Context, TUtf8WriteProc)  // بدون Stream میانی ترجیح داده می‌شود
```

**Acceptance:**

- [x] U-01، U-02، U-07 و تست‌های JS JSON موجود سبز.
- [x] `uses System.JSON` از `Protocol.pas` و `JetStream.pas` حذف شد؛ `NatsJsonGet*` حذف شد (PERF-03b).
- [x] فیلدهای INFO: `server_id`, `version`, `go`, `host`, `port`, `headers`, `auth_required`, `tls_required`, `max_payload`, `connect_urls`, `jetstream`, `proto` (در حد پشتیبانی فعلی) حفظ شوند.
- [x] PERF-03b: JetStream Stream/Consumer `ToJson` + StreamInfo/ConsumerInfo/PublishAck `Parse` + API error/`success` روی `TUtf8JsonReader`/`TUtf8JsonWriter`.

#### SPEC-PERF-04 — Payload: مالکیت واضح + حداقل کپی

**الزامات:**

1. تا وقتی `TNatsMsg.Payload: TBytes` عمومی است، یک کپی به مالکیت handler لازم است (قرارداد فعلی).
2. داخل parser، از double-copy پرهیز شود: یک `Move` از بافر داخلی به `Payload`.
3. (اختیاری / آینده) overload داخلی یا record آزمایشی با `TByteSpan` فقط تا پایان `HandleMsgFrame` — **بدون** expose در API پایدار تا lifetime مشخص شود.

**Acceptance:**

- [x] هیچ API عمومی payload را به‌صورت view ناپایدار برنگرداند مگر با doc صریح و تست lifetime.
- [x] تست integration pub/sub و request/reply بدون رگرسیون.

#### SPEC-PERF-05 — سقف ایمنی و تخصیص بافر parser

**الزامات:**

1. رشد بافر `TDextNatsFrameParser` با ظرفیت توان‌دو یا رشد هندسی (نه `+4096` کور در هر بار اگر باعث fragmentation شود — انتخاب مستند در کامنت کوتاه).
2. `Clear` روی reconnect همان معنای فعلی را داشته باشد.
3. `MaxFrameBytes` همچنان نقض را با `EDextNatsProtocolError` (یا معادل فعلی) بشکند.

**Acceptance:**

- [x] تست ceiling از `TEST_PLAN` (اگر موجود) یا تست جدید unit برای frame oversized.

---

### فاز DI — Dependency Injection

#### SPEC-DI-01 — Unit ثبت سرویس

**فایل جدید:** `Source/Dext.Net.Nats.DependencyInjection.pas`

**الزامات:**

```delphi
/// <summary>Registers TDextNatsClient as singleton (or scoped — documented) in the DI container.</summary>
procedure AddNatsClient(const AServices: IServiceCollection); overload;
procedure AddNatsClient(const AServices: IServiceCollection; const AOptions: TDextNatsOptions); overload;
procedure AddNatsClient(const AServices: IServiceCollection; const AHost: string; APort: Word = NATS_DEFAULT_PORT); overload;
```

الگوی مرجع: `RegisterRedisClient` در `Dext.Net.Redis.pas` (`AddSingleton` + `TServiceType.FromClass` + factory).

**رفتار:**

1. Factory یک `TDextNatsClient.Create(options)` بسازد.
2. **Connect خودکار در factory نکند** مگر overload صریح `AddNatsClientAndConnect(...)` مستند شود — ترجیح: اتصال در `IHostedService` / کد اپ.
3. Lifetime پیش‌فرض: **Singleton** (یک اتصال اشتراکی)، مستند در XML doc.
4. اختیاری: ثبت `TDextNatsJetStreamContext` به‌صورت transient/factory که همان client را resolve کند و **مالکیت client را نگیرد** (composition فعلی حفظ شود).

**Acceptance:**

- [x] کامپایل unit با `Dext.DI.Interfaces` (`Dext.Net.Nats.DependencyInjection`).
- [x] تست DI: resolve → همان instance برای singleton (`AddNatsClient_ShouldResolveSingleton` + JetStream transient).
- [x] بدون وابستگی معکوس JetStream → DI اجباری برای کاربران غیر-DI.

#### SPEC-DI-02 — Options از Configuration (اختیاری ولی توصیه‌شده)

**الزامات:**

1. اگر `Dext.Options` / Configuration binder در دسترس است، `TDextNatsOptions` از section مثلاً `"Nats"` bind شود.
2. فیلدها با نام پایدار: `Host`, `Port`, به‌علاوه فیلدهای موجود options + `TLS.*`.
3. اگر binder آماده نیست، این spec را به فاز بعدی موکول کن و فقط `AddNatsClient(options)` را در PERF/DI اولیه نگه دار.

**Acceptance:**

- [x] `TDextNatsClientSettings` + `BindNatsOptions` / `AddNatsClient(services, configuration, 'Nats')` با `TConfigurationBinder` (binder فقط `class` می‌پذیرد؛ record options از `ToOptions` ساخته می‌شود).
- [x] تست: `BindNatsOptions_FromConfiguration_ShouldMapHostPortTls`.

---

### فاز LOG — Logging

#### SPEC-LOG-01 — تزریق اختیاری `ILogger`

**الزامات:**

1. `TDextNatsClient` یک property یا constructor overload بپذیرد: `Logger: ILogger` (nil = خاموش، رفتار فعلی).
2. Category پیشنهادی: `'Dext.Net.Nats'`.
3. رویدادهای حداقل:
   | سطح | رویداد |
   |-----|--------|
   | Information | connect موفق / reconnect موفق (با `server_id`, host, port, `AIsReconnect`) |
   | Warning | شروع reconnect، stale ping → disconnect |
   | Error | `-ERR`، شکست reconnect، exception در handler (بدون بلعیدن — همان FireError) |
   | Debug | SUB/UNSUB replay شمارش (بدون subjectهای حساس در production مگر Debug) |
4. هرگز credential (password / token / NKey seed) را log نکن.
5. Logging روی مسیر داغ MSG **پیش‌فرض خاموش** باشد (فقط Debug و در صورت `IsEnabled`).

**مرجع:** `Dext.Logging.pas` — `ILogger.LogInformation/Warning/Error`, `IsEnabled`.

**Acceptance:**

- [x] بدون logger → صفر تغییر رفتار externally observable.
- [x] با logger mock در تست: `Logger_FireError_ShouldRecordWhenAttached` (+ connect path در observability suite).

---

### فاز MET — Metrics

#### SPEC-MET-01 — شمارنده‌های استاندارد

**نام‌ها (پیشنهادی، پایدار):**

| متریک | نوع | معنی |
|--------|-----|------|
| `nats.msgs.received` | Counter | MSG/HMSG تحویل‌شده به handler |
| `nats.msgs.published` | Counter | PUB/HPUB ارسال‌شده (موفق از `SendRaw`) |
| `nats.reconnects` | Counter | reconnect موفق |
| `nats.errors` | Counter | `-ERR` / FireError مسیرهای شمارش‌پذیر |
| `nats.ping.rtt_ms` | Histogram | اختیاری؛ فقط اگر Flush/Ping اندازه‌گیری کند |
| `nats.connected` | Gauge | 1/0 |

**الزامات:**

1. استفاده از `TMetrics.Increment` / `Gauge` / `Histogram` در `Dext.Telemetry.Metrics`.
2. فراخوانی‌ها ارزان و بدون allocate رشته در حلقهٔ تنگ — نام متریک ثابت.
3. قابلیت خاموش کردن با `Options.EnableMetrics: Boolean` (default True یا False — در CreateDefault مستند شود؛ پیشنهاد: **False** تا opt-in باشد و نویز اپ‌های بدون sink کم شود).

**Acceptance:**

- [x] تست unit با فعال‌سازی metrics: `Metrics_Publish_ShouldIncrementLocalCounter` / `Metrics_ShouldDefaultDisabled`.
- [x] مسیر بدون metrics بدون هزینهٔ قابل‌توجه (branch روی `EnableMetrics`, default False).

---

### فاز HLTH — Health Checks

#### SPEC-HLTH-01 — `TNatsHealthCheck`

**الزامات:**

1. کلاس `TNatsHealthCheck = class(TInterfacedObject, IHealthCheck)` که `TDextNatsClient` را از DI یا constructor می‌گیرد.
2. `CheckHealth`:
   - `Connected = True` → `THealthCheckResult.Healthy('NATS connected')`
   - در غیر این صورت → `Unhealthy('NATS disconnected')`
3. اختیاری (P2b): `Flush(500)` سبک برای liveness عمیق‌تر؛ timeout → Unhealthy؛ فقط اگر Options بگوید تا health endpoint کند نشود.
4. ثبت: `AddNatsHealthCheck(AServices)` کنار DI.

**مرجع:** `Dext.HealthChecks.pas` — `IHealthCheck`, `THealthCheckResult`.

**Acceptance:**

- [x] تست: client قطع → Unhealthy (`HealthCheck_ShouldReportUnhealthyWhenDisconnected`).

---

### فاز ASYNC — Async API

#### SPEC-ASYNC-01 — `RequestAsync` مبتنی بر `TAsyncBuilder`

**الزامات:**

1. overload جدید بدون شکستن callback فعلی:

```delphi
function RequestAsync(const ASubject: string; const APayload: TBytes;
  ATimeoutMs: Integer = 0): TAsyncBuilder<TNatsMsg>; overload;
```

2. پیاده‌سازی با `TAsyncTask.Run` که همان منطق sync `Request` / claim-gate را روی thread pool اجرا کند **یا** await روی event موجود — بدون race جدید روی `TEvent`.
3. `FlushAsync` مشابه اختیاری.
4. callback-based `RequestAsync(..., AOnReply, AOnTimeout)` باقی بماند.

**مرجع:** `Dext.Threading.Async.pas` — `TAsyncBuilder<T>`, `TAsyncTask.Run<T>`.

**Acceptance:**

- [x] integration: `RequestAsync(...).Await` همان payload را برگرداند (`RequestAsyncBuilder_ShouldAwaitReply`).
- [x] timeout → `EDextNatsTimeoutError` از `Await` (`RequestAsyncBuilder_Timeout_ShouldRaise`).
- [x] claim-gate همان مسیر sync `Request` است (بدون race جدید روی `TEvent`); callback overload حفظ شد؛ `FlushAsync` نیز اضافه شد.

---

### فاز AUTH — NKey / JWT

#### SPEC-AUTH-01 — CONNECT با JWT + NKey sig

**الزامات:**

1. فیلدهای جدید در `TNatsConnectOptions` / `TDextNatsOptions`: `JWT`, `NKeySeed` (یا `NKey` + callback امضا).
2. هنگام handshake، اگر seed تنظیم شده: nonce از INFO را با NKey امضا کن و در CONNECT بفرست (`sig`, `jwt` طبق پروتکل NATS).
3. از OpenSSL / primitives موجود Dext استفاده کن؛ وابستگی جدید خارج از Dext ممنوع مگر ضروری و مستند.
4. seed هرگز در log/metrics نرود.
5. اگر سرور `auth_required` و credentials نباشد → خطای واضح قبل یا بعد از `-ERR`.

**Acceptance:**

- [x] unit: امضای nonce با fixture شناخته‌شده (vector تست).
- [x] integration اختیاری env-gated با سرور NKey (`Tests/nkey/`).

---

### فاز PUSH — JetStream push consumer

#### SPEC-PUSH-01 — Consumer push با deliver subject

**الزامات:**

1. API روی `TDextNatsJetStreamContext`:
   - ایجاد consumer با `DeliverSubject` / `DeliverGroup` (فیلدهای config موجود یا جدید).
   - `SubscribePush(const AStream, AConsumer; AHandler)` که SUB روی deliver subject بزند و پیام‌ها را به `TNatsJsMsg` تبدیل کند.
2. Ack/Nak/Term/InProgress همان helpers فعلی.
3. unsubscribe تمیز روی dispose/cancel.
4. push **مالک thread جدا نسازد** — از RecvLoop/handler مدل فعلی Client استفاده کند.

**Acceptance:**

- [x] integration با `nats-server -js`: publish → push handler → Ack.
- [x] حذف consumer/subscription بدون leak SID.

---

### فاز DOC

#### SPEC-DOC-01 — README

**الزامات:**

1. `README.md` در ریشه: نصب، Connect، Pub/Sub، Request، JetStream Fetch، TLS، DI (`AddNatsClient`)، نمونه Logger.
2. لینک به `Docs/TEST_PLAN.md` و این سند.
3. بدون وعدهٔ قابلیت‌های P3 ناتمام.

---

### فاز KV — JetStream Key-Value

#### SPEC-KV-01 — KV bucket MVP (Put/Get/Delete)

**الزامات:**

1. یونیت `Dext.Net.Nats.KeyValue.pas`: `TDextNatsKeyValue` روی `TDextNatsJetStreamContext` (composition؛ مالک کلاینت/JS نیست).
2. Bucket = stream `KV_<bucket>` با subjects `$KV.<bucket>.>`؛ کلید = `$KV.<bucket>.<key>`.
3. API MVP: `CreateBucket` / `DeleteBucket` / `BucketExists` / `GetStatus` / `Open`، و روی store: `Put` / `Get` / `TryGet` / `Delete` / `Purge` / `Status` / CAS `Create` / `Update`.
4. Delete = header `KV-Operation: DEL`؛ Purge = `KV-Operation: PURGE` + `Nats-Rollup: sub`.
5. Get از `STREAM.MSG.GET` (`last_by_subj`)؛ tombstone → not found.
6. `Keys` / `ListKeys` (pull `last_per_subject`)، `History(key)` (pull `all`)، `Watch` / `WatchAll` (push `last_per_subject` + updates؛ بدون marker پایان snapshot اولیه).
7. CAS: `Create` = `Nats-Expected-Last-Subject-Sequence: 0` (+ retry روی tombstone revision مثل nats.go)؛ `Update(key, value, revision)` همان header با revision؛ conflict → `EDextNatsKeyExists` / `EDextNatsKeyRevisionMismatch`.
8. `Dext.Json.Utf8` / `Dext.Collections`؛ بدون `System.JSON` / `System.Generics.Collections`.

**Acceptance:**

- [x] Unit: `ToStreamConfig` / `TNatsStoredMsg.Parse` / نام نامعتبر.
- [x] Integration soft-skip: Put/Get/Delete (+ Purge) روی `nats-server -js`.
- [x] Integration soft-skip: `Keys` / `History` / `WatchAll` روی `nats-server -js`.
- [x] Integration soft-skip: CAS `Create` / `Update` (+ KeyExists / revision mismatch) روی `nats-server -js`.
- [ ] **Deferred:** per-key TTL، Watch end-of-initial marker / MetaOnly.

---

## ۵. تغییرات فایل (پیش‌بینی)

| فایل | فاز | اقدام |
|------|-----|--------|
| `Source/Dext.Net.Nats.Protocol.pas` | PERF, AUTH | Utf8 JSON، writer، parse span، CONNECT auth fields |
| `Source/Dext.Net.Nats.pas` | LOG, MET, ASYNC, AUTH | Logger، metrics flag، async overloads، sig handshake |
| `Source/Dext.Net.Nats.JetStream.pas` | PERF-03b, PUSH, KV | Utf8 parse admin؛ push API؛ MSG.GET / KV stream flags |
| `Source/Dext.Net.Nats.KeyValue.pas` | KV | **جدید** — Put/Get/Delete/Purge + Keys/History/Watch + CAS Create/Update |
| `Source/Dext.Net.Nats.DependencyInjection.pas` | DI | **جدید** |
| `Source/Dext.Net.Nats.HealthChecks.pas` | HLTH | **جدید** (یا ادغام در DI unit اگر کوچک ماند) |
| `Tests/Dext.Net.Nats.Tests.pas` | همه | fixtureهای جدید per SPEC |
| `README.md` | DOC | **جدید** |
| `AGENTS.md` | همه | تیک زدن pending با ارجاع به این سند |

وابستگی‌های Dext مجاز برای افزودن تدریجی:

```text
Dext.Json.Utf8
Dext.Json.Types          (اگر Writer نیاز دارد)
Dext.DI.Interfaces
Dext.Logging
Dext.Telemetry.Metrics
Dext.HealthChecks
Dext.Threading.Async
Dext.Threading.CancellationToken   (برای WithCancellation روی async)
```

Protocol باید تا حد ممکن **سبک** بماند: ترجیحاً فقط `Span` + `Json.Utf8` + `Collections`؛ DI/Logging وارد Protocol نشوند.

---

## ۶. قیود کیفیت و همزمانی

| ID | قید |
|----|-----|
| CONN-01 | هیچ PRای حق ندارد `TryReconnect` را از `PingLoop` صدا بزند |
| CONN-02 | `SendRaw` تنها نقطهٔ write سوکت/TLS بماند |
| CONN-03 | handler پیام روی RecvThread اجرا می‌شود — مستند بماند؛ deadlock با `Request.Await` از داخل handler ممنوع/مستند |
| MEM-01 | فقط `Dext.Collections` |
| MEM-02 | Free نکردن `TEvent` قبل از `TryClaim` |
| API-01 | breaking change عمومی فقط با bump نسخه / یادداشت README |
| SEC-01 | عدم log کردن secrets |
| PERF-INV | بعد از فاز PERF، `System.JSON` در Protocol نباشد |

---

## ۷. نقشهٔ تست به ازای Spec

| Spec | نوع تست | شناسهٔ پیشنهادی در suite |
|------|---------|---------------------------|
| PERF-01 | Unit encode | U-PERF-01 |
| PERF-02 | Unit parser binary opcodes | U-PERF-02 |
| PERF-03 | Unit INFO/CONNECT utf8 | U-PERF-03 |
| PERF-05 | Unit max frame | U-PERF-05 |
| DI-01 | Unit DI resolve | U-DI-01 |
| LOG-01 | Unit mock logger | U-LOG-01 |
| MET-01 | Unit metrics flag | U-MET-01 |
| HLTH-01 | Unit/Integration | U-HLTH-01 / I-HLTH-01 |
| ASYNC-01 | Integration Await | I-ASYNC-01 |
| AUTH-01 | Unit vector + opt Integration | U-AUTH-01 / I-AUTH-01 |
| PUSH-01 | JetStream integration | J-PUSH-01 |
| KV-01 | Unit + JetStream KV integration | U-KV-01 / J-KV-01 |

جزئیات fixtureها هنگام اجرا به [`TEST_PLAN.md`](TEST_PLAN.md) اضافه شوند.

---

## ۸. تعریف Done برای کل نقشه

- [x] همهٔ SPECهای P0 و P1 پیاده و تست‌شده (PERF، DI incl. DI-02، LOG، MET، HLTH، ASYNC، AUTH، PUSH)
- [x] `AGENTS.md` pending به‌روز (هیچ `[ ]` باز نمانده)
- [x] هیچ رگرسیون در suite پیش‌فرض (Unit+Integration+JS+TLS soft-skip)؛ stress با `DEXT_NATS_RUN_STRESS=1` سبز
- [x] README حداقل برای Connect / PubSub / Request / DI
- [x] P2/P3 یا Done یا صریحاً «deferred» با دلیل در همین سند (همهٔ acceptanceهای اختیاری PERF-01 نیز بسته شد)

---

## ۹. ترتیب PRهای پیشنهادی

1. **PR-PERF:** Protocol writer + span control-line + Utf8 INFO/CONNECT (بدون DI)
2. **PR-DI-LOG:** `DependencyInjection.pas` + optional `ILogger`
3. **PR-OBS:** Metrics + HealthCheck
4. **PR-ASYNC:** `TAsyncBuilder` overloads
5. **PR-AUTH:** NKey/JWT
6. **PR-PUSH:** JetStream push
7. **PR-DOC:** README

هر PR باید مستقلاً قابل merge و سبز روی تست‌های مربوط باشد.

---

## ۱۰. خلاصهٔ یک‌خطی برای Agent

> NATS را مثل یک شهروند درجهٔ یک Dext جلو ببر: اول صفر کردن تخصیص اضافه در Protocol با Span/Utf8، بعد DI+Logging، بعد Metrics/Health/Async، بعد Auth و Push؛ MQTT را دست نزن؛ قرارداد RecvLoop/PingLoop را نشکن.
