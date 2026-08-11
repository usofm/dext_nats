# مقایسهٔ معماری: `Dext.Net.Mqtt` در برابر `Dext.Net.Nats`

> **وضعیت:** سند معماری (غیر اجرایی) — 2026-08-11  
> **غیرهدف:** port قابلیت‌های NATS به MQTT یا برعکس؛ این فایل فقط تصمیم‌گیری و یادگیری است.  
> **مبدأ:** یادداشت کاری ریشهٔ repo (اکنون به اینجا منتقل شد).

## پرسش اولیه

با اسکلت مشترک `TDextTcpClient`، روش پیاده‌سازی در

- `dext/Sources/Net/Dext.Net.Mqtt.pas` (+ parser)
- در برابر `dext_nats/Source/Dext.Net.Nats*.pas`

چه مزیت و هزینه‌ای دارد؟

## اسکلت مشترک

هر دو:

- روی `Dext.Net.Tcp` سوارند
- پروتکل را از I/O جدا کرده‌اند (`Mqtt.Parser` / `Nats.Protocol`)
- دو thread پس‌زمینه برای receive و keepalive دارند (`RecvLoop` / `PingLoop`، معمولاً با `TThread.CreateAnonymousThread`)

پس «روش NATS» تکامل همان الگوی MQTT است، نه یک معماری بیگانه — ولی برای سرویس production سخت‌تر و لایه‌بندی‌شده‌تر شده است.

## جدول مقایسه

| حوزه | MQTT (`Dext.Net.Mqtt`) | NATS (`Dext.Net.Nats*`) |
|------|------------------------|-------------------------|
| لایه‌بندی | Parser + Client (+ Server در همان فضای محصول) | Protocol بدون سوکت → Client → JetStream / KV / ObjectStore با composition |
| Parser | Decode تابعی + بافر دستی‌تر در مسیر receive | `TDextNatsFrameParser` حالت‌دار (`Append` / `TryReadFrame`)، سقف فریم، تست‌پذیر جدا |
| تنظیمات | اغلب hard-code / مینیمال (مثلاً KeepAlive ثابت) | `TDextNatsOptions.CreateDefault` (timeout، reconnect، ping، TLS، auth، metrics، …) |
| Thread-safety ارسال | Publish و Ping بدون serialize سخت‌گیرانهٔ مشترک (ریسک تاریخی) | `FLock` + `FSendLock` (+ `FTlsIoLock` برای TLS) |
| قطع / stale | بستن ساده؛ تشخیص PONG/outstanding ضعیف‌تر | Ping فقط سوکت را می‌بندد؛ فقط `RecvLoop` reconnect می‌کند؛ keepalive با شمارش پینگ |
| Reconnect | عملاً ندارد / مینیمال | outbox، replay سابسکریپشن، چرخش `connect_urls` |
| سابسکریپشن | یک callback سراسری رایج | handler به‌ازای SID، queue group، `MaxMsgs` |
| RPC | ندارد | `Request` / `RequestAsync` با claim-gate |
| رویدادها | عمدتاً پیام | `OnConnected` / `OnDisconnected` / `OnError` |
| امنیت | بدون TLS/auth کامل در کلاینت فعلی | TLS بعد از INFO + user/password/token + NKey/JWT |
| Exceptions | بیشتر `EDextSocketError` | سلسله‌مراتب `EDextNats*` |
| سطح محصول | بروکر داخلی + کلاینت سبک برای دمو/ابزار | کلاینت کامل اکوسیستم NATS (نیاز به `nats-server`) |

**خلاصه:** NATS برای messaging قابل‌اتکا در سرویس؛ MQTT فعلی تمیز ولی مینیمال.

## هزینه‌های رویکرد NATS

1. **حجم و پیچیدگی** — Client + Protocol + JetStream + KV + ObjectStore + DI/Health؛ نگهداری و review سنگین‌تر از MQTT مینیمال است.
2. **سطح اشتباه بالاتر** — reconnect، outbox، TLS، request-gate، Drain؛ قراردادها در [`AGENTS.md`](../AGENTS.md) عمداً سخت‌گیرانه نوشته شده‌اند.
3. **بدون سرور داخلی** — smoke واقعی همیشه به `nats-server` وابسته است (دمو/تست جدا جبران می‌کند).
4. **وابستگی بیشتر به Dext** — Security، Collections، Json.Utf8، Async، Logging/Metrics.
5. **API verboseتر برای UI ساده** — SID/handler قوی‌تر است؛ برای فرم‌ها اغلب یک facade (`TNatsManager` در پلن B2B) پیشنهاد می‌شود.

## ضعف‌های MQTT که الگوی NATS عمداً درست کرده

- ارسال همزمان Publish و Ping بدون serialize  
- نبود تشخیص اتصال کهنه (PONG / outstanding)  
- بافر دریافت پراکنده به‌جای parser واحد قابل unit-test  
- قطع = تمام؛ بدون replay / outbox  
- QoS بالاتر ناقص در کلاینت فعلی  

## جمع‌بندی عملی

| هدف | انتخاب |
|-----|--------|
| کلاینت messaging قابل‌اتکا در سرویس / B2B | **NATS** (وضعیت فعلی Dext.Nats) |
| پروتکل ساده + بروکر داخلی برای تست/ابزار | **MQTT** فعلی کافی‌تر و سبک‌تر است |
| تکامل MQTT در آینده | فقط قرض گرفتن الگوهای NATS — نه کپی JetStream/Request |

## ارزش port از NATS → MQTT (فقط اگر روزی اولویت شود)

اولویت پیشنهادی — **هنوز غیرهدف محصول Dext.Nats است**:

| اولویت | مورد | دلیل |
|--------|------|------|
| 1 | `FSendLock` / serialize ارسال | باگ همزمانی کلاسیک |
| 2 | مالکیت reconnect فقط در Recv | مثل قرارداد AGENTS NATS |
| 3 | Options record + CreateDefault | پیکربندی شفاف |
| 4 | Parser حالت‌دار قابل تست جدا | کیفیت و سقف فریم |
| 5 | OnConnected / OnDisconnected / stale ping | عملیات |
| — | JetStream / Request-Reply / NKey | در MQTT معمولاً بی‌معنا یا خارج از دامنه |

## پیوندها

- [`AGENTS.md`](../AGENTS.md) — قرارداد threading و وضعیت NATS  
- [`NATS_FUTURE_PLAN.md`](NATS_FUTURE_PLAN.md) — این مقایسه آیتم P0 بوده و تکمیل شد  
- [`NATS_DEXT_ROADMAP.md`](NATS_DEXT_ROADMAP.md) — نقشهٔ یکپارچه‌سازی تکمیل‌شده  
- سورس MQTT: `C:\apps_delphi\Comp12\dext\Sources\Net\Dext.Net.Mqtt.pas`
