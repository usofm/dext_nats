# Dext.Nats

**Version:** [1.0.0](CHANGELOG.md) (2026-08-11)

Native [NATS](https://nats.io) client for the [Dext Framework](https://github.com/) (Delphi 12 / Studio 23.0). Built on `Dext.Net.Tcp` with optional TLS via `Dext.Net.Security`, plus JetStream (KV / Object Store).

## Units

| Unit | Role |
|------|------|
| `Source/Dext.Net.Nats.Protocol.pas` | Wire protocol (parser, INFO/CONNECT, PUB/SUB, headers) — no sockets |
| `Source/Dext.Net.Nats.NKeys.pas` | NKey seed / `.creds` parse, Ed25519 nonce signing, CONNECT `jwt`/`nkey`/`sig` |
| `Source/Dext.Net.Nats.pas` | `TDextNatsClient` — connect, pub/sub, request/reply, reconnect, `Drain`/`DrainAsync`/`IsDraining`, TLS, NKey/JWT, optional `ILogger` / metrics |
| `Source/Dext.Net.Nats.DependencyInjection.pas` | `AddNatsClient` / configure / config bind (`Nats` section) / `AddNatsJetStream` |
| `Source/Dext.Net.Nats.HealthChecks.pas` | `TNatsHealthCheck` / `AddNatsHealthCheck` (Connected probe) |
| `Source/Dext.Net.Nats.JetStream.pas` | `TDextNatsJetStreamContext` — streams (`ListStreams` / `ListStreamNames`), pull/push consumers (`ListConsumers` / `ListConsumerNames`), Fetch, SubscribePush, Ack/Nak/Term |
| `Source/Dext.Net.Nats.KeyValue.pas` | `TDextNatsKeyValue` — JetStream KV (Put/Get/Delete/Purge, Keys, History, Watch/WatchAll, CAS Create/Update) |
| `Source/Dext.Net.Nats.ObjectStore.pas` | Object Store (`CreateStore` / `UpdateStore`/`UpdateObjectStore` / `Put` / `Get` / `Delete` / `List` / `Keys`, `Watch`/`WatchAll` with EndOfInitial + MetaOnly/UpdatesOnly, `UpdateMeta`, `Seal`, `AddLink` / `AddBucketLink`, streaming `Put`/`Get` via `TStream` + `PutFile`/`GetFile`) |

## Quick start

```delphi
uses Dext.Net.Nats, Dext.Net.Nats.JetStream, Dext.Net.Security;

var
  Client: TDextNatsClient;
  Reply: TNatsMsg;
  Js: TDextNatsJetStreamContext;
begin
  Client := TDextNatsClient.Create;
  try
    Client.Connect('127.0.0.1', 4222);

    Client.Subscribe('orders.>',
      procedure(const AMsg: TNatsMsg)
      begin
        Writeln(AMsg.Subject, ' = ', AMsg.AsString);
      end);

    Client.Publish('orders.new', 'sku-1');

    // Request/reply (inbox + timeout)
    Reply := Client.Request('svc.echo', 'ping', 2000);

    // Graceful shutdown (UNSUB all → flush → disconnect); or Client.DrainAsync(5000).Await
    // Client.Drain(5000);

    Js := TDextNatsJetStreamContext.Create(Client);
    try
      // Stream/consumer admin (incl. ListStreams / ListConsumers) + Fetch/Ack —
      // see Demo/JetStreamSmokeTest
    finally
      Js.Free;
    end;

    // Object Store (chunked blobs on OBJ_<bucket> / $O.<bucket>.*)
    // uses Dext.Net.Nats.ObjectStore;
    // Os := TDextNatsObjectStoreContext.Create(Client);
    // Store := Os.CreateStore(TNatsObjectStoreConfig.CreateDefault('INVOICES'));
    // Store.Put('invoice.pdf', Bytes); Got := Store.Get('invoice.pdf');
    // Store.PutFile('invoice.pdf', 'C:\data\invoice.pdf');      // chunked from file
    // Store.GetFile('invoice.pdf', 'C:\out\invoice.pdf');
    // Store.Put(Meta, SomeStream); Store.Get('invoice.pdf', OutStream);
    // Store.AddLink('label.png', Store.GetInfo('invoice.pdf')); // Get follows
    // Store.AddBucketLink('other', OtherStore);                 // Get raises
    // Watcher := Store.WatchAll(procedure(const Info: TNatsObjectInfo)
    //   begin if Info.IsEndOfInitial then Exit; ... end);
    // // optional: TNatsObjectStoreWatchOptions.MetaOnly / UpdatesOnly
  finally
    Client.Free;
  end;
end;
```

Object Store buckets are JetStream streams (`OBJ_<bucket>`, subjects `$O.<bucket>.{C,M}.*`). `UpdateStore` / `UpdateObjectStore` maps mutable `TNatsObjectStoreConfig` fields (description, max bytes, TTL/`MaxAge`, storage, replicas, `Compression`, `Placement`) onto `STREAM.UPDATE`. Stream config also accepts `TNatsStoreCompression` (`scS2` → `"compression":"s2"`) and `TNatsPlacement` (`cluster` / `tags`). `Watch` / `WatchAll` push on meta subjects (`last_per_subject` + updates) and deliver an `EndOfInitial` marker when the snapshot is done — check `AInfo.IsEndOfInitial` or `Watcher.InitialDone`. Optional `TNatsObjectStoreWatchOptions.MetaOnly` (`headers_only`; name from subject, no ObjectInfo JSON) / `UpdatesOnly` (`deliver_policy=new`; no marker).

### JetStream Key-Value

Buckets are JetStream streams (`KV_<bucket>`, subjects `$KV.<bucket>.>`). API: put/get/delete/purge, CAS `Create`/`Update`, `Keys`/`ListKeys`, `History`, `Watch`/`WatchAll` (push `last_per_subject` + updates; delivers an `EndOfInitial` marker when the snapshot is done — check `AEntry.IsEndOfInitial` or `Watcher.InitialDone`; optional `TNatsKeyValueWatchOptions.MetaOnly` / `UpdatesOnly`), and per-key TTL (NATS 2.11+: set `LimitMarkerTTL` on the bucket, then `Create(key, value, ttlNanos)` / `Purge(key, ttlNanos)` via `Nats-TTL`; bucket-level `TTL` remains stream `MaxAge`. Put/Update do not take a TTL — ADR-48).

```delphi
uses Dext.Collections, Dext.Net.Nats, Dext.Net.Nats.JetStream, Dext.Net.Nats.KeyValue;

var
  Client: TDextNatsClient;
  Js: TDextNatsJetStreamContext;
  Kv: TDextNatsKeyValue;
  Entry: TNatsKeyValueEntry;
  Cfg: TNatsKeyValueConfig;
  KeyNames: IList<string>;
  Rev: UInt64;
begin
  Client := TDextNatsClient.Create;
  try
    Client.Connect('127.0.0.1', 4222); // nats-server -js
    Js := TDextNatsJetStreamContext.Create(Client);
    try
      Cfg := TNatsKeyValueConfig.CreateDefault('INVENTORY');
      Cfg.Storage := ssMemory;
      Cfg.History := 5;
      Kv := TDextNatsKeyValue.CreateBucket(Js, Cfg);
      try
        Rev := Kv.Create('widget-blue', '42'); // fails with EDextNatsKeyExists if present
        Entry := Kv.Get('widget-blue');        // Entry.AsString = '42'
        Rev := Kv.Update('widget-blue', '41', Entry.Revision); // CAS; mismatch → EDextNatsKeyRevisionMismatch
        KeyNames := Kv.Keys;                   // live keys via last_per_subject
        // Hist := Kv.History('widget-blue');
        // Watcher := Kv.WatchAll(procedure(const E: TNatsKeyValueEntry)
        //   begin if E.IsEndOfInitial then Exit; ... end);
        Kv.Delete('widget-blue');
      finally
        Kv.Free;
        TDextNatsKeyValue.DeleteBucket(Js, 'INVENTORY');
      end;
    finally
      Js.Free;
    end;
  finally
    Client.Free;
  end;
end;
```

### TLS

Enable TLS on options (upgrade after cleartext INFO when `tls_required` or `TLS.Enabled`):

```delphi
var
  Opts: TDextNatsOptions;
begin
  Opts := TDextNatsOptions.CreateDefault;
  Opts.TLS := TDextTLSOptions.DefaultClient;
  Opts.TLS.Enabled := True;
  Opts.TLS.VerifyServerCertificate := False; // local/self-signed
  Client := TDextNatsClient.Create(Opts);
  Client.Connect('127.0.0.1', 4223);
end;
```

Sample self-signed fixture: `Tests/tls/` (`nats-server -c Tests/tls/nats-tls.conf`).

### NKey / JWT auth

After cleartext INFO, if the server sends a `nonce` and you configured credentials, the client signs with the NKey seed and sends CONNECT `sig` plus either `jwt` (credentials) or public `nkey` (bare NKey).

```delphi
var
  Opts: TDextNatsOptions;
begin
  Opts := TDextNatsOptions.CreateDefault;
  // Bare NKey (server authorization.users[].nkey = public U… key):
  Opts.NKeySeed := 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
  // Or JWT + seed from a .creds file (field JWT/NKeySeed override file values when set):
  // Opts.CredentialsFile := 'C:\secrets\user.creds';
  Client := TDextNatsClient.Create(Opts);
  Client.Connect('127.0.0.1', 4224);
end;
```

Requires OpenSSL `libcrypto-3.dll` (same as TLS). Fixture: `Tests/nkey/`.

### Dependency Injection

```delphi
uses Dext.DI.Interfaces, Dext.Net.Nats.DependencyInjection, Dext.Net.Nats, Dext.Net.Nats.JetStream;

var
  Services: TDextServices;
  Provider: IServiceProvider;
  Client: TDextNatsClient;
  Js: TDextNatsJetStreamContext;
begin
  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap, '127.0.0.1', 4222); // singleton, not connected
  AddNatsJetStream(Services.Unwrap);                 // transient; does not own client
  Provider := Services.BuildServiceProvider;

  Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(Provider);
  Client.Connect; // uses Options.Host / Options.Port
  Js := TDextServices.GetRequiredServiceObject<TDextNatsJetStreamContext>(Provider);
  try
    // ...
  finally
    Js.Free; // transient class instances are caller-owned
  end;
end;
```

`AddNatsClientAndConnect` connects inside the factory on first resolve (prefer explicit `Connect` when you need startup error handling). Configure via callback or configuration section:

```delphi
AddNatsClient(Services.Unwrap,
  procedure(var AOptions: TDextNatsOptions)
  begin
    AOptions.Host := '127.0.0.1';
  end);

// Or bind section "Nats" (Host, Port, Name, TLS:Enabled, …):
// AddNatsClient(Services.Unwrap, Configuration);
```

```delphi
AddNatsClient(Services.Unwrap,
  procedure(var O: TDextNatsOptions)
  begin
    O.Host := '127.0.0.1';
    O.Port := 4222;
    O.EnableMetrics := True;
  end);
```

### Observability

- **Logger:** set `Client.Logger` (or register `ILoggerFactory` before resolve — DI attaches category `Dext.Net.Nats`). Never logs secrets.
- **Metrics:** set `Options.EnableMetrics := True` to also publish `nats.msgs.received|published`, `nats.reconnects`, `nats.errors`, `nats.connected` via `TMetrics`. Always available locally as `Client.Metrics`.
- **Health:** `AddNatsHealthCheck` + `TNatsHealthCheck.CheckHealth` (Healthy when `Connected`). Web apps can map the result onto `Dext.HealthChecks.IHealthCheck`.

```delphi
Client.Logger := LoggerFactory.CreateLogger('Dext.Net.Nats');
Client.Options.EnableMetrics := True;
// ...
Check := TNatsHealthCheck.Create(Client);
try
  Res := Check.CheckHealth;
finally
  Check.Free;
end;
```

## Tests

Requires **Delphi 12 / Studio 23.0** (`dcc32`). Framework: `Dext.Testing`. Open `Dext.Nats.groupproj` for the tests project and all demos.

```bat
set BDS=C:\Program Files (x86)\Embarcadero\Studio\23.0
set PATH=%BDS%\bin;%PATH%
msbuild Tests\Dext.Net.Nats.Tests.dproj /p:Config=Debug /p:Platform=Win32 /t:Build
```

Run (unit tests always run; live tests soft-skip if no server):

```bat
rem optional live server
nats-server -js

Output\Win32\Debug\Dext.Net.Nats.Tests.exe
```

| Env var | Meaning |
|---------|---------|
| *(default)* | Soft-skip Integration/JetStream when `127.0.0.1:4222` (or `DEXT_NATS_HOST`/`PORT`) is down |
| `DEXT_NATS_REQUIRE_LIVE=1` | Hard-fail if cleartext/JS server missing (strict CI) |
| `DEXT_NATS_SKIP_LIVE=1` | Soft-skip all live suites |
| `DEXT_NATS_TLS_PORT=4223` | Enable TLS tests (`DEXT_NATS_TLS_HOST` optional) |
| `DEXT_NATS_NKEY_PORT=4224` | Enable NKey tests (`DEXT_NATS_NKEY_SEED` or `*_SEED_FILE` / `DEXT_NATS_CREDS_FILE`) |
| `DEXT_NATS_RUN_STRESS=1` | Run Explicit stress tests |

Full matrix and IDs: [`Docs/TEST_PLAN.md`](Docs/TEST_PLAN.md). Forward plan after the completed Dext roadmap: [`Docs/NATS_FUTURE_PLAN.md`](Docs/NATS_FUTURE_PLAN.md). MQTT vs NATS architecture notes: [`Docs/MQTT_VS_NATS.md`](Docs/MQTT_VS_NATS.md). B2B agent draft: [`Docs/NATS_B2B_AGENT_PLAN.md`](Docs/NATS_B2B_AGENT_PLAN.md).

## Manual E2E demos

Interactive programs under `Demo/` (not a `Dext.Testing` suite). Requires **Delphi 12 / Studio 23.0** (`msbuild` / `dcc32`). Set `BDS` and `PATH` as in [Tests](#tests) if needed. Most console demos accept `[host] [port]` and `-no-wait`; optional `DEXT_NATS_HOST` / `DEXT_NATS_PORT` where noted in each `.dpr` header.

| Demo | What it covers | `nats-server` | Build | Run |
|------|----------------|---------------|-------|-----|
| `Demo/PubSubE2E/` | Core one-way pub/sub | `nats-server` (plain; `-js` unused) | `msbuild Demo\PubSubE2E\PubSubE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\PubSubE2E.exe` |
| `Demo/RequestReplyE2E/` | Request/reply + no-responders | `nats-server` | `msbuild Demo\RequestReplyE2E\RequestReplyE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\RequestReplyE2E.exe` |
| `Demo/QueueGroupE2E/` | Queue-group load balancing | `nats-server` | `msbuild Demo\QueueGroupE2E\QueueGroupE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\QueueGroupE2E.exe` |
| `Demo/HeadersE2E/` | Headers round-trip (HPUB/HMSG + `RequestWithHeaders`) | `nats-server` (needs `headers` in INFO) | `msbuild Demo\HeadersE2E\HeadersE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\HeadersE2E.exe` |
| `Demo/TlsE2E/` | TLS upgrade after cleartext INFO | `nats-server -c Demo\TlsE2E\nats-tls.conf` from repo root (port **4223**; certs from `Tests/tls/`) | `msbuild Demo\TlsE2E\TlsE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\TlsE2E.exe` (default `127.0.0.1:4223`) |
| `Demo/NKeyE2E/` | NKey (bare seed) auth handshake | `nats-server -c Demo\NKeyE2E\nats-nkey.conf` (port **4224**; same user as `Tests/nkey/`) | `msbuild Demo\NKeyE2E\NKeyE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\NKeyE2E.exe` (default `127.0.0.1:4224`) |
| `Demo/JetStreamSmokeTest/` | Stream admin, dedup publish, pull Fetch/Ack | `nats-server -js` | `msbuild Demo\JetStreamSmokeTest\JetStreamSmokeTest.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\JetStreamSmokeTest.exe` |
| `Demo/KeyValueE2E/` | JetStream KV (CreateBucket / Put / Get / Keys / History / Delete / Purge, CAS Create/Update, WatchAll EndOfInitial + IncludeHistory Watch, optional per-key TTL, DeleteBucket) | `nats-server -js` | `msbuild Demo\KeyValueE2E\KeyValueE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\KeyValueE2E.exe` |
| `Demo/ObjectStoreE2E/` | JetStream Object Store (CreateStore / Put / Get / PutFile / GetFile / List / Keys / Delete / UpdateMeta / WatchAll / AddLink / Seal / DeleteStore) | `nats-server -js` | `msbuild Demo\ObjectStoreE2E\ObjectStoreE2E.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\ObjectStoreE2E.exe` |
| `Demo/VCLDemo/` | VCL UI: multi-connection tabs, pub/sub, request/reply, log, JetStream (incl. ListStreams / ListConsumers) / Key-Value / Object Store helpers (Object Store form includes AddLink / AddBucketLink + IsLink in List/GetInfo) | `nats-server` (use `-js` for JS/KV/OS forms) | `msbuild Demo\VCLDemo\VCLDemo.dproj /p:Config=Debug /p:Platform=Win32` | `Output\Win32\Debug\VCLDemo.exe` |

Plain-core demos default to `127.0.0.1:4222`. TlsE2E / NKeyE2E need OpenSSL `libssl-3.dll` / `libcrypto-3.dll` beside the exe (same as other TLS/NKey paths). Equivalent TLS config: `cd Tests\tls` then `nats-server -c nats-tls.conf`. Equivalent NKey config: `nats-server -c Tests\nkey\nats-nkey.conf`.

## License

Apache 2.0 — see `LICENSE`.
