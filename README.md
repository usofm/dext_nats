# Dext.Nats

Native [NATS](https://nats.io) client for the [Dext Framework](https://github.com/) (Delphi 12 / Studio 23.0). Built on `Dext.Net.Tcp` with optional TLS via `Dext.Net.Security`, plus JetStream pull consumers.

## Units

| Unit | Role |
|------|------|
| `Source/Dext.Net.Nats.Protocol.pas` | Wire protocol (parser, INFO/CONNECT, PUB/SUB, headers) — no sockets |
| `Source/Dext.Net.Nats.NKeys.pas` | NKey seed / `.creds` parse, Ed25519 nonce signing, CONNECT `jwt`/`nkey`/`sig` |
| `Source/Dext.Net.Nats.pas` | `TDextNatsClient` — connect, pub/sub, request/reply, reconnect, TLS, NKey/JWT, optional `ILogger` / metrics |
| `Source/Dext.Net.Nats.DependencyInjection.pas` | `AddNatsClient` / configure callback / `AddNatsJetStream` for Dext.DI |
| `Source/Dext.Net.Nats.HealthChecks.pas` | `TNatsHealthCheck` / `AddNatsHealthCheck` (Connected probe) |
| `Source/Dext.Net.Nats.JetStream.pas` | `TDextNatsJetStreamContext` — streams, pull/push consumers, Fetch, SubscribePush, Ack/Nak/Term |

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

    Js := TDextNatsJetStreamContext.Create(Client);
    try
      // Stream/consumer admin + Fetch/Ack — see Demo/JetStreamSmokeTest
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

`AddNatsClientAndConnect` connects inside the factory on first resolve (prefer explicit `Connect` when you need startup error handling). Configure via callback:

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

Requires **Delphi 12 / Studio 23.0** (`dcc32`). Framework: `Dext.Testing`.

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

Full matrix and IDs: [`Docs/TEST_PLAN.md`](Docs/TEST_PLAN.md).

Interactive JetStream smoke: `Demo/JetStreamSmokeTest/` against `nats-server -js`.

## License

Apache 2.0 — see `LICENSE`.
