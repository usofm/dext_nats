# Dext.Nats

Native [NATS](https://nats.io) client for the [Dext Framework](https://github.com/) (Delphi 12 / Studio 23.0). Built on `Dext.Net.Tcp` with optional TLS via `Dext.Net.Security`, plus JetStream pull consumers.

## Units

| Unit | Role |
|------|------|
| `Source/Dext.Net.Nats.Protocol.pas` | Wire protocol (parser, INFO/CONNECT, PUB/SUB, headers) — no sockets |
| `Source/Dext.Net.Nats.pas` | `TDextNatsClient` — connect, pub/sub, request/reply, reconnect, TLS upgrade |
| `Source/Dext.Net.Nats.JetStream.pas` | `TDextNatsJetStreamContext` — streams, pull consumers, Fetch, Ack/Nak/Term |

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
| `DEXT_NATS_RUN_STRESS=1` | Run Explicit stress tests |

Full matrix and IDs: [`Docs/TEST_PLAN.md`](Docs/TEST_PLAN.md).

Interactive JetStream smoke: `Demo/JetStreamSmokeTest/` against `nats-server -js`.

## License

Apache 2.0 — see `LICENSE`.
