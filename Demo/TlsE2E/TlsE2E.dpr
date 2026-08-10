{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           A native NATS client library for the Dext Framework            }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License"); }
{           you may not use this file except in compliance with the License.}
{           You may obtain a copy of the License at                         }
{                                                                           }
{               http://www.apache.org/licenses/LICENSE-2.0                  }
{                                                                           }
{           Unless required by applicable law or agreed to in writing,      }
{           software distributed under the License is distributed on an     }
{           "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,    }
{           either express or implied. See the License for the specific     }
{           language governing permissions and limitations under the        }
{           License.                                                        }
{                                                                           }
{***************************************************************************}
{                                                                           }
{  Manual end-to-end smoke test for TLS upgrade after cleartext INFO (or a  }
{  TLS-required listener). Interactive console against a local nats-server  }
{  started with the repo self-signed fixtures under Tests/tls/.             }
{                                                                           }
{  REQUIRES a TLS-enabled NATS server on port 4223 (default), e.g. from the }
{  repository root:                                                         }
{                                                                           }
{      nats-server -c Demo\TlsE2E\nats-tls.conf                             }
{                                                                           }
{  That config reuses Tests/tls/server-cert.pem and server-key.pem.         }
{  Equivalent (cwd must be Tests\tls so relative cert paths resolve):       }
{                                                                           }
{      cd Tests\tls                                                         }
{      nats-server -c nats-tls.conf                                         }
{                                                                           }
{  Client uses Options.TLS.Enabled with VerifyServerCertificate=False       }
{  (self-signed fixture). OpenSSL libssl-3.dll / libcrypto-3.dll must sit   }
{  beside the exe (already under Output\Win32\Debug for other demos/tests). }
{                                                                           }
{  A plain "nats-server" on 4222 (no TLS) will fail the connect/handshake   }
{  with a clear message — start the TLS config above, not cleartext.        }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\TlsE2E\TlsE2E.dproj /p:Config=Debug /p:Platform=Win32   }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\TlsE2E.exe                                        }
{      Output\Win32\Debug\TlsE2E.exe 127.0.0.1 4223                         }
{                                                                           }
{  Optional: DEXT_NATS_TLS_HOST / DEXT_NATS_TLS_PORT (same as live TLS      }
{  tests). Pass -no-wait to skip pause.                                     }
{                                                                           }
{***************************************************************************}
program TlsE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  Dext.Utils,
  Dext.Net.Security,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas';

const
  { Override: TlsE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4223;

  SUBJECT = 'dext.nats.tls.e2e';
  PAYLOAD = 'tls-e2e-ping';
  RECEIVE_TIMEOUT_MS = 5000;

var
  GFailureCount: Integer = 0;

procedure PrintPass(const AMessage: string);
begin
  Writeln('[PASS] ' + AMessage);
end;

procedure PrintFail(const AMessage: string);
begin
  Inc(GFailureCount);
  Writeln('[FAIL] ' + AMessage);
end;

procedure PrintInfo(const AMessage: string);
begin
  Writeln('       ' + AMessage);
end;

{ Positional args only — skip switches such as ConsolePause's -no-wait. }
function PositionalArg(AIndex: Integer): string;
var
  i, n: Integer;
  arg: string;
begin
  Result := '';
  n := 0;
  for i := 1 to ParamCount do
  begin
    arg := ParamStr(i);
    if (arg <> '') and (arg[1] = '-') then
      Continue;
    Inc(n);
    if n = AIndex then
      Exit(arg);
  end;
end;

function ResolveHost: string;
begin
  Result := PositionalArg(1);
  if Result = '' then
    Result := GetEnvironmentVariable('DEXT_NATS_TLS_HOST');
  if Result = '' then
    Result := DEFAULT_HOST;
end;

function ResolvePort: Word;
var
  text: string;
  value: Integer;
begin
  text := PositionalArg(2);
  if text = '' then
    text := GetEnvironmentVariable('DEXT_NATS_TLS_PORT');
  if text = '' then
    Exit(DEFAULT_PORT);
  value := StrToIntDef(text, -1);
  if (value < 1) or (value > 65535) then
    raise EArgumentException.CreateFmt('Invalid port "%s" (expected 1..65535).', [text]);
  Result := Word(value);
end;

procedure PrintSetupHint;
begin
  Writeln('NOTE: start a TLS nats-server first (cleartext 4222 is not enough):');
  Writeln('      nats-server -c Demo\TlsE2E\nats-tls.conf');
  Writeln('  or: cd Tests\tls && nats-server -c nats-tls.conf');
  Writeln('      OpenSSL DLLs must be beside TlsE2E.exe (Output\Win32\Debug).');
end;

procedure RunTlsE2E;
var
  host: string;
  port: Word;
  opts: TDextNatsOptions;
  client: TDextNatsClient;
  sid: Integer;
  received: string;
  done: TEvent;
  waitOk: Boolean;
begin
  host := ResolveHost;
  port := ResolvePort;

  Writeln('=== Dext.Nats TLS upgrade E2E ===');
  PrintSetupHint;
  Writeln(Format('Target: %s:%d  Subject: %s', [host, port, SUBJECT]));
  Writeln;

  done := TEvent.Create(nil, True, False, '');
  opts := TDextNatsOptions.CreateDefault;
  opts.TLS := TDextTLSOptions.DefaultClient;
  opts.TLS.Enabled := True;
  opts.TLS.VerifyServerCertificate := False; { Tests/tls self-signed fixture }
  client := TDextNatsClient.Create(opts);
  try
    try
      Writeln(Format('Connecting with TLS.Enabled to %s:%d ...', [host, port]));
      client.Connect(host, port);
    except
      on E: Exception do
      begin
        PrintFail('Could not connect / TLS handshake: ' + E.Message);
        PrintInfo('Is nats-server running with Demo\TlsE2E\nats-tls.conf (port 4223)?');
        PrintInfo('Plain "nats-server" on 4222 will not satisfy this demo.');
        Exit;
      end;
    end;

    if not client.Connected then
    begin
      PrintFail('Connect returned but Connected=False.');
      Exit;
    end;
    PrintPass('Connected (TLS handshake completed).');
    { MaxPayload is Int64 — Format('%d') does not accept vtInt64 in open arrays. }
    PrintInfo(Format('ServerId=%s Version=%s MaxPayload=%s TlsRequired=%s Jetstream=%s',
      [client.ServerInfo.ServerId, client.ServerInfo.Version, IntToStr(client.ServerInfo.MaxPayload),
       BoolToStr(client.ServerInfo.TlsRequired, True),
       BoolToStr(client.ServerInfo.Jetstream, True)]));

    try
      sid := client.Subscribe(SUBJECT,
        procedure(const AMsg: TNatsMsg)
        begin
          received := AMsg.AsString;
          Writeln(Format('  <- subject=%s payload=%s', [AMsg.Subject, received]));
          done.SetEvent;
        end);
      PrintPass(Format('Subscribed sid=%d on "%s".', [sid, SUBJECT]));
    except
      on E: Exception do
      begin
        PrintFail('Subscribe failed: ' + E.Message);
        Exit;
      end;
    end;

    Sleep(50);

    try
      client.Publish(SUBJECT, PAYLOAD);
      client.Flush(2000);
      PrintPass(Format('Published "%s" and flushed.', [PAYLOAD]));
    except
      on E: Exception do
      begin
        PrintFail('Publish/Flush failed: ' + E.Message);
        Exit;
      end;
    end;

    waitOk := done.WaitFor(RECEIVE_TIMEOUT_MS) = wrSignaled;
    if waitOk and (received = PAYLOAD) then
      PrintPass(Format('Pub/sub round-trip over TLS ok (payload="%s").', [received]))
    else if waitOk then
      PrintFail(Format('Unexpected payload "%s" (expected "%s").', [received, PAYLOAD]))
    else
      PrintFail(Format('No message on "%s" within %d ms.', [SUBJECT, RECEIVE_TIMEOUT_MS]));

    try
      client.Unsubscribe(sid);
      client.Disconnect;
      PrintPass('Unsubscribed and disconnected.');
    except
      on E: Exception do
        PrintFail('Cleanup failed: ' + E.Message);
    end;
  finally
    client.Free;
    done.Free;
  end;
end;

begin
  SetConsoleCharSet;
  try
    RunTlsE2E;

    Writeln;
    if GFailureCount = 0 then
      Writeln('=== ALL STEPS PASSED ===')
    else
      Writeln(Format('=== %d STEP(S) FAILED - see [FAIL] lines above ===', [GFailureCount]));

    ConsolePause;
    ExitCode := GFailureCount;
  except
    on E: Exception do
    begin
      Writeln('FATAL ERROR: ' + E.ClassName + ': ' + E.Message);
      ConsolePause;
      ExitCode := 1;
    end;
  end;
end.
