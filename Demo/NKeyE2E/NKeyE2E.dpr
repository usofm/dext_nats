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
{  Manual end-to-end smoke test for NKey (bare seed) auth handshake.        }
{  Interactive console against a local nats-server started with the repo    }
{  fixtures under Tests/nkey/ (or the copy in this folder).                 }
{                                                                           }
{  REQUIRES an NKey-enabled NATS server on port 4224 (default), e.g. from   }
{  the repository root:                                                     }
{                                                                           }
{      nats-server -c Demo\NKeyE2E\nats-nkey.conf                           }
{                                                                           }
{  Equivalent (same public key / fixture seed as Tests/nkey):               }
{                                                                           }
{      nats-server -c Tests\nkey\nats-nkey.conf                             }
{                                                                           }
{  Client uses TDextNatsOptions.NKeySeed (docs-sample seed matching         }
{  Tests/nkey/user.nk). Optionally set CredentialsFile to that seed file    }
{  via DEXT_NATS_NKEY_SEED_FILE / DEXT_NATS_CREDS_FILE.                     }
{  OpenSSL libcrypto-3.dll must sit beside the exe (already under           }
{  Output\Win32\Debug for other demos/tests).                               }
{                                                                           }
{  A plain "nats-server" on 4222 (no authorization) will fail the CONNECT   }
{  with a clear message — start the NKey config above.                      }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\NKeyE2E\NKeyE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\NKeyE2E.exe                                       }
{      Output\Win32\Debug\NKeyE2E.exe 127.0.0.1 4224                        }
{                                                                           }
{  Optional: DEXT_NATS_NKEY_HOST / DEXT_NATS_NKEY_PORT /                    }
{  DEXT_NATS_NKEY_SEED (same as live NKey tests). Pass -no-wait to skip     }
{  pause.                                                                   }
{                                                                           }
{***************************************************************************}
program NKeyE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  System.IOUtils,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats.NKeys in '..\..\Source\Dext.Net.Nats.NKeys.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas';

const
  { Override: NKeyE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4224;

  { Same seed as Tests/nkey/user.nk and Tests/nkey/README.md }
  DEFAULT_SEED = 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';

  SUBJECT = 'dext.nats.nkey.e2e';
  PAYLOAD = 'nkey-e2e-ping';
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
    Result := GetEnvironmentVariable('DEXT_NATS_NKEY_HOST');
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
    text := GetEnvironmentVariable('DEXT_NATS_NKEY_PORT');
  if text = '' then
    Exit(DEFAULT_PORT);
  value := StrToIntDef(text, -1);
  if (value < 1) or (value > 65535) then
    raise EArgumentException.CreateFmt('Invalid port "%s" (expected 1..65535).', [text]);
  Result := Word(value);
end;

function ResolveSeedFilePath: string;
var
  candidates: array[0..3] of string;
  i: Integer;
begin
  Result := Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_SEED_FILE'));
  if Result = '' then
    Result := Trim(GetEnvironmentVariable('DEXT_NATS_CREDS_FILE'));
  if (Result <> '') and FileExists(Result) then
    Exit;

  { Walk from cwd and from the exe dir toward the repo Tests/nkey fixture. }
  candidates[0] := TPath.Combine(GetCurrentDir, 'Tests\nkey\user.nk');
  candidates[1] := TPath.Combine(GetCurrentDir, '..\..\..\Tests\nkey\user.nk');
  candidates[2] := TPath.Combine(ExtractFilePath(ParamStr(0)), '..\..\..\Tests\nkey\user.nk');
  candidates[3] := TPath.Combine(ExtractFilePath(ParamStr(0)), '..\..\Tests\nkey\user.nk');
  for i := Low(candidates) to High(candidates) do
  begin
    if FileExists(candidates[i]) then
      Exit(TPath.GetFullPath(candidates[i]));
  end;
  Result := '';
end;

function ResolveSeed(out ACredentialsFile: string): string;
var
  creds: TNatsCredentials;
  seedFile: string;
begin
  ACredentialsFile := '';
  Result := Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_SEED'));
  if Result <> '' then
    Exit;

  seedFile := ResolveSeedFilePath;
  if seedFile <> '' then
  begin
    ACredentialsFile := seedFile;
    creds := TNatsCredentials.FromFile(seedFile);
    Result := creds.Seed;
    if Result <> '' then
      Exit;
  end;

  Result := DEFAULT_SEED;
end;

procedure PrintSetupHint;
begin
  Writeln('NOTE: start an NKey nats-server first (plain 4222 is not enough):');
  Writeln('      nats-server -c Demo\NKeyE2E\nats-nkey.conf');
  Writeln('  or: nats-server -c Tests\nkey\nats-nkey.conf');
  Writeln('      OpenSSL libcrypto-3.dll must be beside NKeyE2E.exe (Output\Win32\Debug).');
end;

procedure RunNKeyE2E;
var
  host: string;
  port: Word;
  seed, credsFile: string;
  opts: TDextNatsOptions;
  client: TDextNatsClient;
  sid: Integer;
  received: string;
  done: TEvent;
  waitOk: Boolean;
begin
  host := ResolveHost;
  port := ResolvePort;
  seed := ResolveSeed(credsFile);

  Writeln('=== Dext.Nats NKey auth E2E ===');
  PrintSetupHint;
  Writeln(Format('Target: %s:%d  Subject: %s', [host, port, SUBJECT]));
  if credsFile <> '' then
    PrintInfo('CredentialsFile=' + credsFile)
  else
    PrintInfo('Using NKeySeed (fixture / DEXT_NATS_NKEY_SEED).');
  Writeln;

  if not NatsNKeyCryptoAvailable then
  begin
    PrintFail('OpenSSL libcrypto-3.dll not available for NKey signing.');
    PrintInfo('Place libcrypto-3.dll beside NKeyE2E.exe (Output\Win32\Debug).');
    Exit;
  end;
  PrintPass('OpenSSL libcrypto available for NKey signing.');

  done := TEvent.Create(nil, True, False, '');
  opts := TDextNatsOptions.CreateDefault;
  opts.NKeySeed := seed;
  if credsFile <> '' then
    opts.CredentialsFile := credsFile;
  client := TDextNatsClient.Create(opts);
  try
    try
      Writeln(Format('Connecting with NKeySeed to %s:%d ...', [host, port]));
      client.Connect(host, port);
    except
      on E: Exception do
      begin
        PrintFail('Could not connect / NKey auth: ' + E.Message);
        PrintInfo('Is nats-server running with Demo\NKeyE2E\nats-nkey.conf (port 4224)?');
        PrintInfo('Plain "nats-server" on 4222 will not satisfy this demo.');
        Exit;
      end;
    end;

    if not client.Connected then
    begin
      PrintFail('Connect returned but Connected=False.');
      Exit;
    end;
    PrintPass('Connected (NKey handshake completed).');
    { MaxPayload is Int64 — Format('%d') does not accept vtInt64 in open arrays. }
    PrintInfo(Format('ServerId=%s Version=%s AuthRequired=%s MaxPayload=%s',
      [client.ServerInfo.ServerId, client.ServerInfo.Version,
       BoolToStr(client.ServerInfo.AuthRequired, True),
       IntToStr(client.ServerInfo.MaxPayload)]));

    if not client.ServerInfo.AuthRequired then
      PrintInfo('Server INFO AuthRequired=False (unusual for NKey listener; continuing).')
    else
      PrintPass('Server advertises AuthRequired.');

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
      PrintPass(Format('Pub/sub round-trip with NKey auth ok (payload="%s").', [received]))
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
    RunNKeyE2E;

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
