{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           A native NATS client library for the Dext Framework            }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License");}
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
{  Manual end-to-end smoke test for core one-way pub/sub (not request/reply }
{  and not JetStream). Interactive console against a local nats-server.     }
{                                                                           }
{  REQUIRES a running NATS server, e.g.:                                    }
{                                                                           }
{      nats-server                                                          }
{                                                                           }
{  JetStream (-js) is optional and unused here.                             }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\PubSubE2E\PubSubE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\PubSubE2E.exe                                     }
{      Output\Win32\Debug\PubSubE2E.exe 127.0.0.1 4222                      }
{                                                                           }
{  Optional: DEXT_NATS_HOST / DEXT_NATS_PORT. Pass -no-wait to skip pause.  }
{                                                                           }
{***************************************************************************}
program PubSubE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas';

const
  { Override: PubSubE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4222;

  SUBJECT = 'dext.nats.pubsub.e2e';
  MESSAGE_COUNT = 20;
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
    Result := GetEnvironmentVariable('DEXT_NATS_HOST');
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
    text := GetEnvironmentVariable('DEXT_NATS_PORT');
  if text = '' then
    Exit(DEFAULT_PORT);
  value := StrToIntDef(text, -1);
  if (value < 1) or (value > 65535) then
    raise EArgumentException.CreateFmt('Invalid port "%s" (expected 1..65535).', [text]);
  Result := Word(value);
end;

procedure RunPubSubE2E;
var
  host: string;
  port: Word;
  client: TDextNatsClient;
  sid: Integer;
  received: Integer;
  lock: TCriticalSection;
  done: TEvent;
  i: Integer;
  payload: string;
  waitOk: Boolean;
  snapshot: Integer;
begin
  host := ResolveHost;
  port := ResolvePort;

  Writeln('=== Dext.Nats one-way pub/sub E2E ===');
  Writeln('NOTE: start a plain nats-server first (JetStream not required).');
  Writeln(Format('Target: %s:%d  Subject: %s  Count: %d',
    [host, port, SUBJECT, MESSAGE_COUNT]));
  Writeln;

  received := 0;
  lock := TCriticalSection.Create;
  done := TEvent.Create(nil, True, False, '');
  client := TDextNatsClient.Create;
  try
    try
      Writeln(Format('Connecting to %s:%d ...', [host, port]));
      client.Connect(host, port);
      PrintPass('Connected.');
      PrintInfo(Format('ServerId=%s Version=%s MaxPayload=%d',
        [client.ServerInfo.ServerId, client.ServerInfo.Version, client.ServerInfo.MaxPayload]));
    except
      on E: Exception do
      begin
        PrintFail('Could not connect: ' + E.Message);
        Exit;
      end;
    end;

    try
      sid := client.Subscribe(SUBJECT,
        procedure(const AMsg: TNatsMsg)
        var
          n: Integer;
        begin
          lock.Enter;
          try
            Inc(received);
            n := received;
            Writeln(Format('  <- #%d subject=%s payload=%s',
              [n, AMsg.Subject, AMsg.AsString]));
            if n >= MESSAGE_COUNT then
              done.SetEvent;
          finally
            lock.Leave;
          end;
        end);
      PrintPass(Format('Subscribed sid=%d on "%s".', [sid, SUBJECT]));
    except
      on E: Exception do
      begin
        PrintFail('Subscribe failed: ' + E.Message);
        Exit;
      end;
    end;

    { Brief pause so SUB is on the wire before PUB (same process, one client). }
    Sleep(50);

    try
      for i := 1 to MESSAGE_COUNT do
      begin
        payload := Format('msg-%d', [i]);
        client.Publish(SUBJECT, payload);
        Writeln(Format('  -> #%d %s', [i, payload]));
      end;
      client.Flush(2000);
      PrintPass(Format('Published %d one-way messages and flushed.', [MESSAGE_COUNT]));
    except
      on E: Exception do
      begin
        PrintFail('Publish/Flush failed: ' + E.Message);
        Exit;
      end;
    end;

    waitOk := done.WaitFor(RECEIVE_TIMEOUT_MS) = wrSignaled;
    lock.Enter;
    try
      snapshot := received;
    finally
      lock.Leave;
    end;

    if waitOk and (snapshot = MESSAGE_COUNT) then
      PrintPass(Format('Received all %d echoed messages.', [MESSAGE_COUNT]))
    else
      PrintFail(Format('Expected %d messages within %d ms, got %d.',
        [MESSAGE_COUNT, RECEIVE_TIMEOUT_MS, snapshot]));

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
    lock.Free;
  end;
end;

begin
  SetConsoleCharSet;
  try
    RunPubSubE2E;

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
