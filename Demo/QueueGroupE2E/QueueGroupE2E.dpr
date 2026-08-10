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
{  Manual end-to-end smoke test for core NATS queue-group load balancing   }
{  (not request/reply and not JetStream). Interactive console against a     }
{  local nats-server.                                                       }
{                                                                           }
{  REQUIRES a running NATS server, e.g.:                                    }
{                                                                           }
{      nats-server                                                          }
{                                                                           }
{  JetStream (-js) is optional and unused here.                             }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\QueueGroupE2E\QueueGroupE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\QueueGroupE2E.exe                                 }
{      Output\Win32\Debug\QueueGroupE2E.exe 127.0.0.1 4222                  }
{                                                                           }
{  Optional: DEXT_NATS_HOST / DEXT_NATS_PORT. Pass -no-wait to skip pause.  }
{                                                                           }
{***************************************************************************}
program QueueGroupE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas';

const
  { Override: QueueGroupE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4222;

  SUBJECT = 'dext.nats.queue.e2e';
  QUEUE = 'workers';
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

procedure RunQueueGroupE2E;
var
  host: string;
  port: Word;
  client: TDextNatsClient;
  sid1, sid2: Integer;
  received1, received2, total: Integer;
  lock: TCriticalSection;
  done: TEvent;
  i: Integer;
  payload: string;
  waitOk: Boolean;
  snap1, snap2, snapTotal: Integer;
begin
  host := ResolveHost;
  port := ResolvePort;

  Writeln('=== Dext.Nats queue-group load-balancing E2E ===');
  Writeln('NOTE: start a plain nats-server first (JetStream not required).');
  Writeln(Format('Target: %s:%d  Subject: %s  Queue: %s  Count: %d',
    [host, port, SUBJECT, QUEUE, MESSAGE_COUNT]));
  Writeln;

  received1 := 0;
  received2 := 0;
  total := 0;
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
      sid1 := client.Subscribe(SUBJECT,
        procedure(const AMsg: TNatsMsg)
        var
          n: Integer;
        begin
          lock.Enter;
          try
            Inc(received1);
            Inc(total);
            n := total;
            Writeln(Format('  <- worker1 #%d (w1=%d) subject=%s payload=%s',
              [n, received1, AMsg.Subject, AMsg.AsString]));
            if n >= MESSAGE_COUNT then
              done.SetEvent;
          finally
            lock.Leave;
          end;
        end, QUEUE);
      sid2 := client.Subscribe(SUBJECT,
        procedure(const AMsg: TNatsMsg)
        var
          n: Integer;
        begin
          lock.Enter;
          try
            Inc(received2);
            Inc(total);
            n := total;
            Writeln(Format('  <- worker2 #%d (w2=%d) subject=%s payload=%s',
              [n, received2, AMsg.Subject, AMsg.AsString]));
            if n >= MESSAGE_COUNT then
              done.SetEvent;
          finally
            lock.Leave;
          end;
        end, QUEUE);
      PrintPass(Format('Subscribed two queue workers sid=%d,%d on "%s" queue="%s".',
        [sid1, sid2, SUBJECT, QUEUE]));
    except
      on E: Exception do
      begin
        PrintFail('Subscribe failed: ' + E.Message);
        Exit;
      end;
    end;

    { Brief pause so both SUBs are on the wire before PUB (same process, one client). }
    Sleep(50);

    try
      for i := 1 to MESSAGE_COUNT do
      begin
        payload := Format('msg-%d', [i]);
        client.Publish(SUBJECT, payload);
        Writeln(Format('  -> #%d %s', [i, payload]));
      end;
      client.Flush(2000);
      PrintPass(Format('Published %d messages and flushed.', [MESSAGE_COUNT]));
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
      snap1 := received1;
      snap2 := received2;
      snapTotal := total;
    finally
      lock.Leave;
    end;

    PrintInfo(Format('worker1=%d  worker2=%d  total=%d', [snap1, snap2, snapTotal]));

    if waitOk and (snapTotal = MESSAGE_COUNT) then
      PrintPass(Format('Aggregate delivery exactly once: %d messages.', [MESSAGE_COUNT]))
    else
      PrintFail(Format('Expected aggregate count %d within %d ms, got %d.',
        [MESSAGE_COUNT, RECEIVE_TIMEOUT_MS, snapTotal]));

    if (snap1 > 0) and (snap2 > 0) then
      PrintPass(Format('Load balancing: both workers received messages (w1=%d, w2=%d).',
        [snap1, snap2]))
    else
      PrintFail(Format('Expected both workers to receive >= 1 message; got w1=%d w2=%d.',
        [snap1, snap2]));

    try
      client.Unsubscribe(sid1);
      client.Unsubscribe(sid2);
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
    RunQueueGroupE2E;

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
