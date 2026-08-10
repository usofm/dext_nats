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
{  Manual end-to-end smoke test for core NATS message headers round-trip    }
{  (HPUB / HMSG; not JetStream). Interactive console against a local        }
{  nats-server that advertises headers support.                             }
{                                                                           }
{  REQUIRES a running NATS server, e.g.:                                    }
{                                                                           }
{      nats-server                                                          }
{                                                                           }
{  JetStream (-js) is optional and unused here. Modern nats-server reports  }
{  headers:true in INFO; this demo fails clearly if HeadersSupported is   }
{  False.                                                                   }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\HeadersE2E\HeadersE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\HeadersE2E.exe                                    }
{      Output\Win32\Debug\HeadersE2E.exe 127.0.0.1 4222                     }
{                                                                           }
{  Optional: DEXT_NATS_HOST / DEXT_NATS_PORT. Pass -no-wait to skip pause.  }
{                                                                           }
{***************************************************************************}
program HeadersE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas';

const
  { Override: HeadersE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4222;

  SUBJECT = 'dext.nats.headers.e2e';
  REQ_SUBJECT = 'dext.nats.headers.req.e2e';
  MESSAGE_COUNT = 3;
  RECEIVE_TIMEOUT_MS = 5000;
  REQUEST_TIMEOUT_MS = 3000;

type
  TExpectedHdrMsg = record
    Payload: string;
    TraceId: string;
    Order: string;
    Seen: Boolean;
  end;

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

function Utf8Bytes(const AText: string): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(AText);
end;

procedure RunHeadersE2E;
var
  host: string;
  port: Word;
  client: TDextNatsClient;
  sid, reqSid: Integer;
  expected: array[0..MESSAGE_COUNT - 1] of TExpectedHdrMsg;
  matched, received: Integer;
  lock: TCriticalSection;
  done: TEvent;
  i: Integer;
  headers: TNatsHeaders;
  waitOk: Boolean;
  snapMatched, snapReceived: Integer;
  reply: TNatsMsg;
  reqHeaders: TNatsHeaders;
  seenTrace: string;
begin
  host := ResolveHost;
  port := ResolvePort;

  Writeln('=== Dext.Nats message headers E2E ===');
  Writeln('NOTE: start a plain nats-server first (JetStream not required).');
  Writeln(Format('Target: %s:%d  Subject: %s  Count: %d',
    [host, port, SUBJECT, MESSAGE_COUNT]));
  Writeln;

  expected[0].Payload := 'order-alpha';
  expected[0].TraceId := 'trace-1001';
  expected[0].Order := 'A-1';
  expected[0].Seen := False;

  expected[1].Payload := 'order-bravo';
  expected[1].TraceId := 'trace-1002';
  expected[1].Order := 'B-2';
  expected[1].Seen := False;

  expected[2].Payload := 'order-charlie';
  expected[2].TraceId := 'trace-1003';
  expected[2].Order := 'C-3';
  expected[2].Seen := False;

  matched := 0;
  received := 0;
  seenTrace := '';
  lock := TCriticalSection.Create;
  done := TEvent.Create(nil, True, False, '');
  client := TDextNatsClient.Create;
  try
    try
      Writeln(Format('Connecting to %s:%d ...', [host, port]));
      client.Connect(host, port);
      PrintPass('Connected.');
      PrintInfo(Format('ServerId=%s Version=%s MaxPayload=%d HeadersSupported=%s',
        [client.ServerInfo.ServerId, client.ServerInfo.Version, client.ServerInfo.MaxPayload,
         BoolToStr(client.ServerInfo.HeadersSupported, True)]));
    except
      on E: Exception do
      begin
        PrintFail('Could not connect: ' + E.Message);
        Exit;
      end;
    end;

    if not client.ServerInfo.HeadersSupported then
    begin
      PrintFail('Server INFO reports HeadersSupported=False; need a modern nats-server with headers.');
      try
        client.Disconnect;
      except
      end;
      Exit;
    end;
    PrintPass('Server advertises headers support.');

    try
      sid := client.Subscribe(SUBJECT,
        procedure(const AMsg: TNatsMsg)
        var
          j: Integer;
          payload, traceId, order: string;
          ok: Boolean;
        begin
          payload := AMsg.AsString;
          traceId := AMsg.Headers.GetValue('X-Trace-Id');
          order := AMsg.Headers.GetValue('X-Order');
          lock.Enter;
          try
            Inc(received);
            Writeln(Format('  <- #%d subject=%s payload=%s X-Trace-Id=%s X-Order=%s',
              [received, AMsg.Subject, payload, traceId, order]));
            ok := False;
            for j := 0 to High(expected) do
            begin
              if expected[j].Seen then
                Continue;
              if (expected[j].Payload = payload) and
                 (expected[j].TraceId = traceId) and
                 (expected[j].Order = order) then
              begin
                expected[j].Seen := True;
                Inc(matched);
                ok := True;
                Break;
              end;
            end;
            if not ok then
              Writeln(Format('  !! unexpected message payload=%s headers Trace=%s Order=%s',
                [payload, traceId, order]));
            if matched >= MESSAGE_COUNT then
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

    try
      reqSid := client.Subscribe(REQ_SUBJECT,
        procedure(const AMsg: TNatsMsg)
        begin
          lock.Enter;
          try
            seenTrace := AMsg.Headers.GetValue('X-Trace-Id');
          finally
            lock.Leave;
          end;
          Writeln(Format('  <- request subject=%s body=%s X-Trace-Id=%s',
            [AMsg.Subject, AMsg.AsString, AMsg.Headers.GetValue('X-Trace-Id')]));
          if AMsg.HasReplyTo then
            client.Publish(AMsg.ReplyTo, 'hdr-echo:' + AMsg.AsString);
        end);
      PrintPass(Format('Subscribed request responder sid=%d on "%s".', [reqSid, REQ_SUBJECT]));
    except
      on E: Exception do
      begin
        PrintFail('Request-subject Subscribe failed: ' + E.Message);
        Exit;
      end;
    end;

    { Brief pause so SUBs are on the wire before HPUB (same process, one client). }
    Sleep(50);

    try
      for i := 0 to High(expected) do
      begin
        headers := nil;
        headers.Add('X-Trace-Id', expected[i].TraceId);
        headers.Add('X-Order', expected[i].Order);
        client.PublishWithHeaders(SUBJECT, Utf8Bytes(expected[i].Payload), headers);
        Writeln(Format('  -> #%d %s  X-Trace-Id=%s X-Order=%s',
          [i + 1, expected[i].Payload, expected[i].TraceId, expected[i].Order]));
      end;
      client.Flush(2000);
      PrintPass(Format('Published %d HPUB messages and flushed.', [MESSAGE_COUNT]));
    except
      on E: Exception do
      begin
        PrintFail('PublishWithHeaders/Flush failed: ' + E.Message);
        Exit;
      end;
    end;

    waitOk := done.WaitFor(RECEIVE_TIMEOUT_MS) = wrSignaled;
    lock.Enter;
    try
      snapMatched := matched;
      snapReceived := received;
    finally
      lock.Leave;
    end;

    if waitOk and (snapMatched = MESSAGE_COUNT) and (snapReceived = MESSAGE_COUNT) then
      PrintPass(Format('Received all %d messages with matching payload and headers.', [MESSAGE_COUNT]))
    else
      PrintFail(Format('Expected %d matching messages within %d ms; matched=%d received=%d.',
        [MESSAGE_COUNT, RECEIVE_TIMEOUT_MS, snapMatched, snapReceived]));

    try
      reqHeaders := nil;
      reqHeaders.Add('X-Trace-Id', 'req-trace-9');
      reply := client.RequestWithHeaders(REQ_SUBJECT, Utf8Bytes('ping-hdr'), reqHeaders,
        REQUEST_TIMEOUT_MS);
      if reply.AsString <> 'hdr-echo:ping-hdr' then
        PrintFail(Format('RequestWithHeaders reply: expected "hdr-echo:ping-hdr", got "%s"',
          [reply.AsString]))
      else
        PrintPass(Format('RequestWithHeaders reply="%s".', [reply.AsString]));

      lock.Enter;
      try
        if seenTrace = 'req-trace-9' then
          PrintPass('RequestWithHeaders delivered X-Trace-Id=req-trace-9 to responder.')
        else
          PrintFail(Format('RequestWithHeaders responder saw X-Trace-Id="%s", expected "req-trace-9".',
            [seenTrace]));
      finally
        lock.Leave;
      end;
    except
      on E: Exception do
        PrintFail(Format('RequestWithHeaders raised %s: %s', [E.ClassName, E.Message]));
    end;

    try
      client.Unsubscribe(sid);
      client.Unsubscribe(reqSid);
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
    RunHeadersE2E;

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
