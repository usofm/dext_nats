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
{  Manual end-to-end smoke test for core request/reply (not one-way pub/sub }
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
{      msbuild Demo\RequestReplyE2E\RequestReplyE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\RequestReplyE2E.exe                               }
{      Output\Win32\Debug\RequestReplyE2E.exe 127.0.0.1 4222                }
{                                                                           }
{  Optional: DEXT_NATS_HOST / DEXT_NATS_PORT. Pass -no-wait to skip pause.  }
{                                                                           }
{***************************************************************************}
program RequestReplyE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas';

const
  { Override: RequestReplyE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4222;

  ECHO_SUBJECT = 'dext.nats.req.echo';
  NO_RESPONDERS_SUBJECT = 'dext.nats.req.no.responders';
  REQUEST_TIMEOUT_MS = 3000;

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

procedure AssertEchoRequest(AClient: TDextNatsClient; const ABody: string);
var
  reply: TNatsMsg;
  expected: string;
begin
  expected := 'echo:' + ABody;
  try
    reply := AClient.Request(ECHO_SUBJECT, ABody, REQUEST_TIMEOUT_MS);
    if reply.AsString = expected then
      PrintPass(Format('Request("%s") -> "%s"', [ABody, reply.AsString]))
    else
      PrintFail(Format('Request("%s"): expected "%s", got "%s"',
        [ABody, expected, reply.AsString]));
  except
    on E: Exception do
      PrintFail(Format('Request("%s") raised %s: %s', [ABody, E.ClassName, E.Message]));
  end;
end;

procedure RunRequestReplyE2E;
var
  host: string;
  port: Word;
  client: TDextNatsClient;
  sid: Integer;
  bodies: TArray<string>;
  i: Integer;
begin
  host := ResolveHost;
  port := ResolvePort;

  Writeln('=== Dext.Nats request/reply E2E ===');
  Writeln('NOTE: start a plain nats-server first (JetStream not required).');
  Writeln(Format('Target: %s:%d  Echo subject: %s',
    [host, port, ECHO_SUBJECT]));
  Writeln;

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
      sid := client.Subscribe(ECHO_SUBJECT,
        procedure(const AMsg: TNatsMsg)
        begin
          if AMsg.HasReplyTo then
          begin
            Writeln(Format('  <- request subject=%s body=%s replyTo=%s',
              [AMsg.Subject, AMsg.AsString, AMsg.ReplyTo]));
            client.Publish(AMsg.ReplyTo, 'echo:' + AMsg.AsString);
          end
          else
            Writeln(Format('  !! request without ReplyTo subject=%s body=%s',
              [AMsg.Subject, AMsg.AsString]));
        end);
      PrintPass(Format('Subscribed responder sid=%d on "%s".', [sid, ECHO_SUBJECT]));
    except
      on E: Exception do
      begin
        PrintFail('Subscribe failed: ' + E.Message);
        Exit;
      end;
    end;

    { Brief pause so SUB is on the wire before Request (same process, one client). }
    Sleep(50);

    bodies := TArray<string>.Create('ping', 'hello', 'payload-3');
    for i := 0 to High(bodies) do
      AssertEchoRequest(client, bodies[i]);

    try
      client.Request(NO_RESPONDERS_SUBJECT, 'orphan', REQUEST_TIMEOUT_MS);
      PrintFail(Format('Expected EDextNatsNoResponders for "%s", but Request returned.',
        [NO_RESPONDERS_SUBJECT]));
    except
      on E: EDextNatsNoResponders do
        PrintPass(Format('No-responders subject raised EDextNatsNoResponders: %s', [E.Message]));
      on E: Exception do
        PrintFail(Format('No-responders expected EDextNatsNoResponders, got %s: %s',
          [E.ClassName, E.Message]));
    end;

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
  end;
end;

begin
  SetConsoleCharSet;
  try
    RunRequestReplyE2E;

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
