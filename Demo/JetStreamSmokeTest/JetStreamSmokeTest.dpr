{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                     }
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
{  Manual smoke test for Dext.Net.Nats.JetStream. Not a Dext.Testing suite  }
{  - interactive console program against a local nats-server covering       }
{  stream admin, dedup publish, pull consumer Fetch, and Ack.               }
{                                                                           }
{  REQUIRES the target nats-server to be started with JetStream enabled,   }
{  e.g.:                                                                    }
{                                                                           }
{      nats-server -js                                                     }
{                                                                           }
{  A plain "nats-server" (no -js) will make every JetStream call in this   }
{  program fail with a clear "jetstream not enabled" API error - that is   }
{  expected in that case, not a bug in this demo or in the library.        }
{                                                                           }
{***************************************************************************}
program JetStreamSmokeTest;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Dext.Collections,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas',
  Dext.Net.Nats.JetStream in '..\..\Source\Dext.Net.Nats.JetStream.pas',
  Dext.Net.Nats.JetStream.Fetch in '..\..\Source\JetStream\Dext.Net.Nats.JetStream.Fetch.pas';

const
  { Change these to point at a different NATS server. }
  NATS_HOST = '127.0.0.1';
  NATS_PORT = 4222;

  STREAM_NAME = 'JS_SMOKE_TEST';
  STREAM_SUBJECT = 'js.smoke.test';
  CONSUMER_NAME = 'JS_SMOKE_PULL';
  DEDUP_MSG_ID = 'smoke-msg-1';
  MSG_PAYLOAD = 'Hello JetStream smoke test!';

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

procedure PrintStreamInfo(const AInfo: TNatsStreamInfo);
begin
  { UInt64 fields are cast to Int64 for Format('%d', ...): array-of-const has no
    dedicated UInt64 slot, and these counters never realistically approach 2^63
    in a smoke test. }
  PrintInfo(Format('Name=%s Messages=%d Bytes=%d FirstSeq=%d LastSeq=%d ConsumerCount=%d',
    [AInfo.Name, Int64(AInfo.Messages), Int64(AInfo.Bytes), Int64(AInfo.FirstSeq),
     Int64(AInfo.LastSeq), AInfo.ConsumerCount]));
end;

procedure PrintPublishAck(const AAck: TNatsPublishAck);
begin
  PrintInfo(Format('Stream=%s Sequence=%d Duplicate=%s Domain=%s',
    [AAck.Stream, Int64(AAck.Sequence), BoolToStr(AAck.Duplicate, True), AAck.Domain]));
end;

procedure RunSmokeTest;
var
  client: TDextNatsClient;
  js: TDextNatsJetStreamContext;
  config: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  info: TNatsStreamInfo;
  cinfo: TNatsConsumerInfo;
  ack1, ack2: TNatsPublishAck;
  msgs: IList<TNatsJsMsg>;
begin
  Writeln('=== Dext.Nats JetStream smoke test ===');
  Writeln('NOTE: this requires nats-server to be running WITH JetStream enabled (nats-server -js).');
  Writeln('      Every step below will fail with a "jetstream not enabled" API error otherwise.');
  Writeln;

  client := TDextNatsClient.Create;
  try
    { Step 1: connect. }
    try
      Writeln(Format('Connecting to %s:%d ...', [NATS_HOST, NATS_PORT]));
      client.Connect(NATS_HOST, NATS_PORT);
      PrintPass('Connected to NATS server.');
      if client.ServerInfo.Jetstream then
        PrintPass('Server INFO reports jetstream=true.')
      else
      begin
        PrintFail('Server INFO reports jetstream=false. Restart with: nats-server -js');
        Exit;
      end;
    except
      on E: Exception do
      begin
        PrintFail('Could not connect: ' + E.Message);
        Exit; { Nothing else can proceed without a connection. }
      end;
    end;

    { Step 2: wrap the client in a JetStream context. }
    js := TDextNatsJetStreamContext.Create(client);
    try
      { Step 3/4: clean up any leftover stream from a previous run. }
      try
        if js.StreamExists(STREAM_NAME) then
        begin
          PrintInfo(Format('Stream "%s" already exists from a previous run, deleting it first...', [STREAM_NAME]));
          js.DeleteStream(STREAM_NAME);
          PrintPass('Deleted pre-existing stream.');
        end
        else
          PrintInfo(Format('Stream "%s" does not exist yet - starting clean.', [STREAM_NAME]));
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('StreamExists/DeleteStream failed: %s (Code=%d, ErrCode=%d)',
            [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('StreamExists/DeleteStream failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 5: create the stream (memory storage avoids Windows file-lock flakiness on delete). }
      try
        config := TNatsStreamConfig.CreateDefault(STREAM_NAME, [STREAM_SUBJECT]);
        config.Storage := ssMemory;
        info := js.CreateStream(config);
        PrintPass(Format('Created stream "%s".', [STREAM_NAME]));
        PrintStreamInfo(info);
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('CreateStream failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('CreateStream failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 6: fetch it again as a round-trip sanity check. }
      try
        info := js.GetStreamInfo(STREAM_NAME);
        PrintPass('GetStreamInfo round-trip succeeded.');
        PrintStreamInfo(info);
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('GetStreamInfo failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('GetStreamInfo failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 7: first publish with a fixed Nats-Msg-Id - should not be a duplicate. }
      try
        ack1 := js.Publish(STREAM_SUBJECT, MSG_PAYLOAD, DEDUP_MSG_ID);
        PrintPass('First publish acknowledged.');
        PrintPublishAck(ack1);
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('First publish failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('First publish failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 8: same payload, same Nats-Msg-Id - the server should report it as a duplicate. }
      try
        ack2 := js.Publish(STREAM_SUBJECT, MSG_PAYLOAD, DEDUP_MSG_ID);
        PrintPass('Second publish (same Nats-Msg-Id) acknowledged.');
        PrintPublishAck(ack2);

        if ack2.Duplicate then
          PrintPass('DEDUP OK - server reported Duplicate=True on the second publish.')
        else
        begin
          PrintFail('DEDUP FAILED - Duplicate flag was False on second publish with same Nats-Msg-Id.');
        end;
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('Second publish failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('Second publish failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 9: message count should still be 1 - the duplicate was not actually stored. }
      try
        info := js.GetStreamInfo(STREAM_NAME);
        PrintStreamInfo(info);
        if info.Messages = 1 then
          PrintPass('Stream message count is 1, as expected (duplicate was not stored).')
        else
          PrintFail(Format('Expected stream message count to be 1, got %d.', [Int64(info.Messages)]));
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('Final GetStreamInfo failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('Final GetStreamInfo failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 10: create a durable pull consumer. }
      try
        consumerCfg := TNatsConsumerConfig.CreateDefault(CONSUMER_NAME, STREAM_SUBJECT);
        cinfo := js.CreateConsumer(STREAM_NAME, consumerCfg);
        PrintPass(Format('Created pull consumer "%s".', [CONSUMER_NAME]));
        PrintInfo(Format('Name=%s Stream=%s NumPending=%d',
          [cinfo.Name, cinfo.StreamName, Int64(cinfo.NumPending)]));
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('CreateConsumer failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('CreateConsumer failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 11: Fetch the stored message and Ack it. }
      try
        msgs := js.Fetch(STREAM_NAME, CONSUMER_NAME, 1, 3000);
        if msgs.Count = 1 then
        begin
          PrintPass(Format('Fetch returned 1 message: "%s".', [msgs[0].AsString]));
          PrintInfo(Format('Stream=%s Seq=%d ReplyTo=%s',
            [msgs[0].Stream, Int64(msgs[0].StreamSequence), msgs[0].ReplyTo]));
          js.Ack(msgs[0]);
          client.Flush(2000);
          PrintPass('Ack sent.');
        end
        else
          PrintFail(Format('Fetch expected 1 message, got %d.', [msgs.Count]));
      except
        on E: Exception do
        begin
          PrintFail('Fetch/Ack failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 12: GetConsumerInfo + DeleteConsumer. }
      try
        cinfo := js.GetConsumerInfo(STREAM_NAME, CONSUMER_NAME);
        PrintPass('GetConsumerInfo succeeded.');
        PrintInfo(Format('NumPending=%d NumAckPending=%d',
          [Int64(cinfo.NumPending), cinfo.NumAckPending]));
        if js.DeleteConsumer(STREAM_NAME, CONSUMER_NAME) then
          PrintPass(Format('Deleted consumer "%s".', [CONSUMER_NAME]))
        else
          PrintFail(Format('DeleteConsumer("%s") returned False.', [CONSUMER_NAME]));
      except
        on E: EDextNatsJetStreamError do
          PrintFail(Format('Consumer info/delete failed: %s (Code=%d, ErrCode=%d)',
            [E.Message, E.Code, E.ErrCode]));
        on E: Exception do
          PrintFail('Consumer info/delete failed: ' + E.Message);
      end;

      { Step 13: clean up stream. }
      try
        if js.DeleteStream(STREAM_NAME) then
          PrintPass(Format('Deleted stream "%s".', [STREAM_NAME]))
        else
          PrintFail(Format('DeleteStream("%s") returned False.', [STREAM_NAME]));
      except
        on E: EDextNatsJetStreamError do
          PrintFail(Format('DeleteStream failed: %s (Code=%d, ErrCode=%d)', [E.Message, E.Code, E.ErrCode]));
        on E: Exception do
          PrintFail('DeleteStream failed: ' + E.Message);
      end;
    finally
      js.Free;
    end;

    { Step 11: disconnect cleanly. }
    try
      client.Disconnect;
      PrintPass('Disconnected cleanly.');
    except
      on E: Exception do
        PrintFail('Disconnect failed: ' + E.Message);
    end;
  finally
    client.Free;
  end;
end;

begin
  SetConsoleCharSet;
  try
    RunSmokeTest;

    Writeln;
    if GFailureCount = 0 then
      Writeln('=== ALL STEPS PASSED ===')
    else
      Writeln(Format('=== %d STEP(S) FAILED - see [FAIL] lines above ===', [GFailureCount]));

    { ConsolePause (Dext.Utils) only actually blocks on ReadLn when running under the
      debugger; pass -no-wait on the command line to force-skip it in any context, and
      it is a no-op in non-interactive/CI runs where no console is attached. }
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
