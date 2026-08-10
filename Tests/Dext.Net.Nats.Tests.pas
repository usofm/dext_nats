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
unit Dext.Net.Nats.Tests;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Collections,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Security,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

type
  [TestFixture('NATS Protocol Parser')]
  TDextNatsProtocolTests = class
  public
    [Test]
    procedure Parser_ShouldDecodeInfoFrame;
    [Test]
    procedure Parser_ShouldDecodeInfoTlsRequired;
    [Test]
    procedure Parser_ShouldDecodeMsgFrame;
    [Test]
    procedure Parser_ShouldDecodeHMsgWithStatusAndHeaders;
    [Test]
    procedure Parser_ShouldDecodePing;
    [Test]
    procedure Encode_ShouldBuildPubAndSubFrames;
    [Test]
    procedure ConnectOptions_ShouldDefaultNoResponders;
    [Test]
    procedure ClientOptions_ShouldDefaultTlsDisabled;
    [Test]
    procedure ConsumerConfig_ShouldSerializeDefaults;
    [Test]
    procedure JsMsg_ShouldParseAckSubjectMetadata;
  end;

  [TestFixture('NATS Client Integration (localhost:4222)')]
  TDextNatsIntegrationTests = class
  private
    FClient: TDextNatsClient;
    procedure EnsureServerOrFail;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure Connect_ShouldHandshake;
    [Test]
    procedure PublishSubscribe_ShouldDeliverPayload;
    [Test]
    procedure RequestReply_ShouldRoundTrip;
    [Test]
    procedure Request_NoResponders_ShouldRaise;
  end;

  [TestFixture('NATS JetStream Integration (requires nats-server -js)')]
  TDextNatsJetStreamTests = class
  private
    FClient: TDextNatsClient;
    FJs: TDextNatsJetStreamContext;
    procedure EnsureJetStreamOrFail;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure Consumer_FetchAndAck_ShouldRoundTrip;
  end;

  [TestFixture('NATS TLS Integration')]
  TDextNatsTlsIntegrationTests = class
  private
    FClient: TDextNatsClient;
    function TryGetTlsEndpoint(out AHost: string; out APort: Word): Boolean;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test]
    [Ignore('Requires a TLS NATS server; set DEXT_NATS_TLS_HOST and DEXT_NATS_TLS_PORT')]
    procedure Connect_Tls_ShouldHandshakeWhenConfigured;
  end;

implementation

function BytesOfUtf8(const S: string): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(S);
end;

function Utf8OfBytes(const B: TBytes): string;
begin
  if Length(B) = 0 then
    Result := ''
  else
    Result := TEncoding.UTF8.GetString(B);
end;

procedure FeedParser(Parser: TDextNatsFrameParser; const S: string);
var
  data: TBytes;
begin
  data := BytesOfUtf8(S);
  Parser.Append(data, Length(data));
end;

{ TDextNatsProtocolTests }

procedure TDextNatsProtocolTests.Parser_ShouldDecodeInfoFrame;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
  info: TNatsServerInfo;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser,
      'INFO {"server_id":"NABC","version":"2.10.0","proto":1,"max_payload":1048576,' +
      '"headers":true,"connect_urls":["127.0.0.1:4223","nats://127.0.0.1:4224"]}' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfInfo));
    info := TNatsServerInfo.Parse(frame.InfoJson);
    Should(info.ServerId).Be('NABC');
    Should(info.Version).Be('2.10.0');
    Should(info.MaxPayload).Be(1048576);
    Should(info.HeadersSupported).BeTrue;
    Should(info.TlsRequired).BeFalse;
    Should(Length(info.ConnectUrls)).Be(2);
    Should(info.ConnectUrls[0]).Be('127.0.0.1:4223');
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeInfoTlsRequired;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
  info: TNatsServerInfo;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser,
      'INFO {"server_id":"NTLS","version":"2.10.0","proto":1,"tls_required":true,' +
      '"max_payload":1048576}' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    info := TNatsServerInfo.Parse(frame.InfoJson);
    Should(info.TlsRequired).BeTrue;
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeMsgFrame;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, 'MSG foo.bar 7 5' + #13#10 + 'hello' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfMsg));
    Should(frame.Subject).Be('foo.bar');
    Should(frame.Sid).Be(7);
    Should(Utf8OfBytes(frame.Payload)).Be('hello');
    Should(frame.StatusCode).Be(0);
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeHMsgWithStatusAndHeaders;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
  headerBlock: string;
  total: Integer;
begin
  parser := TDextNatsFrameParser.Create;
  try
    headerBlock := 'NATS/1.0 503' + #13#10 + 'Nats-Msg-Id: abc' + #13#10 + #13#10;
    total := Length(BytesOfUtf8(headerBlock));
    FeedParser(parser,
      Format('HMSG inbox.1 3 %d %d', [total, total]) + #13#10 + headerBlock + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfHMsg));
    Should(frame.Subject).Be('inbox.1');
    Should(frame.Sid).Be(3);
    Should(frame.StatusCode).Be(503);
    Should(frame.Headers.GetValue('Nats-Msg-Id')).Be('abc');
    Should(Length(frame.Payload)).Be(0);
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodePing;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, 'PING' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfPing));
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Encode_ShouldBuildPubAndSubFrames;
var
  pubBytes, subBytes: TBytes;
begin
  pubBytes := NatsEncodePub('orders', '', BytesOfUtf8('x'));
  Should(Utf8OfBytes(pubBytes)).Be('PUB orders 1' + #13#10 + 'x' + #13#10);

  subBytes := NatsEncodeSub('orders.*', 'workers', 42);
  Should(Utf8OfBytes(subBytes)).Be('SUB orders.* workers 42' + #13#10);
end;

procedure TDextNatsProtocolTests.ConnectOptions_ShouldDefaultNoResponders;
var
  opts: TNatsConnectOptions;
  json: string;
begin
  opts := TNatsConnectOptions.CreateDefault;
  Should(opts.NoResponders).BeTrue;
  Should(opts.Headers).BeTrue;
  json := opts.ToJson;
  Should(json.Contains('"no_responders":true')).BeTrue;
end;

procedure TDextNatsProtocolTests.ClientOptions_ShouldDefaultTlsDisabled;
var
  opts: TDextNatsOptions;
begin
  opts := TDextNatsOptions.CreateDefault;
  Should(opts.TLS.Enabled).BeFalse;
  Should(Ord(opts.TLS.Mode)).Be(Ord(tlsmClient));
end;

procedure TDextNatsProtocolTests.ConsumerConfig_ShouldSerializeDefaults;
var
  cfg: TNatsConsumerConfig;
  json: string;
begin
  cfg := TNatsConsumerConfig.CreateDefault('ORDERS', 'orders.*');
  Should(Ord(cfg.AckPolicy)).Be(Ord(apExplicit));
  Should(Ord(cfg.DeliverPolicy)).Be(Ord(dpAll));
  json := cfg.ToJson;
  Should(json.Contains('"durable_name":"ORDERS"')).BeTrue;
  Should(json.Contains('"filter_subject":"orders.*"')).BeTrue;
  Should(json.Contains('"ack_policy":"explicit"')).BeTrue;
  Should(json.Contains('"deliver_policy":"all"')).BeTrue;
end;

procedure TDextNatsProtocolTests.JsMsg_ShouldParseAckSubjectMetadata;
var
  raw: TNatsMsg;
  js: TNatsJsMsg;
begin
  raw.Subject := 'orders.1';
  raw.ReplyTo := '$JS.ACK.ORDERS.pull1.1.42.7.1700000000.3';
  raw.Payload := BytesOfUtf8('payload');
  raw.Headers := nil;
  raw.Sid := 1;
  raw.StatusCode := 0;

  js := TNatsJsMsg.FromNatsMsg(raw);
  Should(js.Stream).Be('ORDERS');
  Should(js.Consumer).Be('pull1');
  Should(Int64(js.StreamSequence)).Be(42);
  Should(Int64(js.ConsumerSequence)).Be(7);
  Should(js.Timestamp).Be(1700000000);
  Should(js.NumPending).Be(3);
  Should(js.AsString).Be('payload');
end;

{ TDextNatsIntegrationTests }

procedure TDextNatsIntegrationTests.EnsureServerOrFail;
begin
  try
    FClient.Connect('127.0.0.1', NATS_DEFAULT_PORT);
  except
    on E: Exception do
      raise EDextNatsException.Create(
        'NATS server not reachable at 127.0.0.1:4222 (' + E.Message +
        '). Start nats-server before running integration tests.');
  end;
end;

procedure TDextNatsIntegrationTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
end;

procedure TDextNatsIntegrationTests.TearDown;
begin
  if Assigned(FClient) then
  begin
    try
      FClient.Disconnect;
    except
    end;
    FreeAndNil(FClient);
  end;
end;

procedure TDextNatsIntegrationTests.Connect_ShouldHandshake;
begin
  EnsureServerOrFail;
  Should(FClient.Connected).BeTrue;
  Should(FClient.ServerInfo.ServerId).NotBeEmpty;
end;

procedure TDextNatsIntegrationTests.PublishSubscribe_ShouldDeliverPayload;
var
  received: TEvent;
  payload: string;
  subject: string;
begin
  EnsureServerOrFail;
  subject := 'dext.nats.test.pubsub.' + FormatDateTime('hhnnsszzz', Now);
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        payload := AMsg.AsString;
        received.SetEvent;
      end);

    FClient.Publish(subject, 'hello-nats');

    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(payload).Be('hello-nats');
  finally
    received.Free;
  end;
end;

procedure TDextNatsIntegrationTests.RequestReply_ShouldRoundTrip;
var
  serviceSubject: string;
  reply: TNatsMsg;
begin
  EnsureServerOrFail;
  serviceSubject := 'dext.nats.test.req.' + FormatDateTime('hhnnsszzz', Now);

  FClient.Subscribe(serviceSubject,
    procedure(const AMsg: TNatsMsg)
    begin
      if AMsg.HasReplyTo then
        FClient.Publish(AMsg.ReplyTo, 'pong:' + AMsg.AsString);
    end);

  reply := FClient.Request(serviceSubject, 'ping', 3000);
  Should(reply.AsString).Be('pong:ping');
  Should(reply.IsNoResponders).BeFalse;
end;

procedure TDextNatsIntegrationTests.Request_NoResponders_ShouldRaise;
var
  subject: string;
begin
  EnsureServerOrFail;
  subject := 'dext.nats.test.no.responders.' + FormatDateTime('hhnnsszzz', Now);
  Should(
    procedure
    begin
      FClient.Request(subject, 'anything', 2000);
    end).Throw(EDextNatsNoResponders);
end;

{ TDextNatsJetStreamTests }

procedure TDextNatsJetStreamTests.EnsureJetStreamOrFail;
begin
  try
    FClient.Connect('127.0.0.1', NATS_DEFAULT_PORT);
  except
    on E: Exception do
      raise EDextNatsException.Create(
        'NATS server not reachable at 127.0.0.1:4222 (' + E.Message +
        '). Start nats-server before running JetStream tests.');
  end;

  if not FClient.ServerInfo.Jetstream then
    raise EDextNatsException.Create(
      'NATS server at 127.0.0.1:4222 has JetStream disabled (INFO jetstream!=true). ' +
      'Start with: nats-server -js');

  FJs := TDextNatsJetStreamContext.Create(FClient);
end;

procedure TDextNatsJetStreamTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
  FJs := nil;
end;

procedure TDextNatsJetStreamTests.TearDown;
begin
  FreeAndNil(FJs);
  if Assigned(FClient) then
  begin
    try
      FClient.Disconnect;
    except
    end;
    FreeAndNil(FClient);
  end;
end;

procedure TDextNatsJetStreamTests.Consumer_FetchAndAck_ShouldRoundTrip;
const
  STREAM = 'DEXT_JS_TEST_STREAM';
  CONSUMER = 'DEXT_JS_TEST_PULL';
  SUBJECT = 'dext.js.test.orders';
var
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  info: TNatsConsumerInfo;
  msgs: IList<TNatsJsMsg>;
  ack: TNatsPublishAck;
begin
  EnsureJetStreamOrFail;

  if FJs.StreamExists(STREAM) then
    FJs.DeleteStream(STREAM);

  streamCfg := TNatsStreamConfig.CreateDefault(STREAM, [SUBJECT]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);

  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(CONSUMER, SUBJECT);
    info := FJs.CreateConsumer(STREAM, consumerCfg);
    Should(info.Name).Be(CONSUMER);
    Should(info.StreamName).Be(STREAM);

    info := FJs.GetConsumerInfo(STREAM, CONSUMER);
    Should(info.DurableName).Be(CONSUMER);

    ack := FJs.Publish(SUBJECT, 'order-1', 'js-test-msg-1');
    Should(ack.Stream).Be(STREAM);
    Should(ack.Duplicate).BeFalse;

    msgs := FJs.Fetch(STREAM, CONSUMER, 1, 3000);
    Should(msgs.Count).Be(1);
    Should(msgs[0].AsString).Be('order-1');
    Should(msgs[0].Stream).Be(STREAM);
    Should(msgs[0].ReplyTo.StartsWith('$JS.ACK.')).BeTrue;

    FJs.Ack(msgs[0]);
    FClient.Flush(2000);

    msgs := FJs.Fetch(STREAM, CONSUMER, 1, 500);
    Should(msgs.Count).Be(0);

    Should(FJs.DeleteConsumer(STREAM, CONSUMER)).BeTrue;
  finally
    FJs.DeleteStream(STREAM);
  end;
end;

{ TDextNatsTlsIntegrationTests }

function TDextNatsTlsIntegrationTests.TryGetTlsEndpoint(out AHost: string; out APort: Word): Boolean;
var
  portStr: string;
begin
  AHost := Trim(GetEnvironmentVariable('DEXT_NATS_TLS_HOST'));
  portStr := Trim(GetEnvironmentVariable('DEXT_NATS_TLS_PORT'));
  if AHost = '' then
    AHost := '127.0.0.1';
  APort := Word(StrToIntDef(portStr, 0));
  Result := APort > 0;
end;

procedure TDextNatsTlsIntegrationTests.SetUp;
var
  opts: TDextNatsOptions;
begin
  opts := TDextNatsOptions.CreateDefault;
  opts.TLS := TDextTLSOptions.DefaultClient;
  opts.TLS.VerifyServerCertificate := False; // local/dev certs often self-signed
  FClient := TDextNatsClient.Create(opts);
end;

procedure TDextNatsTlsIntegrationTests.TearDown;
begin
  if Assigned(FClient) then
  begin
    try
      FClient.Disconnect;
    except
    end;
    FreeAndNil(FClient);
  end;
end;

procedure TDextNatsTlsIntegrationTests.Connect_Tls_ShouldHandshakeWhenConfigured;
var
  host: string;
  port: Word;
begin
  // Ignored by default; remove [Ignore] and set DEXT_NATS_TLS_HOST/PORT against a TLS nats-server.
  if not TryGetTlsEndpoint(host, port) then
    raise EDextNatsException.Create(
      'Set DEXT_NATS_TLS_HOST and DEXT_NATS_TLS_PORT to run the TLS integration test');

  FClient.Connect(host, port);
  Should(FClient.Connected).BeTrue;
  Should(FClient.ServerInfo.ServerId).NotBeEmpty;
end;

end.
