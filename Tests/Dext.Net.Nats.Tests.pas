{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                     }
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
unit Dext.Net.Nats.Tests;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Diagnostics,
  Dext.Collections,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.DI.Interfaces,
  Dext.DI.Core,
  Dext.Logging,
  Dext.Telemetry.Metrics,
  Dext.Net.Security,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.NKeys,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.DependencyInjection,
  Dext.Net.Nats.HealthChecks;

type
  /// <summary>In-memory ILogger for observability unit tests.</summary>
  TRecordingNatsLogger = class(TAbstractLogger)
  private
    FLock: TCriticalSection;
    FEntries: IList<string>;
    FMinLevel: TLogLevel;
  public
    constructor Create(AMinLevel: TLogLevel = TLogLevel.Trace);
    destructor Destroy; override;
    procedure Log(ALevel: TLogLevel; const AMessage: string; const AArgs: array of const); override;
    procedure Log(ALevel: TLogLevel; const AException: Exception; const AMessage: string;
      const AArgs: array of const); override;
    function IsEnabled(ALevel: TLogLevel): Boolean; override;
    function BeginScope(const AMessage: string; const AArgs: array of const): IDisposable; overload; override;
    function BeginScope(const AState: TObject): IDisposable; overload; override;
    function Contains(const AFragment: string): Boolean;
    function Count: Integer;
  end;

  [TestFixture('NATS Protocol Parser')]
  TDextNatsProtocolTests = class
  public
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeInfoFrame;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeInfoTlsRequired;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeMsgFrame;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeHMsgWithStatusAndHeaders;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodePing;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodePong;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeOk;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeErr;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeMsgWithReplyTo;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeHMsgWithPayload;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeIncrementalFragments;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeMultipleFramesInBuffer;
    [Test, Category('Unit')]
    procedure Parser_Clear_ShouldDropIncompleteFrame;
    [Test, Category('Unit')]
    procedure Parser_MaxFrameBytes_ShouldRaise;
    [Test, Category('Unit')]
    procedure Parser_GarbageLine_ShouldRaise;
    [Test, Category('Unit')]
    procedure Parser_ShouldDecodeInfoJetstreamAndAuth;
    [Test, Category('Unit')]
    procedure Encode_ShouldBuildPubAndSubFrames;
    [Test, Category('Unit')]
    procedure Encode_ShouldBuildPubWithReplyTo;
    [Test, Category('Unit')]
    procedure Encode_ShouldBuildHPub;
    [Test, Category('Unit')]
    procedure Encode_ShouldBuildConnect;
    [Test, Category('Unit')]
    procedure Encode_ShouldBuildUnsubPingPong;
    [Test, Category('Unit')]
    procedure Headers_ShouldAddSetGetIndexCount;
    [Test, Category('Unit')]
    procedure Headers_Encode_ShouldBuildNatsBlock;
    [Test, Category('Unit')]
    procedure JsonHelpers_ShouldEscapeAndParse;
    [Test, Category('Unit')]
    procedure NatsNewInbox_ShouldBeUniqueWithPrefix;
    [Test, Category('Unit')]
    procedure ConnectOptions_ShouldDefaultNoResponders;
    [Test, Category('Unit')]
    procedure ClientOptions_ShouldDefaultTlsDisabled;
    [Test, Category('Unit')]
    procedure NKey_DecodeSeed_ShouldMatchKnownVector;
    [Test, Category('Unit')]
    procedure NKey_PublicKeyAndSignNonce_ShouldMatchKnownVector;
    [Test, Category('Unit')]
    procedure NKey_ParseCreds_ShouldExtractJwtAndSeed;
    [Test, Category('Unit')]
    procedure Encode_Connect_ShouldIncludeJwtNkeySig;
    [Test, Category('Unit')]
    procedure ConsumerConfig_ShouldSerializeDefaults;
    [Test, Category('Unit')]
    procedure ConsumerConfig_ShouldSerializePushDeliverSubject;
    [Test, Category('Unit')]
    procedure ConsumerConfig_ShouldSerializeEnumVariants;
    [Test, Category('Unit')]
    procedure JsMsg_ShouldParseAckSubjectMetadata;
    [Test, Category('Unit')]
    procedure StreamConfig_ShouldSerializeDefaults;
    [Test, Category('Unit')]
    procedure StreamInfo_ShouldParseSuccessAndError;
    [Test, Category('Unit')]
    procedure ConsumerInfo_ShouldParse;
    [Test, Category('Unit')]
    procedure PublishAck_ShouldParseSuccessDuplicateAndError;
    [Test, Category('Unit')]
    procedure AckWireContract_ShouldDocumentPayloads;
  end;

  [TestFixture('NATS Client Integration (localhost:4222)')]
  TDextNatsIntegrationTests = class
  private
    FClient: TDextNatsClient;
    /// <summary>Connect to cleartext NATS. True = ready; False = soft-skip (Exit caller).</summary>
    function EnsureServerOrFail: Boolean;
    function UniqueSubject(const APrefix: string): string;
    procedure RecreateClientForStalePingReconnect(AReconnectWaitMs: Integer;
      AMaxPendingBufferBytes: Int64);
    procedure StabilizePingAfterForcedDisconnect;
    function TryConnectLiveOrSoftSkip: Boolean;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('Integration')]
    procedure Connect_ShouldHandshake;
    [Test, Category('Integration')]
    procedure PublishSubscribe_ShouldDeliverPayload;
    [Test, Category('Integration')]
    procedure RequestReply_ShouldRoundTrip;
    [Test, Category('Integration')]
    procedure Request_NoResponders_ShouldRaise;
    [Test, Category('Integration')]
    procedure QueueGroup_ShouldDeliverToOneSubscriber;
    [Test, Category('Integration')]
    procedure PublishWithHeaders_ShouldDeliverHMsg;
    [Test, Category('Integration')]
    procedure RequestWithHeaders_ShouldRoundTrip;
    [Test, Category('Integration')]
    procedure Unsubscribe_ShouldStopDelivery;
    [Test, Category('Integration')]
    procedure Unsubscribe_MaxMsgs_ShouldAutoCancel;
    [Test, Category('Integration')]
    procedure Flush_ShouldRoundTrip;
    [Test, Category('Integration')]
    procedure Ping_ShouldBeAnsweredByFlush;
    [Test, Category('Integration')]
    procedure MaxPayload_ShouldRejectOversizedPublish;
    [Test, Category('Integration')]
    procedure RequestAsync_ShouldReplyAndTimeout;
    [Test, Category('Integration')]
    procedure Events_OnConnected_ShouldFire;
    [Test, Category('Integration')]
    procedure WildcardSubscribe_ShouldMatch;
    [Test, Category('Integration')]
    procedure BinaryPayload_ShouldRoundTrip;
    [Test, Category('Integration')]
    procedure Reconnect_Outbox_ShouldDeliverBufferedPublish;
    [Test, Category('Integration')]
    procedure Resubscribe_AfterReconnect_ShouldDeliver;
    [Test, Category('Integration'), Category('Negative')]
    procedure Connect_ClosedPort_ShouldRaise;
    [Test, Category('Integration'), Category('Negative')]
    procedure Publish_BeforeConnect_ShouldRaise;
    [Test, Category('Integration'), Category('Negative')]
    procedure HandlerException_ShouldFireOnError;
    [Test, Category('Integration'), Category('Negative')]
    procedure Request_Timeout_ShouldRaise;
  end;

  [TestFixture('NATS JetStream Integration (requires nats-server -js)')]
  TDextNatsJetStreamTests = class
  private
    FClient: TDextNatsClient;
    FJs: TDextNatsJetStreamContext;
    /// <summary>Connect + require JetStream. True = ready; False = soft-skip.</summary>
    function EnsureJetStreamOrFail: Boolean;
    function UniqueName(const APrefix: string): string;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('JetStream')]
    procedure Consumer_FetchAndAck_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Consumer_PushSubscribe_ShouldDeliverAndAck;
    [Test, Category('JetStream')]
    procedure Stream_CRUD_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Stream_Update_ShouldChangeMaxMsgs;
    [Test, Category('JetStream')]
    procedure Publish_Dedup_ShouldMarkDuplicate;
    [Test, Category('JetStream')]
    procedure Consumer_CRUD_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Fetch_Batch_ShouldReturnMultiple;
    [Test, Category('JetStream')]
    procedure Nak_ShouldRedeliver;
    [Test, Category('JetStream')]
    procedure Term_ShouldNotRedeliver;
    [Test, Category('JetStream')]
    procedure InProgress_ShouldExtendAckWait;
    [Test, Category('JetStream')]
    procedure Publish_ExpectedStreamMismatch_ShouldRaise;
    [Test, Category('JetStream')]
    procedure Fetch_Empty_ShouldReturnZero;
    [Test, Category('JetStream')]
    procedure StreamExists_Missing_ShouldBeFalse;
    [Test, Category('JetStream')]
    procedure GetStreamInfo_Missing_ShouldRaise;
    [Test, Category('JetStream'), Category('Negative')]
    procedure DeleteConsumer_Missing_ShouldRaise;
    [Test, Category('JetStream'), Category('Negative')]
    procedure CreateStream_IncompatibleDuplicate_ShouldRaise;
  end;

  [TestFixture('NATS TLS Integration')]
  TDextNatsTlsIntegrationTests = class
  private
    FClient: TDextNatsClient;
    function TryGetTlsEndpoint(out AHost: string; out APort: Word): Boolean;
    /// <summary>Resolve TLS endpoint and connect. True = ready; False = soft-skip.</summary>
    function EnsureTlsOrSoftSkip(out AHost: string; out APort: Word): Boolean;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('TLS')]
    procedure Connect_Tls_ShouldHandshakeWhenConfigured;
    [Test, Category('TLS')]
    procedure PublishSubscribe_Tls_ShouldDeliverWhenConfigured;
    [Test, Category('TLS')]
    procedure RequestReply_Tls_ShouldRoundTripWhenConfigured;
  end;

  [TestFixture('NATS NKey Integration')]
  TDextNatsNKeyIntegrationTests = class
  private
    FClient: TDextNatsClient;
    function TryGetNKeyEndpoint(out AHost: string; out APort: Word;
      out ASeed: string): Boolean;
    /// <summary>Resolve NKey endpoint + seed and connect. True = ready; False = soft-skip.</summary>
    function EnsureNKeyOrSoftSkip(out AHost: string; out APort: Word): Boolean;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('NKey')]
    procedure Connect_NKey_ShouldHandshakeWhenConfigured;
    [Test, Category('NKey')]
    procedure PublishSubscribe_NKey_ShouldDeliverWhenConfigured;
  end;

  [TestFixture('NATS Concurrency / Stress')]
  TDextNatsStressTests = class
  private
    FClient: TDextNatsClient;
    function EnsureServerOrFail: Boolean;
    procedure RecreateClientForStalePingReconnect(AReconnectWaitMs: Integer;
      AMaxPendingBufferBytes: Int64);
    procedure StabilizePingAfterForcedDisconnect;
    function TryConnectLiveOrSoftSkip: Boolean;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('Stress'), Explicit('Set DEXT_NATS_RUN_STRESS=1')]
    procedure MultiSubscribe_ShouldDeliverIndependently;
    [Test, Category('Stress'), Explicit('Set DEXT_NATS_RUN_STRESS=1')]
    procedure ConcurrentRequests_ShouldRoundTrip;
    [Test, Category('Stress'), Explicit('Set DEXT_NATS_RUN_STRESS=1')]
    procedure RequestTimeout_LateReply_ShouldNotCrash;
    [Test, Category('Stress'), Explicit('Set DEXT_NATS_RUN_STRESS=1')]
    procedure StalePing_ShouldDisconnectAndReconnect;
    [Test, Category('Stress'), Explicit('Set DEXT_NATS_RUN_STRESS=1')]
    procedure PendingBuffer_ShouldRejectWhenFullDuringReconnect;
  end;

  [TestFixture('NATS DI')]
  TDextNatsDiTests = class
  public
    [Test, Category('DI')]
    procedure AddNatsClient_ShouldResolveSingleton;
    [Test, Category('DI')]
    procedure AddNatsJetStream_ShouldResolveTransientBoundToSameClient;
    [Test, Category('DI')]
    procedure ClientOptions_ShouldDefaultHostAndPort;
    [Test, Category('DI')]
    procedure AddNatsClient_ConfigureCallback_ShouldApplyOptions;
    [Test, Category('DI')]
    procedure HealthCheck_ShouldReportUnhealthyWhenDisconnected;
  end;

  [TestFixture('NATS Observability')]
  TDextNatsObservabilityTests = class
  public
    [Test, Category('Unit')]
    procedure Metrics_ShouldDefaultDisabled;
    [Test, Category('Unit')]
    procedure Metrics_Publish_ShouldIncrementLocalCounter;
    [Test, Category('Unit')]
    procedure Logger_FireError_ShouldRecordWhenAttached;
  end;

implementation

{ TRecordingNatsLogger }

constructor TRecordingNatsLogger.Create(AMinLevel: TLogLevel);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FEntries := TCollections.CreateList<string>;
  FMinLevel := AMinLevel;
end;

destructor TRecordingNatsLogger.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure TRecordingNatsLogger.Log(ALevel: TLogLevel; const AMessage: string; const AArgs: array of const);
begin
  if not IsEnabled(ALevel) then
    Exit;
  FLock.Enter;
  try
    FEntries.Add(TLogFormatter.FormatMessage(AMessage, AArgs));
  finally
    FLock.Leave;
  end;
end;

procedure TRecordingNatsLogger.Log(ALevel: TLogLevel; const AException: Exception; const AMessage: string;
  const AArgs: array of const);
begin
  Log(ALevel, AMessage, AArgs);
end;

function TRecordingNatsLogger.IsEnabled(ALevel: TLogLevel): Boolean;
begin
  Result := Ord(ALevel) >= Ord(FMinLevel);
end;

function TRecordingNatsLogger.BeginScope(const AMessage: string; const AArgs: array of const): IDisposable;
begin
  Result := TNullDisposable.Create;
end;

function TRecordingNatsLogger.BeginScope(const AState: TObject): IDisposable;
begin
  Result := TNullDisposable.Create;
end;

function TRecordingNatsLogger.Contains(const AFragment: string): Boolean;
var
  S: string;
begin
  Result := False;
  FLock.Enter;
  try
    for S in FEntries do
      if S.Contains(AFragment) then
        Exit(True);
  finally
    FLock.Leave;
  end;
end;

function TRecordingNatsLogger.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FEntries.Count;
  finally
    FLock.Leave;
  end;
end;

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

procedure FeedParserBytes(Parser: TDextNatsFrameParser; const Data: TBytes; AOffset, ACount: Integer);
var
  slice: TBytes;
begin
  SetLength(slice, ACount);
  if ACount > 0 then
    Move(Data[AOffset], slice[0], ACount);
  Parser.Append(slice, ACount);
end;

function EnvFlagTrue(const AName: string): Boolean;
var
  v: string;
begin
  v := Trim(GetEnvironmentVariable(AName));
  Result := SameText(v, '1') or SameText(v, 'true') or SameText(v, 'yes');
end;

function NatsTestHost: string;
begin
  Result := Trim(GetEnvironmentVariable('DEXT_NATS_HOST'));
  if Result = '' then
    Result := '127.0.0.1';
end;

function NatsTestPort: Word;
begin
  Result := Word(StrToIntDef(Trim(GetEnvironmentVariable('DEXT_NATS_PORT')),
    NATS_DEFAULT_PORT));
end;

/// <summary>
/// Default: soft-skip live tests when the server is absent (return False → Exit).
/// Set DEXT_NATS_REQUIRE_LIVE=1 to hard-fail instead. DEXT_NATS_SKIP_LIVE=1 always soft-skips.
/// </summary>
function LiveSoftSkipOrFail(const AReason: string): Boolean;
begin
  if EnvFlagTrue('DEXT_NATS_REQUIRE_LIVE') then
    raise EDextNatsException.Create(AReason);
  Result := False;
end;

function LiveSkippedByEnv: Boolean;
begin
  Result := EnvFlagTrue('DEXT_NATS_SKIP_LIVE');
end;

function JsUniqueSubject(const AStream: string): string;
begin
  Result := 'dext.js.' + AStream.ToLowerInvariant + '.orders';
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

procedure TDextNatsProtocolTests.Parser_ShouldDecodePong;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, 'PONG' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfPong));
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeOk;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, '+OK' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfOK));
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeErr;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, '-ERR ''Permissions Violation''' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfErr));
    Should(frame.ErrorText).Be('Permissions Violation');
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeMsgWithReplyTo;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, 'MSG foo.bar 9 _INBOX.xyz 4' + #13#10 + 'ping' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(frame.Subject).Be('foo.bar');
    Should(frame.Sid).Be(9);
    Should(frame.ReplyTo).Be('_INBOX.xyz');
    Should(Utf8OfBytes(frame.Payload)).Be('ping');
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeHMsgWithPayload;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
  headerBlock: string;
  hdrLen, total: Integer;
  payload: string;
begin
  parser := TDextNatsFrameParser.Create;
  try
    headerBlock := 'NATS/1.0' + #13#10 + 'X-Test: 1' + #13#10 + #13#10;
    payload := 'body';
    hdrLen := Length(BytesOfUtf8(headerBlock));
    total := hdrLen + Length(BytesOfUtf8(payload));
    FeedParser(parser,
      Format('HMSG orders.1 2 %d %d', [hdrLen, total]) + #13#10 +
      headerBlock + payload + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfHMsg));
    Should(frame.Headers.GetValue('X-Test')).Be('1');
    Should(Utf8OfBytes(frame.Payload)).Be('body');
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeIncrementalFragments;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
  raw: TBytes;
begin
  parser := TDextNatsFrameParser.Create;
  try
    raw := BytesOfUtf8('PING' + #13#10);
    FeedParserBytes(parser, raw, 0, 2);
    Should(parser.TryReadFrame(frame)).BeFalse;
    FeedParserBytes(parser, raw, 2, Length(raw) - 2);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfPing));
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeMultipleFramesInBuffer;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, 'PING' + #13#10 + 'PONG' + #13#10 + '+OK' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfPing));
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfPong));
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfOK));
    Should(parser.TryReadFrame(frame)).BeFalse;
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_Clear_ShouldDropIncompleteFrame;
var
  parser: TDextNatsFrameParser;
  frame: TNatsFrame;
begin
  parser := TDextNatsFrameParser.Create;
  try
    FeedParser(parser, 'MSG foo 1 5' + #13#10 + 'he');
    Should(parser.TryReadFrame(frame)).BeFalse;
    parser.Clear;
    FeedParser(parser, 'PING' + #13#10);
    Should(parser.TryReadFrame(frame)).BeTrue;
    Should(Ord(frame.Kind)).Be(Ord(nfPing));
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_MaxFrameBytes_ShouldRaise;
var
  parser: TDextNatsFrameParser;
begin
  parser := TDextNatsFrameParser.Create;
  try
    parser.MaxFrameBytes := 8;
    Should(
      procedure
      var
        frame: TNatsFrame;
      begin
        FeedParser(parser, 'MSG foo 1 100' + #13#10);
        parser.TryReadFrame(frame);
      end).Throw(EDextNatsProtocolError);
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_GarbageLine_ShouldRaise;
var
  parser: TDextNatsFrameParser;
begin
  parser := TDextNatsFrameParser.Create;
  try
    Should(
      procedure
      var
        frame: TNatsFrame;
      begin
        FeedParser(parser, 'NOTAVALIDFRAME' + #13#10);
        parser.TryReadFrame(frame);
      end).Throw(EDextNatsProtocolError);
  finally
    parser.Free;
  end;
end;

procedure TDextNatsProtocolTests.Parser_ShouldDecodeInfoJetstreamAndAuth;
var
  info: TNatsServerInfo;
begin
  info := TNatsServerInfo.Parse(
    '{"server_id":"N1","version":"2.10.0","proto":1,"auth_required":true,' +
    '"tls_available":true,"jetstream":true,"nonce":"abc123","max_payload":1048576}');
  Should(info.AuthRequired).BeTrue;
  Should(info.TlsAvailable).BeTrue;
  Should(info.Jetstream).BeTrue;
  Should(info.Nonce).Be('abc123');
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

procedure TDextNatsProtocolTests.Encode_ShouldBuildPubWithReplyTo;
var
  pubBytes: TBytes;
begin
  pubBytes := NatsEncodePub('orders', '_INBOX.r1', BytesOfUtf8('hi'));
  Should(Utf8OfBytes(pubBytes)).Be('PUB orders _INBOX.r1 2' + #13#10 + 'hi' + #13#10);
end;

procedure TDextNatsProtocolTests.Encode_ShouldBuildHPub;
var
  headers: TNatsHeaders;
  encoded, encodedReply: TBytes;
  headerBlock: TBytes;
begin
  headers.Add('X-A', '1');
  headerBlock := headers.Encode;
  encoded := NatsEncodeHPub('subj', '', headers, BytesOfUtf8('Z'));
  Should(Utf8OfBytes(encoded).StartsWith(
    Format('HPUB subj %d %d', [Length(headerBlock), Length(headerBlock) + 1]) + #13#10)).BeTrue;
  Should(Utf8OfBytes(encoded).Contains('NATS/1.0')).BeTrue;
  Should(Utf8OfBytes(encoded).Contains('X-A: 1')).BeTrue;

  encodedReply := NatsEncodeHPub('subj', 'reply.1', headers, BytesOfUtf8('Z'));
  Should(Utf8OfBytes(encodedReply).StartsWith(
    Format('HPUB subj reply.1 %d %d', [Length(headerBlock), Length(headerBlock) + 1]) + #13#10)).BeTrue;
end;

procedure TDextNatsProtocolTests.Encode_ShouldBuildConnect;
var
  opts: TNatsConnectOptions;
  encoded: string;
begin
  opts := TNatsConnectOptions.CreateDefault;
  encoded := Utf8OfBytes(NatsEncodeConnect(opts));
  Should(encoded.StartsWith('CONNECT {')).BeTrue;
  Should(encoded.EndsWith(#13#10)).BeTrue;
  Should(encoded.Contains('"no_responders":true')).BeTrue;
end;

procedure TDextNatsProtocolTests.Encode_ShouldBuildUnsubPingPong;
begin
  Should(Utf8OfBytes(NatsEncodeUnsub(9, 0))).Be('UNSUB 9' + #13#10);
  Should(Utf8OfBytes(NatsEncodeUnsub(9, 3))).Be('UNSUB 9 3' + #13#10);
  Should(Utf8OfBytes(NatsEncodePing)).Be('PING' + #13#10);
  Should(Utf8OfBytes(NatsEncodePong)).Be('PONG' + #13#10);
end;

procedure TDextNatsProtocolTests.Headers_ShouldAddSetGetIndexCount;
var
  headers: TNatsHeaders;
begin
  headers.Add('A', '1');
  headers.Add('B', '2');
  Should(headers.Count).Be(2);
  Should(headers.GetValue('A')).Be('1');
  Should(headers.IndexOf('B')).Be(1);
  headers.SetValue('A', '9');
  Should(headers.GetValue('A')).Be('9');
  Should(headers.Count).Be(2);
  headers.SetValue('C', '3');
  Should(headers.Count).Be(3);
  Should(headers.GetValue('missing', 'd')).Be('d');
end;

procedure TDextNatsProtocolTests.Headers_Encode_ShouldBuildNatsBlock;
var
  headers: TNatsHeaders;
  block: string;
begin
  headers.Add('X-One', 'a');
  block := Utf8OfBytes(headers.Encode);
  Should(block.StartsWith('NATS/1.0' + #13#10)).BeTrue;
  Should(block.Contains('X-One: a' + #13#10)).BeTrue;
  Should(block.EndsWith(#13#10 + #13#10)).BeTrue;
end;

procedure TDextNatsProtocolTests.JsonHelpers_ShouldEscapeAndParse;
var
  ack: TNatsPublishAck;
begin
  Should(NatsJsonEscape('a"b\c')).Be('a\"b\\c');
  Should(NatsJsonEscape('x' + #9 + 'y')).Be('x\ty');
  Should(NatsBoolStr(True)).Be('true');
  Should(NatsBoolStr(False)).Be('false');

  { Field getters now live on TUtf8JsonReader paths (JetStream/INFO); defaults via missing keys. }
  ack := TNatsPublishAck.Parse('{"stream":"S","seq":1}');
  Should(ack.Stream).Be('S');
  Should(ack.Sequence).Be(UInt64(1));
  Should(ack.Duplicate).BeFalse;
  Should(ack.Domain).Be('');
end;

procedure TDextNatsProtocolTests.NatsNewInbox_ShouldBeUniqueWithPrefix;
var
  a, b: string;
begin
  a := NatsNewInbox;
  b := NatsNewInbox;
  Should(a.StartsWith(NATS_INBOX_PREFIX)).BeTrue;
  Should(b.StartsWith(NATS_INBOX_PREFIX)).BeTrue;
  Should(a <> b).BeTrue;
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
  Should(opts.JWT).Be('');
  Should(opts.NKeySeed).Be('');
  Should(opts.CredentialsFile).Be('');
  Should(opts.Host).Be('localhost');
  Should(opts.Port).Be(NATS_DEFAULT_PORT);
  Should(opts.EnableMetrics).BeFalse;
end;

procedure TDextNatsProtocolTests.NKey_DecodeSeed_ShouldMatchKnownVector;
var
  raw: TBytes;
  role: Byte;
  hex: string;
  I: Integer;
const
  Seed = 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
  ExpectedSeedHex = '29497ba00f41dd45936b4cc4bda1decb67c128e69fb3d490ee820daf7c318c0a';
begin
  NatsDecodeSeed(Seed, raw, role);
  try
    Should(Length(raw)).Be(32);
    Should(role).Be($A0); // user prefix
    hex := '';
    for I := 0 to High(raw) do
      hex := hex + LowerCase(IntToHex(raw[I], 2));
    Should(hex).Be(ExpectedSeedHex);
  finally
    if Length(raw) > 0 then
      FillChar(raw[0], Length(raw), 0);
  end;
end;

procedure TDextNatsProtocolTests.NKey_PublicKeyAndSignNonce_ShouldMatchKnownVector;
const
  Seed = 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
  ExpectedPub = 'UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4';
  Nonce = 'nonce-challenge-1234567890';
  ExpectedSig =
    'qR6EjGCIjLX1njDVSXdVqTC0pw5Y4g57vCNFA6MIL590yTysHvczYlc1Mbjhbt4e8R7ug_2CZrt896AW5ghJBw';
var
  opts: TNatsConnectOptions;
begin
  // Signing needs OpenSSL libcrypto-3.dll (same as TLS); soft-skip if absent.
  if not NatsNKeyCryptoAvailable then
    Exit;

  Should(NatsPublicKeyFromSeed(Seed)).Be(ExpectedPub);
  Should(NatsSignNonce(Seed, Nonce)).Be(ExpectedSig);

  opts := TNatsConnectOptions.CreateDefault;
  NatsApplyCredentialsToConnect(opts, '', Seed, Nonce);
  Should(opts.Nkey).Be(ExpectedPub);
  Should(opts.JWT).Be('');
  Should(opts.Sig).Be(ExpectedSig);
end;

procedure TDextNatsProtocolTests.NKey_ParseCreds_ShouldExtractJwtAndSeed;
var
  creds: TNatsCredentials;
  text: string;
const
  Seed = 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
begin
  text :=
    '-----BEGIN NATS USER JWT-----' + sLineBreak +
    'eyJtest.jwt.payload' + sLineBreak +
    '------END NATS USER JWT------' + sLineBreak + sLineBreak +
    '-----BEGIN USER NKEY SEED-----' + sLineBreak +
    Seed + sLineBreak +
    '------END USER NKEY SEED------';
  creds := TNatsCredentials.Parse(text);
  Should(creds.JWT).Be('eyJtest.jwt.payload');
  Should(creds.Seed).Be(Seed);

  creds := TNatsCredentials.Parse('# comment' + sLineBreak + Seed + sLineBreak);
  Should(creds.HasJWT).BeFalse;
  Should(creds.Seed).Be(Seed);
end;

procedure TDextNatsProtocolTests.Encode_Connect_ShouldIncludeJwtNkeySig;
var
  opts: TNatsConnectOptions;
  encoded: string;
begin
  opts := TNatsConnectOptions.CreateDefault;
  opts.JWT := 'header.payload.sig';
  opts.Nkey := 'UDUMMY';
  opts.Sig := 'abc_def';
  encoded := opts.ToJson;
  Should(encoded.Contains('"jwt":"header.payload.sig"')).BeTrue;
  Should(encoded.Contains('"nkey":"UDUMMY"')).BeTrue;
  Should(encoded.Contains('"sig":"abc_def"')).BeTrue;
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
  Should(json.Contains('"max_waiting":')).BeTrue;
  Should(json.Contains('"deliver_subject"')).BeFalse;
end;

procedure TDextNatsProtocolTests.ConsumerConfig_ShouldSerializePushDeliverSubject;
var
  cfg: TNatsConsumerConfig;
  json: string;
begin
  cfg := TNatsConsumerConfig.CreateDefault('PUSH1', 'orders.*');
  cfg.DeliverSubject := 'deliver.push1';
  cfg.DeliverGroup := 'workers';
  json := cfg.ToJson;
  Should(json.Contains('"deliver_subject":"deliver.push1"')).BeTrue;
  Should(json.Contains('"deliver_group":"workers"')).BeTrue;
  Should(json.Contains('"max_waiting"')).BeFalse;
end;

procedure TDextNatsProtocolTests.ConsumerConfig_ShouldSerializeEnumVariants;
var
  cfg: TNatsConsumerConfig;
  json: string;
begin
  cfg := TNatsConsumerConfig.CreateDefault('C1', 's.*');
  cfg.DeliverPolicy := dpLast;
  cfg.AckPolicy := apAll;
  cfg.ReplayPolicy := rpOriginal;
  json := cfg.ToJson;
  Should(json.Contains('"deliver_policy":"last"')).BeTrue;
  Should(json.Contains('"ack_policy":"all"')).BeTrue;
  Should(json.Contains('"replay_policy":"original"')).BeTrue;
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

procedure TDextNatsProtocolTests.StreamConfig_ShouldSerializeDefaults;
var
  cfg: TNatsStreamConfig;
  json: string;
begin
  cfg := TNatsStreamConfig.CreateDefault('ORDERS', ['orders.*', 'returns.*']);
  cfg.Storage := ssMemory;
  cfg.Retention := srLimits;
  json := cfg.ToJson;
  Should(json.Contains('"name":"ORDERS"')).BeTrue;
  Should(json.Contains('"orders.*"')).BeTrue;
  Should(json.Contains('"returns.*"')).BeTrue;
  Should(json.Contains('"storage":"memory"')).BeTrue;
  Should(json.Contains('"retention":"limits"')).BeTrue;
  Should(json.Contains('"duplicate_window":120000000000')).BeTrue;
end;

procedure TDextNatsProtocolTests.StreamInfo_ShouldParseSuccessAndError;
var
  info: TNatsStreamInfo;
begin
  info := TNatsStreamInfo.Parse(
    '{"config":{"name":"S1"},"state":{"messages":3,"bytes":30,"first_seq":1,' +
    '"last_seq":3,"consumer_count":2}}');
  Should(info.Name).Be('S1');
  Should(Int64(info.Messages)).Be(3);
  Should(Int64(info.Bytes)).Be(30);
  Should(Int64(info.FirstSeq)).Be(1);
  Should(Int64(info.LastSeq)).Be(3);
  Should(info.ConsumerCount).Be(2);

  Should(
    procedure
    begin
      TNatsStreamInfo.Parse(
        '{"error":{"code":404,"err_code":10059,"description":"stream not found"}}');
    end).Throw(EDextNatsJetStreamError);
end;

procedure TDextNatsProtocolTests.ConsumerInfo_ShouldParse;
var
  info: TNatsConsumerInfo;
begin
  info := TNatsConsumerInfo.Parse(
    '{"stream_name":"S1","name":"C1","num_pending":4,"num_ack_pending":1,' +
    '"num_redelivered":0,"num_waiting":0,"config":{"durable_name":"C1",' +
    '"filter_subject":"orders.*"}}');
  Should(info.StreamName).Be('S1');
  Should(info.Name).Be('C1');
  Should(info.DurableName).Be('C1');
  Should(info.FilterSubject).Be('orders.*');
  Should(Int64(info.NumPending)).Be(4);
  Should(info.NumAckPending).Be(1);
end;

procedure TDextNatsProtocolTests.PublishAck_ShouldParseSuccessDuplicateAndError;
var
  ack: TNatsPublishAck;
begin
  ack := TNatsPublishAck.Parse('{"stream":"S1","seq":9,"duplicate":true,"domain":"d1"}');
  Should(ack.Stream).Be('S1');
  Should(Int64(ack.Sequence)).Be(9);
  Should(ack.Duplicate).BeTrue;
  Should(ack.Domain).Be('d1');

  Should(
    procedure
    begin
      TNatsPublishAck.Parse(
        '{"error":{"code":400,"err_code":10060,"description":"wrong last sequence"}}');
    end).Throw(EDextNatsJetStreamError);
end;

procedure TDextNatsProtocolTests.AckWireContract_ShouldDocumentPayloads;
begin
  // Contract mirrored from TDextNatsJetStreamContext ack helpers (no I/O).
  Should('+ACK').Be('+ACK');
  Should('+NAK').Be('+NAK');
  Should(Format('+NAK {"delay":%d}', [Int64(250) * 1000000])).Be('+NAK {"delay":250000000}');
  Should('+TERM').Be('+TERM');
  Should('+WPI').Be('+WPI');
end;

{ TDextNatsIntegrationTests }

function TDextNatsIntegrationTests.UniqueSubject(const APrefix: string): string;
begin
  Result := APrefix + '.' + FormatDateTime('hhnnsszzz', Now) + '.' +
    IntToHex(Random(MaxInt), 8);
end;

procedure TDextNatsIntegrationTests.StabilizePingAfterForcedDisconnect;
var
  opts: TDextNatsOptions;
begin
  // MaxPingsOutstanding=0 is only used to induce one stale-ping socket close.
  // Raise limits immediately so PingLoop does not flap after reconnect.
  opts := FClient.Options;
  opts.MaxPingsOutstanding := 10;
  opts.PingIntervalMs := 120000;
  FClient.Options := opts;
end;

procedure TDextNatsIntegrationTests.RecreateClientForStalePingReconnect(
  AReconnectWaitMs: Integer; AMaxPendingBufferBytes: Int64);
var
  opts: TDextNatsOptions;
begin
  if Assigned(FClient) then
  begin
    try
      FClient.Disconnect;
    except
    end;
    FreeAndNil(FClient);
  end;

  opts := TDextNatsOptions.CreateDefault;
  opts.AllowReconnect := True;
  opts.MaxReconnectAttempts := 20;
  opts.ReconnectWaitMs := AReconnectWaitMs;
  opts.PingIntervalMs := 120;
  opts.MaxPingsOutstanding := 0;
  opts.MaxPendingBufferBytes := AMaxPendingBufferBytes;
  opts.ConnectTimeoutMs := 5000;
  opts.RequestTimeoutMs := 5000;
  FClient := TDextNatsClient.Create(opts);
end;

function TDextNatsIntegrationTests.TryConnectLiveOrSoftSkip: Boolean;
begin
  Result := False;
  if LiveSkippedByEnv then
    Exit;

  try
    FClient.Connect(NatsTestHost, NatsTestPort);
    Result := True;
  except
    on E: Exception do
      Result := LiveSoftSkipOrFail(
        Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
          'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [NatsTestHost, NatsTestPort, E.Message]));
  end;
end;

function TDextNatsIntegrationTests.EnsureServerOrFail: Boolean;
begin
  Result := TryConnectLiveOrSoftSkip;
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
  if not EnsureServerOrFail then
    Exit;
  Should(FClient.Connected).BeTrue;
  Should(FClient.ServerInfo.ServerId).NotBeEmpty;
end;

procedure TDextNatsIntegrationTests.PublishSubscribe_ShouldDeliverPayload;
var
  received: TEvent;
  payload: string;
  subject: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.pubsub');
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
  if not EnsureServerOrFail then
    Exit;
  serviceSubject := UniqueSubject('dext.nats.test.req');

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
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.no.responders');
  Should(
    procedure
    begin
      FClient.Request(subject, 'anything', 2000);
    end).Throw(EDextNatsNoResponders);
end;

procedure TDextNatsIntegrationTests.QueueGroup_ShouldDeliverToOneSubscriber;
var
  subject, queue: string;
  done: TEvent;
  hits: Integer;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.queue');
  queue := 'workers';
  hits := 0;
  done := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        done.SetEvent;
      end, queue);
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        done.SetEvent;
      end, queue);

    FClient.Publish(subject, 'one');
    Should(done.WaitFor(3000) = wrSignaled).BeTrue;
    Sleep(200);
    Should(hits).Be(1);
  finally
    done.Free;
  end;
end;

procedure TDextNatsIntegrationTests.PublishWithHeaders_ShouldDeliverHMsg;
var
  subject: string;
  headers: TNatsHeaders;
  received: TEvent;
  gotHeader, gotPayload: string;
  gotStatus: Integer;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.hdr');
  headers.Add('X-Order', '42');
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        gotHeader := AMsg.Headers.GetValue('X-Order');
        gotPayload := AMsg.AsString;
        gotStatus := AMsg.StatusCode;
        received.SetEvent;
      end);

    FClient.PublishWithHeaders(subject, BytesOfUtf8('hdr-body'), headers);
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(gotHeader).Be('42');
    Should(gotPayload).Be('hdr-body');
    Should(gotStatus).Be(0);
  finally
    received.Free;
  end;
end;

procedure TDextNatsIntegrationTests.RequestWithHeaders_ShouldRoundTrip;
var
  subject: string;
  headers: TNatsHeaders;
  reply: TNatsMsg;
  seen: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.reqhdr');
  headers.Add('X-Trace', 't-1');
  seen := '';

  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
      seen := AMsg.Headers.GetValue('X-Trace');
      if AMsg.HasReplyTo then
        FClient.Publish(AMsg.ReplyTo, 'ok');
    end);

  reply := FClient.RequestWithHeaders(subject, BytesOfUtf8('q'), headers, 3000);
  Should(reply.AsString).Be('ok');
  Should(seen).Be('t-1');
end;

procedure TDextNatsIntegrationTests.Unsubscribe_ShouldStopDelivery;
var
  subject: string;
  sid: Integer;
  hits: Integer;
  received: TEvent;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.unsub');
  hits := 0;
  received := TEvent.Create(nil, True, False, '');
  try
    sid := FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        received.SetEvent;
      end);
    FClient.Publish(subject, 'a');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;

    FClient.Unsubscribe(sid);
    FClient.Flush(2000);
    received.ResetEvent;
    FClient.Publish(subject, 'b');
    Should(received.WaitFor(500) = wrSignaled).BeFalse;
    Should(hits).Be(1);
  finally
    received.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Unsubscribe_MaxMsgs_ShouldAutoCancel;
var
  subject: string;
  sid: Integer;
  hits: Integer;
  received: TEvent;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.maxmsgs');
  hits := 0;
  received := TEvent.Create(nil, False, False, '');
  try
    sid := FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        received.SetEvent;
      end);
    FClient.Unsubscribe(sid, 1);
    FClient.Publish(subject, '1');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    FClient.Publish(subject, '2');
    Sleep(400);
    Should(hits).Be(1);
  finally
    received.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Flush_ShouldRoundTrip;
begin
  if not EnsureServerOrFail then
    Exit;
  FClient.Publish(UniqueSubject('dext.nats.test.flush'), 'x');
  FClient.Flush(3000);
  Should(FClient.Connected).BeTrue;
end;

procedure TDextNatsIntegrationTests.Ping_ShouldBeAnsweredByFlush;
begin
  if not EnsureServerOrFail then
    Exit;
  FClient.Ping;
  FClient.Flush(3000);
  Should(FClient.Connected).BeTrue;
end;

procedure TDextNatsIntegrationTests.MaxPayload_ShouldRejectOversizedPublish;
var
  oversized: TBytes;
  maxPayload: Int64;
begin
  if not EnsureServerOrFail then
    Exit;
  maxPayload := FClient.ServerInfo.MaxPayload;
  Should(maxPayload > 0).BeTrue;
  SetLength(oversized, maxPayload + 1);
  Should(
    procedure
    begin
      FClient.Publish(UniqueSubject('dext.nats.test.maxpayload'), oversized);
    end).Throw(EDextNatsException);
end;

procedure TDextNatsIntegrationTests.RequestAsync_ShouldReplyAndTimeout;
var
  subject, silentSubject: string;
  replied, timedOut: TEvent;
  replyText: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.reqasync');
  silentSubject := UniqueSubject('dext.nats.test.reqasync.silent');
  replied := TEvent.Create(nil, True, False, '');
  timedOut := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        if AMsg.HasReplyTo then
          FClient.Publish(AMsg.ReplyTo, 'async-ok');
      end);

    FClient.RequestAsync(subject, BytesOfUtf8('q'),
      procedure(const AMsg: TNatsMsg)
      begin
        replyText := AMsg.AsString;
        replied.SetEvent;
      end, nil, 3000);
    Should(replied.WaitFor(3000) = wrSignaled).BeTrue;
    Should(replyText).Be('async-ok');

    // Subscriber present but silent — avoids 503 no-responders; timeout path must fire.
    FClient.Subscribe(silentSubject,
      procedure(const AMsg: TNatsMsg)
      begin
      end);
    FClient.RequestAsync(silentSubject, BytesOfUtf8('q'),
      procedure(const AMsg: TNatsMsg)
      begin
      end,
      procedure
      begin
        timedOut.SetEvent;
      end, 300);
    Should(timedOut.WaitFor(2000) = wrSignaled).BeTrue;
  finally
    replied.Free;
    timedOut.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Events_OnConnected_ShouldFire;
var
  connected: Boolean;
  serverId: string;
begin
  connected := False;
  FClient.OnConnected :=
    procedure(const AInfo: TNatsServerInfo; AIsReconnect: Boolean)
    begin
      connected := True;
      serverId := AInfo.ServerId;
    end;
  if not EnsureServerOrFail then
    Exit;
  Should(connected).BeTrue;
  Should(serverId).NotBeEmpty;
end;

procedure TDextNatsIntegrationTests.WildcardSubscribe_ShouldMatch;
var
  root, leaf: string;
  received: TEvent;
  got: string;
begin
  if not EnsureServerOrFail then
    Exit;
  root := UniqueSubject('dext.nats.test.wild');
  leaf := root + '.child';
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(root + '.>',
      procedure(const AMsg: TNatsMsg)
      begin
        got := AMsg.AsString;
        received.SetEvent;
      end);
    FClient.Publish(leaf, 'wild');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(got).Be('wild');
  finally
    received.Free;
  end;
end;

procedure TDextNatsIntegrationTests.BinaryPayload_ShouldRoundTrip;
var
  subject: string;
  payload, got: TBytes;
  received: TEvent;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.bin');
  payload := TBytes.Create(0, 1, 2, 255, 127, 10);
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        got := Copy(AMsg.Payload);
        received.SetEvent;
      end);
    FClient.Publish(subject, payload);
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(Length(got)).Be(Length(payload));
    Should(got[0]).Be(0);
    Should(got[3]).Be(255);
    Should(got[5]).Be(10);
  finally
    received.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Reconnect_Outbox_ShouldDeliverBufferedPublish;
var
  subject: string;
  received, reconnected: TEvent;
  payload: string;
begin
  if LiveSkippedByEnv then
    Exit;

  RecreateClientForStalePingReconnect(400, 8 * 1024 * 1024);
  subject := UniqueSubject('dext.nats.test.outbox');
  received := TEvent.Create(nil, True, False, '');
  reconnected := TEvent.Create(nil, True, False, '');
  try
    FClient.OnDisconnected :=
      procedure
      begin
        StabilizePingAfterForcedDisconnect;
        // Still inside HandleConnectionLost, before TryReconnect — buffer into outbox.
        FClient.Publish(subject, 'buffered-during-disconnect');
      end;
    FClient.OnConnected :=
      procedure(const AInfo: TNatsServerInfo; AIsReconnect: Boolean)
      begin
        if AIsReconnect then
          reconnected.SetEvent;
      end;

    try
      FClient.Connect(NatsTestHost, NatsTestPort);
    except
      on E: Exception do
      begin
        LiveSoftSkipOrFail(
          Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
            'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
            [NatsTestHost, NatsTestPort, E.Message]));
        Exit;
      end;
    end;

    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        payload := AMsg.AsString;
        received.SetEvent;
      end);

    Should(reconnected.WaitFor(10000) = wrSignaled).BeTrue;
    Should(received.WaitFor(5000) = wrSignaled).BeTrue;
    Should(payload).Be('buffered-during-disconnect');
    Should(FClient.Connected).BeTrue;
  finally
    received.Free;
    reconnected.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Resubscribe_AfterReconnect_ShouldDeliver;
var
  subject: string;
  received, reconnected: TEvent;
  payload: string;
begin
  if LiveSkippedByEnv then
    Exit;

  RecreateClientForStalePingReconnect(400, 8 * 1024 * 1024);
  subject := UniqueSubject('dext.nats.test.resub');
  received := TEvent.Create(nil, True, False, '');
  reconnected := TEvent.Create(nil, True, False, '');
  try
    FClient.OnDisconnected :=
      procedure
      begin
        StabilizePingAfterForcedDisconnect;
      end;
    FClient.OnConnected :=
      procedure(const AInfo: TNatsServerInfo; AIsReconnect: Boolean)
      begin
        if AIsReconnect then
          reconnected.SetEvent;
      end;

    try
      FClient.Connect(NatsTestHost, NatsTestPort);
    except
      on E: Exception do
      begin
        LiveSoftSkipOrFail(
          Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
            'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
            [NatsTestHost, NatsTestPort, E.Message]));
        Exit;
      end;
    end;

    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        payload := AMsg.AsString;
        received.SetEvent;
      end);

    Should(reconnected.WaitFor(10000) = wrSignaled).BeTrue;
    Should(FClient.Connected).BeTrue;

    // Fresh publish after reconnect — proves ResendSubscriptions restored the SUB.
    FClient.Publish(subject, 'after-reconnect');
    Should(received.WaitFor(5000) = wrSignaled).BeTrue;
    Should(payload).Be('after-reconnect');
  finally
    received.Free;
    reconnected.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Connect_ClosedPort_ShouldRaise;
begin
  Should(
    procedure
    begin
      FClient.Connect('127.0.0.1', 1);
    end).Throw(Exception);
end;

procedure TDextNatsIntegrationTests.Publish_BeforeConnect_ShouldRaise;
begin
  Should(
    procedure
    begin
      FClient.Publish('dext.nats.test.before.connect', 'x');
    end).Throw(EDextNatsException);
end;

procedure TDextNatsIntegrationTests.HandlerException_ShouldFireOnError;
var
  subject: string;
  errEvent: TEvent;
  errText: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.handler.err');
  errEvent := TEvent.Create(nil, True, False, '');
  try
    FClient.OnError :=
      procedure(const AErrorMessage: string)
      begin
        errText := AErrorMessage;
        errEvent.SetEvent;
      end;
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        raise Exception.Create('boom-from-handler');
      end);
    FClient.Publish(subject, 'x');
    Should(errEvent.WaitFor(3000) = wrSignaled).BeTrue;
    Should(errText.Contains('boom-from-handler')).BeTrue;
    Should(FClient.Connected).BeTrue;
  finally
    errEvent.Free;
  end;
end;

procedure TDextNatsIntegrationTests.Request_Timeout_ShouldRaise;
var
  subject: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.req.timeout');
  // Silent subscriber avoids 503 no-responders; Request must time out instead.
  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
    end);

  Should(
    procedure
    begin
      FClient.Request(subject, 'q', 250);
    end).Throw(EDextNatsTimeoutError);
end;

{ TDextNatsJetStreamTests }

function TDextNatsJetStreamTests.UniqueName(const APrefix: string): string;
begin
  Result := APrefix + '_' + FormatDateTime('hhnnsszzz', Now) + '_' +
    IntToHex(Random(MaxInt), 6);
end;

function TDextNatsJetStreamTests.EnsureJetStreamOrFail: Boolean;
begin
  Result := False;
  if LiveSkippedByEnv then
    Exit;

  try
    FClient.Connect(NatsTestHost, NatsTestPort);
  except
    on E: Exception do
    begin
      Result := LiveSoftSkipOrFail(
        Format('NATS server not reachable at %s:%d (%s). Start nats-server -js, ' +
          'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [NatsTestHost, NatsTestPort, E.Message]));
      Exit;
    end;
  end;

  if not FClient.ServerInfo.Jetstream then
  begin
    Result := LiveSoftSkipOrFail(
      Format('NATS server at %s:%d has JetStream disabled (INFO jetstream!=true). ' +
        'Start with: nats-server -js (or omit DEXT_NATS_REQUIRE_LIVE for soft-skip).',
        [NatsTestHost, NatsTestPort]));
    Exit;
  end;

  FJs := TDextNatsJetStreamContext.Create(FClient);
  Result := True;
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
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  info: TNatsConsumerInfo;
  msgs: IList<TNatsJsMsg>;
  ack: TNatsPublishAck;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_STREAM');
  consumer := UniqueName('DEXT_JS_PULL');
  subject := JsUniqueSubject(stream);

  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    info := FJs.CreateConsumer(stream, consumerCfg);
    Should(info.Name).Be(consumer);
    Should(info.StreamName).Be(stream);

    info := FJs.GetConsumerInfo(stream, consumer);
    Should(info.DurableName).Be(consumer);

    ack := FJs.Publish(subject, 'order-1', 'js-test-msg-1');
    Should(ack.Stream).Be(stream);
    Should(ack.Duplicate).BeFalse;

    msgs := FJs.Fetch(stream, consumer, 1, 3000);
    Should(msgs.Count).Be(1);
    Should(msgs[0].AsString).Be('order-1');
    Should(msgs[0].Stream).Be(stream);
    Should(msgs[0].ReplyTo.StartsWith('$JS.ACK.')).BeTrue;

    FJs.Ack(msgs[0]);
    FClient.Flush(2000);

    msgs := FJs.Fetch(stream, consumer, 1, 500);
    Should(msgs.Count).Be(0);

    Should(FJs.DeleteConsumer(stream, consumer)).BeTrue;
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Consumer_PushSubscribe_ShouldDeliverAndAck;
var
  stream, consumer, subject, deliver: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  info: TNatsConsumerInfo;
  sub: TDextNatsJetStreamPushSubscription;
  got: TEvent;
  payload: string;
  jsCtx: TDextNatsJetStreamContext;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_PUSH_S');
  consumer := UniqueName('DEXT_JS_PUSH_C');
  subject := JsUniqueSubject(stream);
  deliver := '_INBOX.dext.push.' + UniqueName('d');
  payload := '';
  got := TEvent.Create(nil, True, False, '');
  try
    streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
    streamCfg.Storage := ssMemory;
    FJs.CreateStream(streamCfg);
    try
      consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
      consumerCfg.DeliverSubject := deliver;
      info := FJs.CreateConsumer(stream, consumerCfg);
      Should(info.DeliverSubject).Be(deliver);

      jsCtx := FJs;
      sub := FJs.SubscribePush(stream, consumer,
        procedure(const AMsg: TNatsJsMsg)
        begin
          payload := AMsg.AsString;
          jsCtx.Ack(AMsg);
          got.SetEvent;
        end);
      try
        FJs.Publish(subject, 'push-hello');
        Should(got.WaitFor(5000) = wrSignaled).BeTrue;
        Should(payload).Be('push-hello');
      finally
        sub.Free;
      end;

      Should(FJs.DeleteConsumer(stream, consumer)).BeTrue;
    finally
      FJs.DeleteStream(stream);
    end;
  finally
    got.Free;
  end;
end;

procedure TDextNatsJetStreamTests.Stream_CRUD_ShouldRoundTrip;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
  info: TNatsStreamInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_CRUD');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  info := FJs.CreateStream(cfg);
  try
    Should(info.Name).Be(stream);
    Should(FJs.StreamExists(stream)).BeTrue;
    info := FJs.GetStreamInfo(stream);
    Should(info.Name).Be(stream);
    Should(FJs.DeleteStream(stream)).BeTrue;
    Should(FJs.StreamExists(stream)).BeFalse;
  except
    if FJs.StreamExists(stream) then
      FJs.DeleteStream(stream);
    raise;
  end;
end;

procedure TDextNatsJetStreamTests.Stream_Update_ShouldChangeMaxMsgs;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
  info: TNatsStreamInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_UPD');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  cfg.MaxMsgs := 100;
  FJs.CreateStream(cfg);
  try
    cfg.MaxMsgs := 50;
    info := FJs.UpdateStream(cfg);
    Should(info.Name).Be(stream);
    // Update accepted when no exception; server may not echo MaxMsgs in StreamInfo.
    Should(FJs.StreamExists(stream)).BeTrue;
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Publish_Dedup_ShouldMarkDuplicate;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
  ack1, ack2: TNatsPublishAck;
  info: TNatsStreamInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_DEDUP');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  FJs.CreateStream(cfg);
  try
    ack1 := FJs.Publish(subject, 'same', 'dedup-id-1');
    ack2 := FJs.Publish(subject, 'same', 'dedup-id-1');
    Should(ack1.Duplicate).BeFalse;
    Should(ack2.Duplicate).BeTrue;
    info := FJs.GetStreamInfo(stream);
    Should(Int64(info.Messages)).Be(1);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Consumer_CRUD_ShouldRoundTrip;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  info: TNatsConsumerInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_CC');
  consumer := UniqueName('DEXT_JS_CCONS');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    info := FJs.CreateConsumer(stream, consumerCfg);
    Should(info.Name).Be(consumer);
    info := FJs.GetConsumerInfo(stream, consumer);
    Should(info.DurableName).Be(consumer);
    Should(FJs.DeleteConsumer(stream, consumer)).BeTrue;
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Fetch_Batch_ShouldReturnMultiple;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  msgs: IList<TNatsJsMsg>;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_BATCH');
  consumer := UniqueName('DEXT_JS_BATCHC');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    FJs.CreateConsumer(stream, consumerCfg);
    for i := 1 to 3 do
      FJs.Publish(subject, 'm' + IntToStr(i));
    msgs := FJs.Fetch(stream, consumer, 3, 3000);
    Should(msgs.Count).Be(3);
    Should(msgs[0].AsString).Be('m1');
    Should(msgs[2].AsString).Be('m3');
    for i := 0 to msgs.Count - 1 do
      FJs.Ack(msgs[i]);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Nak_ShouldRedeliver;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  msgs: IList<TNatsJsMsg>;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_NAK');
  consumer := UniqueName('DEXT_JS_NAKC');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    consumerCfg.AckWait := 1000000000; // 1s
    FJs.CreateConsumer(stream, consumerCfg);
    FJs.Publish(subject, 'nak-me');
    msgs := FJs.Fetch(stream, consumer, 1, 3000);
    Should(msgs.Count).Be(1);
    FJs.Nak(msgs[0], 200);
    FClient.Flush(2000);
    Sleep(400);
    msgs := FJs.Fetch(stream, consumer, 1, 3000);
    Should(msgs.Count).Be(1);
    Should(msgs[0].AsString).Be('nak-me');
    FJs.Ack(msgs[0]);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Term_ShouldNotRedeliver;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  msgs: IList<TNatsJsMsg>;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_TERM');
  consumer := UniqueName('DEXT_JS_TERMC');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    consumerCfg.AckWait := 1000000000;
    FJs.CreateConsumer(stream, consumerCfg);
    FJs.Publish(subject, 'term-me');
    msgs := FJs.Fetch(stream, consumer, 1, 3000);
    Should(msgs.Count).Be(1);
    FJs.Term(msgs[0]);
    FClient.Flush(2000);
    Sleep(1200);
    msgs := FJs.Fetch(stream, consumer, 1, 800);
    Should(msgs.Count).Be(0);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.InProgress_ShouldExtendAckWait;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  msgs: IList<TNatsJsMsg>;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_WPI');
  consumer := UniqueName('DEXT_JS_WPIC');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    consumerCfg.AckWait := 1000000000; // 1s
    FJs.CreateConsumer(stream, consumerCfg);
    FJs.Publish(subject, 'wpi-me');
    msgs := FJs.Fetch(stream, consumer, 1, 3000);
    Should(msgs.Count).Be(1);
    Sleep(600);
    FJs.InProgress(msgs[0]);
    FClient.Flush(2000);
    Sleep(600);
    // Still within extended AckWait — should not redeliver a second copy yet.
    msgs := FJs.Fetch(stream, consumer, 1, 300);
    Should(msgs.Count).Be(0);
    // Ack original via a fresh fetch after wait expires would redeliver; ack by publishing WPI then Ack on first reply.
    // Re-fetch after AckWait from last WPI:
    Sleep(1200);
    msgs := FJs.Fetch(stream, consumer, 1, 2000);
    Should(msgs.Count).Be(1);
    FJs.Ack(msgs[0]);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Publish_ExpectedStreamMismatch_ShouldRaise;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
  opts: TNatsJetStreamPublishOptions;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_EXP');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  FJs.CreateStream(cfg);
  try
    opts := TNatsJetStreamPublishOptions.CreateDefault;
    opts.ExpectedStream := 'NO_SUCH_STREAM_XYZ';
    Should(
      procedure
      begin
        FJs.Publish(subject, BytesOfUtf8('x'), opts);
      end).Throw(EDextNatsJetStreamError);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Fetch_Empty_ShouldReturnZero;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  msgs: IList<TNatsJsMsg>;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_EMPTY');
  consumer := UniqueName('DEXT_JS_EMPTYC');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    FJs.CreateConsumer(stream, consumerCfg);
    msgs := FJs.Fetch(stream, consumer, 1, 400);
    Should(msgs.Count).Be(0);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.StreamExists_Missing_ShouldBeFalse;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  Should(FJs.StreamExists('DEXT_JS_DOES_NOT_EXIST_' + IntToHex(Random(MaxInt), 8))).BeFalse;
end;

procedure TDextNatsJetStreamTests.GetStreamInfo_Missing_ShouldRaise;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  Should(
    procedure
    begin
      FJs.GetStreamInfo('DEXT_JS_MISSING_' + IntToHex(Random(MaxInt), 8));
    end).Throw(EDextNatsJetStreamError);
end;

procedure TDextNatsJetStreamTests.DeleteConsumer_Missing_ShouldRaise;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_DELC');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  FJs.CreateStream(cfg);
  try
    Should(
      procedure
      begin
        FJs.DeleteConsumer(stream, 'no_such_consumer');
      end).Throw(EDextNatsJetStreamError);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.CreateStream_IncompatibleDuplicate_ShouldRaise;
var
  stream, subjectA, subjectB: string;
  cfg: TNatsStreamConfig;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_DUPCFG');
  subjectA := JsUniqueSubject(stream) + '.a';
  subjectB := JsUniqueSubject(stream) + '.b';
  cfg := TNatsStreamConfig.CreateDefault(stream, [subjectA]);
  cfg.Storage := ssMemory;
  FJs.CreateStream(cfg);
  try
    cfg := TNatsStreamConfig.CreateDefault(stream, [subjectB]);
    cfg.Storage := ssMemory;
    Should(
      procedure
      begin
        FJs.CreateStream(cfg);
      end).Throw(EDextNatsJetStreamError);
  finally
    FJs.DeleteStream(stream);
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

function TDextNatsTlsIntegrationTests.EnsureTlsOrSoftSkip(out AHost: string;
  out APort: Word): Boolean;
begin
  Result := False;
  if LiveSkippedByEnv then
    Exit;

  // TLS remains env-gated: missing DEXT_NATS_TLS_PORT soft-skips even with REQUIRE_LIVE.
  if not TryGetTlsEndpoint(AHost, APort) then
    Exit;

  try
    FClient.Connect(AHost, APort);
    Result := True;
  except
    on E: Exception do
      Result := LiveSoftSkipOrFail(
        Format('NATS TLS server not reachable at %s:%d (%s). Start nats-server -c Tests/tls/nats-tls.conf, ' +
          'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [AHost, APort, E.Message]));
  end;
end;

procedure TDextNatsTlsIntegrationTests.SetUp;
var
  opts: TDextNatsOptions;
begin
  opts := TDextNatsOptions.CreateDefault;
  opts.TLS := TDextTLSOptions.DefaultClient;
  opts.TLS.Enabled := True;
  opts.TLS.VerifyServerCertificate := False;
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
  // Dext.Testing has no programmatic Skip; soft-skip = Exit without assertions.
  if not EnsureTlsOrSoftSkip(host, port) then
    Exit;

  Should(FClient.Connected).BeTrue;
  Should(FClient.ServerInfo.ServerId).NotBeEmpty;
end;

procedure TDextNatsTlsIntegrationTests.PublishSubscribe_Tls_ShouldDeliverWhenConfigured;
var
  host: string;
  port: Word;
  subject: string;
  received: TEvent;
  payload: string;
begin
  if not EnsureTlsOrSoftSkip(host, port) then
    Exit;

  subject := 'dext.nats.tls.' + FormatDateTime('hhnnsszzz', Now);
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        payload := AMsg.AsString;
        received.SetEvent;
      end);
    FClient.Publish(subject, 'tls-hi');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(payload).Be('tls-hi');
  finally
    received.Free;
  end;
end;

procedure TDextNatsTlsIntegrationTests.RequestReply_Tls_ShouldRoundTripWhenConfigured;
var
  host: string;
  port: Word;
  subject: string;
  reply: TNatsMsg;
begin
  if not EnsureTlsOrSoftSkip(host, port) then
    Exit;

  subject := 'dext.nats.tls.req.' + FormatDateTime('hhnnsszzz', Now);
  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
      if AMsg.HasReplyTo then
        FClient.Publish(AMsg.ReplyTo, 'tls-reply:' + AMsg.AsString);
    end);

  reply := FClient.Request(subject, 'ping', 3000);
  Should(reply.AsString).Be('tls-reply:ping');
end;

{ TDextNatsNKeyIntegrationTests }

function TDextNatsNKeyIntegrationTests.TryGetNKeyEndpoint(out AHost: string;
  out APort: Word; out ASeed: string): Boolean;
var
  portStr, seedFile, credsFile: string;
  creds: TNatsCredentials;
begin
  AHost := Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_HOST'));
  if AHost = '' then
    AHost := '127.0.0.1';
  portStr := Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_PORT'));
  APort := Word(StrToIntDef(portStr, 0));
  ASeed := Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_SEED'));
  seedFile := Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_SEED_FILE'));
  credsFile := Trim(GetEnvironmentVariable('DEXT_NATS_CREDS_FILE'));

  if (ASeed = '') and (seedFile <> '') and FileExists(seedFile) then
  begin
    creds := TNatsCredentials.FromFile(seedFile);
    ASeed := creds.Seed;
  end;
  if (ASeed = '') and (credsFile <> '') and FileExists(credsFile) then
  begin
    creds := TNatsCredentials.FromFile(credsFile);
    ASeed := creds.Seed;
  end;

  Result := (APort > 0) and (ASeed <> '');
end;

function TDextNatsNKeyIntegrationTests.EnsureNKeyOrSoftSkip(out AHost: string;
  out APort: Word): Boolean;
var
  seed: string;
  opts: TDextNatsOptions;
begin
  Result := False;
  if LiveSkippedByEnv then
    Exit;

  // NKey remains env-gated: missing DEXT_NATS_NKEY_PORT/SEED soft-skips even with REQUIRE_LIVE.
  if not TryGetNKeyEndpoint(AHost, APort, seed) then
    Exit;

  if not NatsNKeyCryptoAvailable then
  begin
    Result := LiveSoftSkipOrFail(
      'OpenSSL libcrypto-3.dll not available for NKey signing (place beside the test exe).');
    Exit;
  end;

  opts := TDextNatsOptions.CreateDefault;
  opts.NKeySeed := seed;
  opts.CredentialsFile := Trim(GetEnvironmentVariable('DEXT_NATS_CREDS_FILE'));
  if Assigned(FClient) then
    FreeAndNil(FClient);
  FClient := TDextNatsClient.Create(opts);

  try
    FClient.Connect(AHost, APort);
    Result := True;
  except
    on E: Exception do
      Result := LiveSoftSkipOrFail(
        Format('NATS NKey server not reachable at %s:%d (%s). Start nats-server -c Tests/nkey/nats-nkey.conf, ' +
          'set DEXT_NATS_NKEY_PORT/SEED, or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [AHost, APort, E.Message]));
  end;
end;

procedure TDextNatsNKeyIntegrationTests.SetUp;
begin
  FClient := nil;
end;

procedure TDextNatsNKeyIntegrationTests.TearDown;
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

procedure TDextNatsNKeyIntegrationTests.Connect_NKey_ShouldHandshakeWhenConfigured;
var
  host: string;
  port: Word;
begin
  if not EnsureNKeyOrSoftSkip(host, port) then
    Exit;

  Should(FClient.Connected).BeTrue;
  Should(FClient.ServerInfo.AuthRequired).BeTrue;
  Should(FClient.ServerInfo.ServerId).NotBeEmpty;
end;

procedure TDextNatsNKeyIntegrationTests.PublishSubscribe_NKey_ShouldDeliverWhenConfigured;
var
  host: string;
  port: Word;
  subject: string;
  received: TEvent;
  payload: string;
begin
  if not EnsureNKeyOrSoftSkip(host, port) then
    Exit;

  subject := 'dext.nats.nkey.' + FormatDateTime('hhnnsszzz', Now);
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        payload := AMsg.AsString;
        received.SetEvent;
      end);
    FClient.Publish(subject, 'nkey-ok');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(payload).Be('nkey-ok');
  finally
    received.Free;
  end;
end;

{ TDextNatsStressTests }

procedure TDextNatsStressTests.StabilizePingAfterForcedDisconnect;
var
  opts: TDextNatsOptions;
begin
  opts := FClient.Options;
  opts.MaxPingsOutstanding := 10;
  opts.PingIntervalMs := 120000;
  FClient.Options := opts;
end;

procedure TDextNatsStressTests.RecreateClientForStalePingReconnect(
  AReconnectWaitMs: Integer; AMaxPendingBufferBytes: Int64);
var
  opts: TDextNatsOptions;
begin
  if Assigned(FClient) then
  begin
    try
      FClient.Disconnect;
    except
    end;
    FreeAndNil(FClient);
  end;

  opts := TDextNatsOptions.CreateDefault;
  opts.AllowReconnect := True;
  opts.MaxReconnectAttempts := 20;
  opts.ReconnectWaitMs := AReconnectWaitMs;
  opts.PingIntervalMs := 120;
  opts.MaxPingsOutstanding := 0;
  opts.MaxPendingBufferBytes := AMaxPendingBufferBytes;
  opts.ConnectTimeoutMs := 5000;
  opts.RequestTimeoutMs := 5000;
  FClient := TDextNatsClient.Create(opts);
end;

function TDextNatsStressTests.TryConnectLiveOrSoftSkip: Boolean;
begin
  Result := False;
  if LiveSkippedByEnv then
    Exit;

  try
    FClient.Connect(NatsTestHost, NatsTestPort);
    Result := True;
  except
    on E: Exception do
      Result := LiveSoftSkipOrFail(
        Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
          'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [NatsTestHost, NatsTestPort, E.Message]));
  end;
end;

function TDextNatsStressTests.EnsureServerOrFail: Boolean;
begin
  Result := TryConnectLiveOrSoftSkip;
end;

procedure TDextNatsStressTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
end;

procedure TDextNatsStressTests.TearDown;
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

procedure TDextNatsStressTests.MultiSubscribe_ShouldDeliverIndependently;
var
  s1, s2: string;
  e1, e2: TEvent;
  p1, p2: string;
begin
  if not EnsureServerOrFail then
    Exit;
  s1 := 'dext.nats.stress.a.' + IntToHex(Random(MaxInt), 8);
  s2 := 'dext.nats.stress.b.' + IntToHex(Random(MaxInt), 8);
  e1 := TEvent.Create(nil, True, False, '');
  e2 := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(s1,
      procedure(const AMsg: TNatsMsg)
      begin
        p1 := AMsg.AsString;
        e1.SetEvent;
      end);
    FClient.Subscribe(s2,
      procedure(const AMsg: TNatsMsg)
      begin
        p2 := AMsg.AsString;
        e2.SetEvent;
      end);
    FClient.Publish(s1, 'A');
    FClient.Publish(s2, 'B');
    Should(e1.WaitFor(3000) = wrSignaled).BeTrue;
    Should(e2.WaitFor(3000) = wrSignaled).BeTrue;
    Should(p1).Be('A');
    Should(p2).Be('B');
  finally
    e1.Free;
    e2.Free;
  end;
end;

procedure TDextNatsStressTests.ConcurrentRequests_ShouldRoundTrip;
var
  subject: string;
  okCount: Integer;
  i: Integer;
  remaining: Integer;
  done: TEvent;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := 'dext.nats.stress.req.' + IntToHex(Random(MaxInt), 8);
  okCount := 0;
  remaining := 4;
  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
      if AMsg.HasReplyTo then
        FClient.Publish(AMsg.ReplyTo, 'r:' + AMsg.AsString);
    end);

  done := TEvent.Create(nil, True, False, '');
  try
    for i := 1 to 4 do
    begin
      var th := TThread.CreateAnonymousThread(
        procedure
        var
          reply: TNatsMsg;
        begin
          try
            reply := FClient.Request(subject, 'x', 3000);
            if reply.AsString.StartsWith('r:') then
              TInterlocked.Increment(okCount);
          finally
            if TInterlocked.Decrement(remaining) = 0 then
              done.SetEvent;
          end;
        end);
      th.FreeOnTerminate := True;
      th.Start;
    end;
    Should(done.WaitFor(8000) = wrSignaled).BeTrue;
    Should(okCount).Be(4);
  finally
    done.Free;
  end;
end;

procedure TDextNatsStressTests.RequestTimeout_LateReply_ShouldNotCrash;
var
  subject: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := 'dext.nats.stress.late.' + IntToHex(Random(MaxInt), 8);
  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
      Sleep(800);
      if AMsg.HasReplyTo then
        FClient.Publish(AMsg.ReplyTo, 'late');
    end);

  Should(
    procedure
    begin
      FClient.Request(subject, 'q', 200);
    end).Throw(EDextNatsTimeoutError);

  // Allow late reply to arrive after timeout / claim-gate release without AV.
  Sleep(1000);
  Should(FClient.Connected).BeTrue;
end;

procedure TDextNatsStressTests.StalePing_ShouldDisconnectAndReconnect;
var
  disconnected, reconnected: TEvent;
  sawDisconnect: Boolean;
begin
  if LiveSkippedByEnv then
    Exit;

  RecreateClientForStalePingReconnect(400, 8 * 1024 * 1024);
  sawDisconnect := False;
  disconnected := TEvent.Create(nil, True, False, '');
  reconnected := TEvent.Create(nil, True, False, '');
  try
    FClient.OnDisconnected :=
      procedure
      begin
        StabilizePingAfterForcedDisconnect;
        sawDisconnect := True;
        disconnected.SetEvent;
      end;
    FClient.OnConnected :=
      procedure(const AInfo: TNatsServerInfo; AIsReconnect: Boolean)
      begin
        if AIsReconnect then
          reconnected.SetEvent;
      end;

    try
      FClient.Connect(NatsTestHost, NatsTestPort);
    except
      on E: Exception do
      begin
        LiveSoftSkipOrFail(
          Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
            'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
            [NatsTestHost, NatsTestPort, E.Message]));
        Exit;
      end;
    end;

    Should(disconnected.WaitFor(5000) = wrSignaled).BeTrue;
    Should(sawDisconnect).BeTrue;
    Should(reconnected.WaitFor(10000) = wrSignaled).BeTrue;
    Should(FClient.Connected).BeTrue;
  finally
    disconnected.Free;
    reconnected.Free;
  end;
end;

procedure TDextNatsStressTests.PendingBuffer_ShouldRejectWhenFullDuringReconnect;
var
  rejected: Boolean;
  disconnected, done: TEvent;
  errText: string;
begin
  if LiveSkippedByEnv then
    Exit;

  // Tiny outbox + long reconnect wait so Publish during disconnect hits the ceiling.
  RecreateClientForStalePingReconnect(2500, 32);
  rejected := False;
  errText := '';
  disconnected := TEvent.Create(nil, True, False, '');
  done := TEvent.Create(nil, True, False, '');
  try
    FClient.OnDisconnected :=
      procedure
      begin
        StabilizePingAfterForcedDisconnect;
        disconnected.SetEvent;
        try
          FClient.Publish('dext.nats.stress.pending', StringOfChar('x', 128));
        except
          on E: EDextNatsException do
          begin
            rejected := True;
            errText := E.Message;
          end;
        end;
        done.SetEvent;
      end;

    try
      FClient.Connect(NatsTestHost, NatsTestPort);
    except
      on E: Exception do
      begin
        LiveSoftSkipOrFail(
          Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
            'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
            [NatsTestHost, NatsTestPort, E.Message]));
        Exit;
      end;
    end;

    Should(disconnected.WaitFor(5000) = wrSignaled).BeTrue;
    Should(done.WaitFor(2000) = wrSignaled).BeTrue;
    Should(rejected).BeTrue;
    Should(errText.Contains('reconnect buffer') or errText.Contains('Not connected')).BeTrue;
  finally
    disconnected.Free;
    done.Free;
  end;
end;

{ TDextNatsDiTests }

procedure TDextNatsDiTests.ClientOptions_ShouldDefaultHostAndPort;
var
  opts: TDextNatsOptions;
begin
  opts := TDextNatsOptions.CreateDefault;
  Should(opts.Host).Be('localhost');
  Should(opts.Port).Be(NATS_DEFAULT_PORT);
end;

procedure TDextNatsDiTests.AddNatsClient_ShouldResolveSingleton;
var
  Services: TDextServices;
  Provider: IServiceProvider;
  A, B: TDextNatsClient;
begin
  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap, '127.0.0.1', 4222);
  Provider := Services.BuildServiceProvider;
  A := TDextServices.GetRequiredServiceObject<TDextNatsClient>(Provider);
  B := TDextServices.GetRequiredServiceObject<TDextNatsClient>(Provider);
  Should(A <> nil).BeTrue;
  Should(Pointer(A) = Pointer(B)).BeTrue;
  Should(A.Connected).BeFalse;
  Should(A.Options.Host).Be('127.0.0.1');
  Should(A.Options.Port).Be(4222);
end;

procedure TDextNatsDiTests.AddNatsJetStream_ShouldResolveTransientBoundToSameClient;
var
  Services: TDextServices;
  Provider: IServiceProvider;
  Client: TDextNatsClient;
  Js1, Js2: TDextNatsJetStreamContext;
begin
  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap);
  AddNatsJetStream(Services.Unwrap);
  Provider := Services.BuildServiceProvider;
  Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(Provider);
  Js1 := TDextServices.GetRequiredServiceObject<TDextNatsJetStreamContext>(Provider);
  Js2 := TDextServices.GetRequiredServiceObject<TDextNatsJetStreamContext>(Provider);
  try
    Should(Js1 <> nil).BeTrue;
    Should(Js2 <> nil).BeTrue;
    Should(Pointer(Js1) <> Pointer(Js2)).BeTrue;
    Should(Pointer(Js1.Client) = Pointer(Client)).BeTrue;
    Should(Pointer(Js2.Client) = Pointer(Client)).BeTrue;
  finally
    // Transient instances are not owned by the provider the same way as singletons;
    // free what we resolved as transient. Singleton client is owned by the provider.
    Js1.Free;
    Js2.Free;
  end;
end;

procedure TDextNatsDiTests.AddNatsClient_ConfigureCallback_ShouldApplyOptions;
var
  Services: TDextServices;
  Provider: IServiceProvider;
  Client: TDextNatsClient;
begin
  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap,
    procedure(var AOptions: TDextNatsOptions)
    begin
      AOptions.Host := 'nats.example';
      AOptions.Port := 4229;
      AOptions.Name := 'di-test';
      AOptions.EnableMetrics := True;
    end);
  Provider := Services.BuildServiceProvider;
  Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(Provider);
  Should(Client.Options.Host).Be('nats.example');
  Should(Client.Options.Port).Be(4229);
  Should(Client.Options.Name).Be('di-test');
  Should(Client.Options.EnableMetrics).BeTrue;
end;

procedure TDextNatsDiTests.HealthCheck_ShouldReportUnhealthyWhenDisconnected;
var
  Services: TDextServices;
  Provider: IServiceProvider;
  Check: TNatsHealthCheck;
  Res: TNatsHealthResult;
begin
  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap);
  AddNatsHealthCheck(Services.Unwrap);
  Provider := Services.BuildServiceProvider;
  Check := TDextServices.GetRequiredServiceObject<TNatsHealthCheck>(Provider);
  try
    Res := Check.CheckHealth;
    Should(Ord(Res.Status)).Be(Ord(nhsUnhealthy));
    Should(Res.Description.Contains('disconnected')).BeTrue;
  finally
    Check.Free;
  end;
end;

{ TDextNatsObservabilityTests }

procedure TDextNatsObservabilityTests.Metrics_ShouldDefaultDisabled;
var
  Opts: TDextNatsOptions;
begin
  Opts := TDextNatsOptions.CreateDefault;
  Should(Opts.EnableMetrics).BeFalse;
end;

procedure TDextNatsObservabilityTests.Metrics_Publish_ShouldIncrementLocalCounter;
var
  Opts: TDextNatsOptions;
  Client: TDextNatsClient;
  Snap: TNatsClientMetrics;
  Flushed: string;
begin
  Opts := TDextNatsOptions.CreateDefault;
  Opts.EnableMetrics := True;
  TMetrics.Flush;
  Client := TDextNatsClient.Create(Opts);
  try
    Client.NotifyError('probe-error');
    Snap := Client.Metrics;
    Should(Snap.Errors).Be(1);
    Flushed := TMetrics.Flush;
    Should(Flushed.Contains(NATS_METRIC_ERRORS)).BeTrue;
  finally
    Client.Free;
  end;
end;

procedure TDextNatsObservabilityTests.Logger_FireError_ShouldRecordWhenAttached;
var
  Client: TDextNatsClient;
  Rec: TRecordingNatsLogger;
  Logger: ILogger;
begin
  Rec := TRecordingNatsLogger.Create;
  Logger := Rec;
  Client := TDextNatsClient.Create;
  try
    Client.Logger := Logger;
    Client.NotifyError('NATS server error: probe');
    Should(Rec.Contains('probe')).BeTrue;
    Should(Client.Metrics.Errors).Be(1);
  finally
    Client.Free;
    Logger := nil;
  end;
end;

end.
