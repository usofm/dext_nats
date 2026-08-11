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
  System.IOUtils,
  System.SyncObjs,
  System.Diagnostics,
  Dext.Collections,
  Dext.Collections.Dict,
  Dext.Core.Span,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.DI.Interfaces,
  Dext.DI.Core,
  Dext.Configuration.Core,
  Dext.Configuration.Interfaces,
  Dext.Logging,
  Dext.Telemetry.Metrics,
  Dext.Net.Security,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.NKeys,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.KeyValue,
  Dext.Net.Nats.ObjectStore,
  Dext.Net.Nats.Services,
  Dext.Net.Nats.DependencyInjection,
  Dext.Net.Nats.HealthChecks,
  Dext.Utils;

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
    procedure Parser_ShouldDecodeInfoAccountAndLimitFields;
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
    [Test, Category('Unit'), Category('Benchmark')]
    procedure Encode_MicroBenchmark_PubAndCachedPing;
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
    procedure ConsumerConfig_ShouldSerializeHeadersOnly;
    [Test, Category('Unit')]
    procedure ConsumerConfig_ShouldSerializeOrderedFields;
    [Test, Category('Unit')]
    procedure ConsumerConfig_ShouldSerializeFilterSubjects;
    [Test, Category('Unit')]
    procedure OrderedConsumerOptions_ShouldDefault;
    [Test, Category('Unit')]
    procedure Msg_PayloadSpan_ShouldViewOwnedBytes;
    [Test, Category('Unit')]
    procedure JsMsg_ShouldParseAckSubjectMetadata;
    [Test, Category('Unit')]
    procedure JsMsg_PayloadSpan_ShouldViewOwnedBytes;
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
    [Test, Category('Unit')]
    procedure StreamConfig_ShouldSerializeKvFlags;
    [Test, Category('Unit')]
    procedure StreamConfig_ShouldSerializeCompressionAndPlacement;
    [Test, Category('Unit')]
    procedure StreamConfig_ShouldSerializeMirrorSourcesRePublish;
    [Test, Category('Unit')]
    procedure StreamPurgeRequest_ShouldSerializeOptionalFields;
    [Test, Category('Unit')]
    procedure StreamInfo_ShouldParseCompressionAndPlacement;
    [Test, Category('Unit')]
    procedure StreamInfo_ShouldParseMirrorSourcesRePublish;
    [Test, Category('Unit')]
    procedure KeyValueConfig_ShouldMapToStreamConfig;
    [Test, Category('Unit')]
    procedure KeyValueConfig_FromStreamConfig_ShouldRoundTrip;
    [Test, Category('Unit')]
    procedure KeyValueConfig_LimitMarkerTTL_ShouldEnableMsgTTL;
    [Test, Category('Unit')]
    procedure KeyValue_ValidateTTL_ShouldRejectSubSecond;
    [Test, Category('Unit')]
    procedure StoredMsg_ShouldParseDataAndKvHeaders;
    [Test, Category('Unit')]
    procedure KeyValue_ValidateNames_ShouldRejectInvalid;
    [Test, Category('Unit'), Category('KeyValue'), Category('Negative')]
    procedure KeyValue_ValidateSearchKey_ShouldRejectBadWildcards;
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
    /// <summary>
    /// Intentional Disconnect must join RecvLoop/PingLoop without leaving a
    /// first-chance EDextSocketError (WSAEINTR 10004) on the receive thread.
    /// </summary>
    [Test, Category('Integration')]
    procedure Disconnect_ShouldJoinThreadsCleanly;
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
    procedure UnsubscribeSubject_ShouldCancelAllOnSubject;
    [Test, Category('Integration')]
    procedure Unsubscribe_MaxMsgs_ShouldAutoCancel;
    [Test, Category('Integration')]
    procedure Flush_ShouldRoundTrip;
    [Test, Category('Integration')]
    procedure HealthCheck_WithFlush_ShouldReportHealthyWhenLive;
    [Test, Category('Integration')]
    procedure Ping_ShouldBeAnsweredByFlush;
    [Test, Category('Integration')]
    procedure MaxPayload_ShouldRejectOversizedPublish;
    [Test, Category('Integration')]
    procedure RequestAsync_ShouldReplyAndTimeout;
    [Test, Category('Integration')]
    procedure RequestAsyncBuilder_ShouldAwaitReply;
    [Test, Category('Integration')]
    procedure RequestAsyncBuilder_Timeout_ShouldRaise;
    [Test, Category('Integration')]
    procedure FlushAsync_ShouldAwait;
    [Test, Category('Integration')]
    procedure Events_OnConnected_ShouldFire;
    [Test, Category('Integration')]
    procedure Events_OnDisconnected_ShouldFire;
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
    procedure OrderedConsumer_ShouldDeliverInOrder;
    [Test, Category('JetStream')]
    procedure Stream_CRUD_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Stream_List_ShouldIncludeCreatedStream;
    [Test, Category('JetStream')]
    procedure Stream_Update_ShouldChangeMaxMsgs;
    [Test, Category('JetStream')]
    procedure Publish_Dedup_ShouldMarkDuplicate;
    [Test, Category('JetStream')]
    procedure Consumer_CRUD_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Consumer_List_ShouldIncludeCreatedConsumer;
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
    [Test, Category('JetStream')]
    procedure Stream_CreateWithCompression_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Stream_CreateWithRePublish_ShouldRoundTrip;
    [Test, Category('JetStream')]
    procedure Stream_CreateWithMirror_ShouldRoundTrip;
  end;

  [TestFixture('NATS JetStream Key-Value (requires nats-server -js)')]
  TDextNatsKeyValueTests = class
  private
    FClient: TDextNatsClient;
    FJs: TDextNatsJetStreamContext;
    function EnsureJetStreamOrFail: Boolean;
    function UniqueBucket(const APrefix: string): string;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Bucket_CreatePutGetDelete_ShouldRoundTrip;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Bucket_Purge_ShouldHideKey;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure PurgeDeletes_ShouldRemoveMarkersWhenForced;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure BucketExists_Missing_ShouldBeFalse;
    [Test, Category('JetStream'), Category('KeyValue'), Category('Negative')]
    procedure Get_MissingKey_ShouldRaise;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Keys_ShouldListLiveKeysOnly;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure ListKeysFiltered_ShouldMatchWildcardAndMultiFilters;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure History_ShouldReturnRevisions;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure GetRevision_ShouldReturnHistoricalPutAndTombstone;
    [Test, Category('JetStream'), Category('KeyValue'), Category('Negative')]
    procedure GetRevision_WrongKeyOrMissing_ShouldRaise;
    [Test, Category('Unit'), Category('KeyValue')]
    procedure Entry_EndOfInitialMarker_ShouldNotBePut;
    [Test, Category('Unit'), Category('KeyValue')]
    procedure WatchOptions_ShouldDefaultFalse;
    [Test, Category('Unit'), Category('KeyValue'), Category('Negative')]
    procedure WatchOptions_IncludeHistoryWithUpdatesOnly_ShouldRaise;
    [Test, Category('Unit'), Category('KeyValue')]
    procedure PurgeDeletesOptions_ShouldDefaultZero;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_ShouldDeliverCurrentAndUpdates;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_ShouldSignalEndOfInitial;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_EmptyBucket_ShouldSignalEndOfInitial;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_UpdatesOnly_ShouldSkipInitialAndMarker;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_MetaOnly_ShouldOmitValues;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_IncludeHistory_ShouldReplayRevisions;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchAll_IgnoreDeletes_ShouldSkipDeleteMarkers;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Watch_ResumeFromRevision_ShouldSkipEarlierRevisions;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure WatchFiltered_ShouldMatchWildcardKeys;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Config_ShouldRoundTripFromStream;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Create_ShouldPutOnlyIfAbsent;
    [Test, Category('JetStream'), Category('KeyValue'), Category('Negative')]
    procedure Create_ExistingKey_ShouldRaiseKeyExists;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Create_AfterDelete_ShouldSucceed;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Update_ShouldSucceedWhenRevisionMatches;
    [Test, Category('JetStream'), Category('KeyValue'), Category('Negative')]
    procedure Update_WrongRevision_ShouldRaiseMismatch;
    [Test, Category('JetStream'), Category('KeyValue')]
    procedure Create_WithPerKeyTTL_ShouldExpire;
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
    /// <summary>
    ///   <c>TDextServices.BuildServiceProvider</c> assigns the first provider to
    ///   <c>DefaultProvider</c>. Clear it after each test so owned singletons
    ///   (e.g. <c>TDextNatsClient</c>) are not destroyed during RTL unit finalization
    ///   (Windows runtime error 216 / AV after the suite summary).
    /// </summary>
    [TearDown]
    procedure TearDown;
    [Test, Category('DI')]
    procedure AddNatsClient_ShouldResolveSingleton;
    [Test, Category('DI')]
    procedure AddNatsJetStream_ShouldResolveTransientBoundToSameClient;
    [Test, Category('DI')]
    procedure ClientOptions_ShouldDefaultHostAndPort;
    [Test, Category('DI')]
    procedure AddNatsClient_ConfigureCallback_ShouldApplyOptions;
    [Test, Category('DI')]
    procedure BindNatsOptions_FromConfiguration_ShouldMapHostPortTls;
    [Test, Category('DI')]
    procedure HealthCheck_ShouldReportUnhealthyWhenDisconnected;
    [Test, Category('DI')]
    procedure HealthCheck_Options_ShouldDefaultToConnectedOnly;
    [Test, Category('DI')]
    procedure HealthCheck_WithFlush_ShouldStayUnhealthyWhenDisconnected;
    [Test, Category('DI')]
    procedure AddNatsHealthCheck_WithFlushOptions_ShouldApplyTimeout;
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

  [TestFixture('NATS Services API ($SRV.*)')]
  TDextNatsServicesTests = class
  private
    FClient: TDextNatsClient;
    function EnsureServerOrFail: Boolean;
    function UniqueServiceName(const APrefix: string): string;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('Unit'), Category('Services')]
    procedure ControlSubject_ShouldBuildAllKindAndInstance;
    [Test, Category('Unit'), Category('Services')]
    procedure ControlSubject_IdWithoutName_ShouldRaise;
    [Test, Category('Unit'), Category('Services')]
    procedure NameAndSemVer_ShouldValidate;
    [Test, Category('Unit'), Category('Services')]
    procedure Subject_ShouldRejectSpacesAndMisplacedGt;
    [Test, Category('Unit'), Category('Services')]
    procedure Config_InvalidName_ShouldRaise;
    [Test, Category('Unit'), Category('Services')]
    procedure PingJson_ShouldIncludeTypeAndIdentity;
    [Test, Category('Unit'), Category('Services')]
    procedure JoinSubject_ShouldPrefixAndAllowEmpty;
    [Test, Category('Unit'), Category('Services')]
    procedure AddGroup_ShouldPrefixEndpointSubjectsInInfo;
    [Test, Category('Unit'), Category('Services')]
    procedure GroupConfig_InvalidPrefix_ShouldRaise;
    [Test, Category('Integration'), Category('Services')]
    procedure AddService_PingDiscovery_ShouldRespond;
    [Test, Category('Integration'), Category('Services')]
    procedure Endpoint_ShouldEchoAndStopUnsubscribes;
    [Test, Category('Integration'), Category('Services')]
    procedure AddGroup_NestedEndpoint_ShouldRespond;
  end;

  [TestFixture('NATS JetStream Object Store (requires nats-server -js)')]
  TDextNatsObjectStoreTests = class
  private
    FClient: TDextNatsClient;
    FOs: TDextNatsObjectStoreContext;
    function EnsureJetStreamOrFail: Boolean;
    function UniqueBucket(const APrefix: string): string;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('Unit'), Category('ObjectStore')]
    procedure ObjectInfo_ParseToJson_ShouldRoundTrip;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure ObjectInfo_Link_ParseToJson_ShouldRoundTrip;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure ObjectInfo_EndOfInitialMarker_ShouldBeEmpty;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure WatchOptions_ShouldDefaultFalse;
    [Test, Category('Unit'), Category('ObjectStore'), Category('Negative')]
    procedure WatchOptions_IncludeHistoryWithUpdatesOnly_ShouldRaise;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure GetOptions_ShouldDefaultShowDeletedFalse;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure ListOptions_ShouldDefaultShowDeletedFalse;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure ObjectStoreConfig_ShouldMapToStreamConfig;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Store_CreatePutGetDelete_ShouldRoundTrip;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure GetInfo_ShowDeleted_ShouldReturnTombstone;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Get_ShowDeleted_ShouldReturnEmptyAfterDelete;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Put_AfterDelete_ShouldOverwrite;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure UpdateStore_ShouldChangeDescriptionMaxBytesAndTTL;
    [Test, Category('JetStream'), Category('ObjectStore'), Category('Negative')]
    procedure UpdateStore_MissingBucket_ShouldRaise;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Store_PutGet_StreamAndFile_ShouldRoundTrip;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure GetResult_ShouldStreamChunksLazily;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure GetResult_ShowDeleted_ShouldEofEmpty;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Store_PutOverwrite_ShouldReturnLatest;
    [Test, Category('JetStream'), Category('ObjectStore'), Category('Negative')]
    procedure Get_MissingObject_ShouldRaise;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Store_ListAndKeys_ShouldReturnLiveObjects;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Store_List_EmptyBucket_ShouldReturnEmpty;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_ShouldDeliverCurrentAndUpdates;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_ShouldSignalEndOfInitial;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_EmptyBucket_ShouldSignalEndOfInitial;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_UpdatesOnly_ShouldSkipInitialAndMarker;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_MetaOnly_ShouldOmitPayload;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_IgnoreDeletes_ShouldSkipDeleted;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure WatchAll_IncludeHistory_ShouldReplayMeta;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure UpdateMeta_ShouldChangeDescriptionHeadersAndRename;
    [Test, Category('JetStream'), Category('ObjectStore'), Category('Negative')]
    procedure UpdateMeta_DeletedOrConflict_ShouldRaise;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure Seal_ShouldRejectFurtherMutations;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure AddLink_Get_ShouldFollowSameBucket;
    [Test, Category('JetStream'), Category('ObjectStore')]
    procedure AddLink_CrossBucket_Get_ShouldFollow;
    [Test, Category('JetStream'), Category('ObjectStore'), Category('Negative')]
    procedure AddBucketLink_Get_ShouldRaise;
    [Test, Category('JetStream'), Category('ObjectStore'), Category('Negative')]
    procedure AddLink_DeletedOrLinkTarget_ShouldRaise;
  end;

  /// <summary>
  /// Opt-in throughput harness (CPU encode + live pub/sub). Soft-skips unless
  /// <c>DEXT_NATS_RUN_BENCH=1</c>; live path soft-skips without nats-server.
  /// Not a CI perf gate — reports msgs/sec / ops/sec for local comparison.
  /// </summary>
  [TestFixture('NATS Benchmark')]
  TDextNatsBenchmarkTests = class
  private
    FClient: TDextNatsClient;
    function EnsureServerOrFail: Boolean;
    function TryConnectLiveOrSoftSkip: Boolean;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('Benchmark'), Explicit('Set DEXT_NATS_RUN_BENCH=1')]
    procedure Encode_Throughput_ShouldReportOpsPerSec;
    [Test, Category('Benchmark'), Explicit('Set DEXT_NATS_RUN_BENCH=1')]
    procedure PubSub_Throughput_ShouldReportMsgsPerSec;
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

function NatsBenchEnabled: Boolean;
begin
  Result := EnvFlagTrue('DEXT_NATS_RUN_BENCH');
end;

function NatsStressEnabled: Boolean;
begin
  Result := EnvFlagTrue('DEXT_NATS_RUN_STRESS');
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

procedure TDextNatsProtocolTests.Parser_ShouldDecodeInfoAccountAndLimitFields;
var
  info: TNatsServerInfo;
begin
  { Wire keys from modern nats-server Info JSON — account/domain/limit-ish surface. }
  info := TNatsServerInfo.Parse(
    '{"server_id":"NACC","server_name":"s1","version":"2.11.0","proto":1,' +
    '"go":"go1.22.5","git_commit":"abcdef0","host":"0.0.0.0","port":4222,' +
    '"ip":"10.0.0.9","headers":true,"max_payload":2097152,"client_id":42,' +
    '"client_ip":"203.0.113.10","auth_required":true,"tls_required":false,' +
    '"tls_verify":true,"tls_available":true,"jetstream":true,"api_lvl":1,' +
    '"cluster":"c1","cluster_dynamic":true,"domain":"tenant-a",' +
    '"remote_account":"ACC_ORDERS","acc_is_sys":false,"ldm":true,' +
    '"connect_urls":["nats://a:4222"],' +
    '"ws_connect_urls":["wss://a:8443"]}');
  Should(info.ServerId).Be('NACC');
  Should(info.GitCommit).Be('abcdef0');
  Should(info.Ip).Be('10.0.0.9');
  Should(info.MaxPayload).Be(2097152);
  Should(info.ClientId).Be(42);
  Should(info.ClientIp).Be('203.0.113.10');
  Should(info.TlsVerify).BeTrue;
  Should(info.JsApiLevel).Be(1);
  Should(info.Cluster).Be('c1');
  Should(info.ClusterDynamic).BeTrue;
  Should(info.Domain).Be('tenant-a');
  Should(info.RemoteAccount).Be('ACC_ORDERS');
  Should(info.IsSystemAccount).BeFalse;
  Should(info.LameDuckMode).BeTrue;
  Should(Length(info.ConnectUrls)).Be(1);
  Should(info.ConnectUrls[0]).Be('nats://a:4222');
  Should(Length(info.WsConnectUrls)).Be(1);
  Should(info.WsConnectUrls[0]).Be('wss://a:8443');
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

procedure TDextNatsProtocolTests.Encode_MicroBenchmark_PubAndCachedPing;
const
  Iterations = 40000;
var
  i: Integer;
  payload, frame, ping1, ping2: TBytes;
  sw: TStopwatch;
  ms: Int64;
  opsPerSec: Double;
begin
  // Cached PING/PONG: same dynamic-array reference (no per-call allocation).
  ping1 := NatsEncodePing;
  ping2 := NatsEncodePing;
  Should(Pointer(ping1) = Pointer(ping2)).BeTrue;
  Should(Pointer(NatsEncodePong) = Pointer(NatsEncodePong)).BeTrue;

  payload := BytesOfUtf8('bench-payload');
  sw := TStopwatch.StartNew;
  frame := nil;
  for i := 1 to Iterations do
    frame := NatsEncodePub('bench.subject', '', payload);
  sw.Stop;
  ms := sw.ElapsedMilliseconds;
  if ms < 1 then
    ms := 1;
  opsPerSec := Iterations * 1000.0 / ms;
  SafeWriteLn(Format(
    'BENCH Encode_MicroBenchmark_PubAndCachedPing: %d pubs in %d ms = %.0f ops/sec',
    [Iterations, ms, opsPerSec]));

  Should(Utf8OfBytes(frame).StartsWith('PUB bench.subject ')).BeTrue;
  Should(Utf8OfBytes(frame).Contains('bench-payload')).BeTrue;
  // Generous ceiling for CI VMs; local machines are typically well under 500ms.
  Should(ms < 5000).BeTrue;
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

procedure TDextNatsProtocolTests.ConsumerConfig_ShouldSerializeHeadersOnly;
var
  cfg: TNatsConsumerConfig;
  json: string;
begin
  cfg := TNatsConsumerConfig.CreateDefault('C2', 's.*');
  cfg.HeadersOnly := True;
  json := cfg.ToJson;
  Should(json.Contains('"headers_only":true')).BeTrue;

  cfg := TNatsConsumerConfig.CreateDefault('C3', 's.*');
  json := cfg.ToJson;
  Should(json.Contains('headers_only')).BeFalse;
end;

procedure TDextNatsProtocolTests.ConsumerConfig_ShouldSerializeOrderedFields;
var
  cfg: TNatsConsumerConfig;
  json: string;
begin
  cfg := TNatsConsumerConfig.CreateDefault;
  cfg.Name := 'ord_1';
  cfg.DeliverSubject := '_INBOX.ordered.1';
  cfg.AckPolicy := apNone;
  cfg.MaxAckPending := 0;
  cfg.MaxDeliver := 1;
  cfg.FlowControl := True;
  cfg.IdleHeartbeat := Int64(5) * 1000000000;
  cfg.InactiveThreshold := Int64(5) * 60 * 1000000000;
  cfg.MemoryStorage := True;
  cfg.NumReplicas := 1;
  json := cfg.ToJson;
  Should(json.Contains('"flow_control":true')).BeTrue;
  Should(json.Contains('"idle_heartbeat":5000000000')).BeTrue;
  Should(json.Contains('"inactive_threshold":300000000000')).BeTrue;
  Should(json.Contains('"mem_storage":true')).BeTrue;
  Should(json.Contains('"num_replicas":1')).BeTrue;
  Should(json.Contains('"ack_policy":"none"')).BeTrue;
  Should(json.Contains('"max_waiting"')).BeFalse;

  cfg := TNatsConsumerConfig.CreateDefault('pull', 's.*');
  json := cfg.ToJson;
  Should(json.Contains('flow_control')).BeFalse;
  Should(json.Contains('idle_heartbeat')).BeFalse;
  Should(json.Contains('mem_storage')).BeFalse;
  Should(json.Contains('num_replicas')).BeFalse;
end;

procedure TDextNatsProtocolTests.ConsumerConfig_ShouldSerializeFilterSubjects;
var
  cfg: TNatsConsumerConfig;
  json: string;
begin
  cfg := TNatsConsumerConfig.CreateDefault;
  cfg.Name := 'multi';
  cfg.FilterSubject := 'ignored.when.array.set';
  cfg.FilterSubjects := ['$KV.B.a.*', '$KV.B.b.>'];
  json := cfg.ToJson;
  Should(json.Contains('"filter_subjects":["$KV.B.a.*","$KV.B.b.>"]')).BeTrue;
  Should(json.Contains('"filter_subject"')).BeFalse;
end;

procedure TDextNatsProtocolTests.OrderedConsumerOptions_ShouldDefault;
var
  opts: TNatsOrderedConsumerOptions;
begin
  opts := TNatsOrderedConsumerOptions.CreateDefault('orders.*');
  Should(opts.FilterSubject).Be('orders.*');
  Should(Ord(opts.DeliverPolicy)).Be(Ord(dpAll));
  Should(opts.OptStartSeq).Be(UInt64(0));
  Should(opts.HeadersOnly).BeFalse;
  Should(opts.NamePrefix).Be('');
  Should(opts.IdleHeartbeat).Be(Int64(0));
  Should(opts.InactiveThreshold).Be(Int64(0));
  Should(opts.MaxResetAttempts).Be(0);
  Should(Assigned(opts.OnError)).BeFalse;
end;

procedure TDextNatsProtocolTests.Msg_PayloadSpan_ShouldViewOwnedBytes;
var
  msg: TNatsMsg;
  span: TByteSpan;
  kept: TBytes;
begin
  msg := Default(TNatsMsg);
  span := msg.PayloadSpan;
  Should(span.Length).Be(0);
  Should(NativeInt(span.Data)).Be(0);

  msg.Payload := BytesOfUtf8('hello-span');
  span := msg.PayloadSpan;
  Should(span.Length).Be(Length(msg.Payload));
  Should(NativeInt(span.Data)).Be(NativeInt(@msg.Payload[0]));
  Should(span.EqualsString('hello-span')).BeTrue;
  Should(Utf8OfBytes(span.ToBytes)).Be('hello-span');

  { Lifetime: keeping Payload (TBytes) keeps the bytes alive; the span alone must not be stored. }
  kept := msg.Payload;
  span := TByteSpan.FromBytes(kept);
  SetLength(msg.Payload, 0);
  Should(span.EqualsString('hello-span')).BeTrue;
  Should(Utf8OfBytes(kept)).Be('hello-span');
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

procedure TDextNatsProtocolTests.JsMsg_PayloadSpan_ShouldViewOwnedBytes;
var
  js: TNatsJsMsg;
  span: TByteSpan;
begin
  js := Default(TNatsJsMsg);
  js.Payload := BytesOfUtf8('js-body');
  span := js.PayloadSpan;
  Should(span.Length).Be(Length(js.Payload));
  Should(NativeInt(span.Data)).Be(NativeInt(@js.Payload[0]));
  Should(span.Equals(TByteSpan.FromBytes(js.Payload))).BeTrue;
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
    '{"config":{"name":"S1","subjects":["s.>"],"storage":"memory","discard":"new",' +
    '"allow_rollup_hdrs":true,"sealed":true},"state":{"messages":3,"bytes":30,' +
    '"first_seq":1,"last_seq":3,"consumer_count":2}}');
  Should(info.Name).Be('S1');
  Should(info.Config.Name).Be('S1');
  Should(Length(info.Config.Subjects)).Be(1);
  Should(info.Config.Subjects[0]).Be('s.>');
  Should(info.Config.Storage = ssMemory).BeTrue;
  Should(info.Config.Discard = sdNew).BeTrue;
  Should(info.Config.AllowRollup).BeTrue;
  Should(info.Config.Sealed).BeTrue;
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

procedure TDextNatsProtocolTests.StreamConfig_ShouldSerializeKvFlags;
var
  cfg: TNatsStreamConfig;
  json: string;
begin
  cfg := TNatsStreamConfig.CreateDefault('KV_DEMO', ['$KV.DEMO.>']);
  cfg.MaxMsgsPerSubject := 5;
  cfg.AllowDirect := True;
  cfg.DenyDelete := True;
  cfg.AllowRollup := True;
  cfg.AllowMsgTTL := True;
  cfg.SubjectDeleteMarkerTTL := 2000000000;
  cfg.Sealed := True;
  cfg.Discard := sdNew;
  cfg.Description := 'kv demo';
  json := cfg.ToJson;
  Should(json.Contains('"max_msgs_per_subject":5')).BeTrue;
  Should(json.Contains('"allow_direct":true')).BeTrue;
  Should(json.Contains('"deny_delete":true')).BeTrue;
  Should(json.Contains('"allow_rollup_hdrs":true')).BeTrue;
  Should(json.Contains('"sealed":true')).BeTrue;
  Should(json.Contains('"allow_msg_ttl":true')).BeTrue;
  Should(json.Contains('"subject_delete_marker_ttl":2000000000')).BeTrue;
  Should(json.Contains('"discard":"new"')).BeTrue;
  Should(json.Contains('"description":"kv demo"')).BeTrue;
end;

procedure TDextNatsProtocolTests.StreamConfig_ShouldSerializeCompressionAndPlacement;
var
  cfg: TNatsStreamConfig;
  json: string;
begin
  cfg := TNatsStreamConfig.CreateDefault('COMP', ['comp.>']);
  json := cfg.ToJson;
  Should(json.Contains('"compression"')).BeFalse;
  Should(json.Contains('"placement"')).BeFalse;
  Should(cfg.Placement.IsSet).BeFalse;

  cfg.Compression := scS2;
  cfg.Placement.Cluster := 'east';
  cfg.Placement.Tags := ['region:us-east', 'disk:ssd'];
  json := cfg.ToJson;
  Should(json.Contains('"compression":"s2"')).BeTrue;
  Should(json.Contains('"placement":{"cluster":"east","tags":["region:us-east","disk:ssd"]}')).BeTrue;
  Should(cfg.Placement.IsSet).BeTrue;
end;

procedure TDextNatsProtocolTests.StreamConfig_ShouldSerializeMirrorSourcesRePublish;
var
  cfg: TNatsStreamConfig;
  json: string;
  xform: TNatsSubjectTransform;
begin
  cfg := TNatsStreamConfig.CreateDefault('DEST', ['dest.>']);
  json := cfg.ToJson;
  Should(json.Contains('"mirror"')).BeFalse;
  Should(json.Contains('"sources"')).BeFalse;
  Should(json.Contains('"republish"')).BeFalse;
  Should(json.Contains('"mirror_direct"')).BeFalse;
  Should(cfg.Mirror.IsSet).BeFalse;
  Should(cfg.RePublish.IsSet).BeFalse;

  cfg.Mirror.Name := 'ORIGIN';
  cfg.Mirror.FilterSubject := 'orders.>';
  cfg.Mirror.OptStartSeq := 7;
  cfg.Mirror.OptStartTime := '2024-01-02T03:04:05Z';
  cfg.Mirror.ExternalStream.ApiPrefix := '$JS.other.API';
  cfg.Mirror.ExternalStream.DeliverPrefix := 'deliver.prefix';
  cfg.MirrorDirect := True;
  xform.Source := 'a.>';
  xform.Destination := 'b.>';
  cfg.Sources := [Default(TNatsStreamSource)];
  cfg.Sources[0].Name := 'S1';
  cfg.Sources[0].SubjectTransforms := [xform];
  cfg.RePublish.Source := 'foo.>';
  cfg.RePublish.Destination := 'bar.>';
  cfg.RePublish.HeadersOnly := True;
  json := cfg.ToJson;
  Should(json.Contains(
    '"mirror":{"name":"ORIGIN","opt_start_seq":7,"opt_start_time":"2024-01-02T03:04:05Z",' +
    '"filter_subject":"orders.>","external":{"api":"$JS.other.API","deliver":"deliver.prefix"}}')).BeTrue;
  Should(json.Contains('"mirror_direct":true')).BeTrue;
  Should(json.Contains(
    '"sources":[{"name":"S1","subject_transforms":[{"src":"a.>","dest":"b.>"}]}]')).BeTrue;
  Should(json.Contains(
    '"republish":{"src":"foo.>","dest":"bar.>","headers_only":true}')).BeTrue;
end;

procedure TDextNatsProtocolTests.StreamPurgeRequest_ShouldSerializeOptionalFields;
var
  req: TNatsStreamPurgeRequest;
  json: string;
begin
  req := TNatsStreamPurgeRequest.CreateDefault;
  json := req.ToJson;
  Should(json).Be('{}');

  req.Subject := '$KV.DEMO.gone';
  json := req.ToJson;
  Should(json.Contains('"subject":"$KV.DEMO.gone"')).BeTrue;
  Should(json.Contains('"keep"')).BeFalse;
  Should(json.Contains('"seq"')).BeFalse;

  req.Keep := 1;
  req.Sequence := 42;
  json := req.ToJson;
  Should(json.Contains('"subject":"$KV.DEMO.gone"')).BeTrue;
  Should(json.Contains('"keep":1')).BeTrue;
  Should(json.Contains('"seq":42')).BeTrue;
end;

procedure TDextNatsProtocolTests.StreamInfo_ShouldParseCompressionAndPlacement;
var
  info: TNatsStreamInfo;
begin
  info := TNatsStreamInfo.Parse(
    '{"config":{"name":"S2","subjects":["s.>"],"compression":"s2",' +
    '"placement":{"cluster":"west","tags":["az:1","ssd"]}},' +
    '"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}');
  Should(info.Config.Name).Be('S2');
  Should(Ord(info.Config.Compression)).Be(Ord(scS2));
  Should(info.Config.Placement.Cluster).Be('west');
  Should(Length(info.Config.Placement.Tags)).Be(2);
  Should(info.Config.Placement.Tags[0]).Be('az:1');
  Should(info.Config.Placement.Tags[1]).Be('ssd');
  Should(info.Config.Placement.IsSet).BeTrue;

  info := TNatsStreamInfo.Parse(
    '{"config":{"name":"NONE","subjects":["n.>"],"compression":"none"},' +
    '"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}');
  Should(Ord(info.Config.Compression)).Be(Ord(scNone));
  Should(info.Config.Placement.IsSet).BeFalse;
end;

procedure TDextNatsProtocolTests.StreamInfo_ShouldParseMirrorSourcesRePublish;
var
  info: TNatsStreamInfo;
begin
  info := TNatsStreamInfo.Parse(
    '{"config":{"name":"DEST","subjects":[],' +
    '"mirror":{"name":"ORIGIN","opt_start_seq":3,"opt_start_time":"2024-06-01T00:00:00Z",' +
    '"filter_subject":"x.>","external":{"api":"$JS.X.API","deliver":"d.pre"}},' +
    '"mirror_direct":true,' +
    '"sources":[{"name":"S1","subject_transforms":[{"src":"in.>","dest":"out.>"}]}],' +
    '"republish":{"src":"a.>","dest":"b.>","headers_only":true}},' +
    '"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}');
  Should(info.Config.Name).Be('DEST');
  Should(info.Config.Mirror.IsSet).BeTrue;
  Should(info.Config.Mirror.Name).Be('ORIGIN');
  Should(info.Config.Mirror.OptStartSeq).Be(UInt64(3));
  Should(info.Config.Mirror.OptStartTime).Be('2024-06-01T00:00:00Z');
  Should(info.Config.Mirror.FilterSubject).Be('x.>');
  Should(info.Config.Mirror.ExternalStream.ApiPrefix).Be('$JS.X.API');
  Should(info.Config.Mirror.ExternalStream.DeliverPrefix).Be('d.pre');
  Should(info.Config.MirrorDirect).BeTrue;
  Should(Length(info.Config.Sources)).Be(1);
  Should(info.Config.Sources[0].Name).Be('S1');
  Should(Length(info.Config.Sources[0].SubjectTransforms)).Be(1);
  Should(info.Config.Sources[0].SubjectTransforms[0].Source).Be('in.>');
  Should(info.Config.Sources[0].SubjectTransforms[0].Destination).Be('out.>');
  Should(info.Config.RePublish.IsSet).BeTrue;
  Should(info.Config.RePublish.Source).Be('a.>');
  Should(info.Config.RePublish.Destination).Be('b.>');
  Should(info.Config.RePublish.HeadersOnly).BeTrue;
end;

procedure TDextNatsProtocolTests.KeyValueConfig_ShouldMapToStreamConfig;
var
  kv: TNatsKeyValueConfig;
  stream: TNatsStreamConfig;
  json: string;
begin
  kv := TNatsKeyValueConfig.CreateDefault('INVENTORY');
  kv.History := 10;
  kv.Storage := ssMemory;
  stream := kv.ToStreamConfig;
  Should(stream.Name).Be('KV_INVENTORY');
  Should(Length(stream.Subjects)).Be(1);
  Should(stream.Subjects[0]).Be('$KV.INVENTORY.>');
  Should(stream.MaxMsgsPerSubject).Be(10);
  Should(stream.AllowDirect).BeTrue;
  Should(stream.DenyDelete).BeTrue;
  Should(stream.AllowRollup).BeTrue;
  Should(stream.AllowMsgTTL).BeFalse;
  Should(stream.SubjectDeleteMarkerTTL).Be(0);
  Should(Ord(stream.Discard)).Be(Ord(sdNew));
  Should(Ord(stream.Storage)).Be(Ord(ssMemory));
  Should(Ord(stream.Compression)).Be(Ord(scNone));
  Should(stream.Placement.IsSet).BeFalse;
  json := stream.ToJson;
  Should(json.Contains('"compression"')).BeFalse;
  Should(json.Contains('"placement"')).BeFalse;

  kv.Compression := scS2;
  kv.Placement.Cluster := 'east';
  kv.Placement.Tags := ['kv:hot'];
  stream := kv.ToStreamConfig;
  Should(Ord(stream.Compression)).Be(Ord(scS2));
  Should(stream.Placement.IsSet).BeTrue;
  json := stream.ToJson;
  Should(json.Contains('"compression":"s2"')).BeTrue;
  Should(json.Contains('"placement":{"cluster":"east","tags":["kv:hot"]}')).BeTrue;

  kv := TNatsKeyValueConfig.CreateDefault('MIRROR_SRC');
  kv.Mirror.Name := 'KV_ORIGIN';
  kv.RePublish.Destination := 'kv.out.>';
  stream := kv.ToStreamConfig;
  Should(stream.Mirror.Name).Be('KV_ORIGIN');
  Should(stream.MirrorDirect).BeTrue;
  Should(Length(stream.Subjects)).Be(0);
  Should(stream.RePublish.Destination).Be('kv.out.>');
  json := stream.ToJson;
  Should(json.Contains('"mirror":{"name":"KV_ORIGIN"}')).BeTrue;
  Should(json.Contains('"mirror_direct":true')).BeTrue;
  Should(json.Contains('"republish":{"dest":"kv.out.>"}')).BeTrue;
end;

procedure TDextNatsProtocolTests.KeyValueConfig_FromStreamConfig_ShouldRoundTrip;
var
  kv, back: TNatsKeyValueConfig;
  stream: TNatsStreamConfig;
begin
  kv := TNatsKeyValueConfig.CreateDefault('ROUND');
  kv.Description := 'inventory';
  kv.History := 7;
  kv.MaxBytes := 1024 * 1024;
  kv.MaxValueSize := 4096;
  kv.TTL := 60 * NATS_KV_MIN_TTL_NANOS;
  kv.LimitMarkerTTL := 5 * NATS_KV_MIN_TTL_NANOS;
  kv.Storage := ssMemory;
  kv.NumReplicas := 1;
  kv.Compression := scS2;
  kv.Placement.Cluster := 'west';
  kv.Placement.Tags := ['kv'];
  kv.Mirror.Name := 'KV_UPSTREAM';
  kv.Sources := [Default(TNatsStreamSource)];
  kv.Sources[0].Name := 'KV_OTHER';
  kv.RePublish.Destination := 'mirror.out.>';
  stream := kv.ToStreamConfig;
  back := TNatsKeyValueConfig.FromStreamConfig('ROUND', stream);
  Should(back.Bucket).Be('ROUND');
  Should(back.Description).Be('inventory');
  Should(back.History).Be(7);
  Should(back.MaxBytes).Be(1024 * 1024);
  Should(back.MaxValueSize).Be(4096);
  Should(back.TTL).Be(60 * NATS_KV_MIN_TTL_NANOS);
  Should(back.LimitMarkerTTL).Be(5 * NATS_KV_MIN_TTL_NANOS);
  Should(Ord(back.Storage)).Be(Ord(ssMemory));
  Should(back.NumReplicas).Be(1);
  Should(Ord(back.Compression)).Be(Ord(scS2));
  Should(back.Placement.Cluster).Be('west');
  Should(Length(back.Placement.Tags)).Be(1);
  Should(back.Placement.Tags[0]).Be('kv');
  Should(back.Mirror.Name).Be('KV_UPSTREAM');
  Should(Length(back.Sources)).Be(1);
  Should(back.Sources[0].Name).Be('KV_OTHER');
  Should(back.RePublish.Destination).Be('mirror.out.>');
end;

procedure TDextNatsProtocolTests.KeyValueConfig_LimitMarkerTTL_ShouldEnableMsgTTL;
var
  kv: TNatsKeyValueConfig;
  stream: TNatsStreamConfig;
  json: string;
begin
  kv := TNatsKeyValueConfig.CreateDefault('TTLBUCKET');
  kv.History := 1;
  kv.LimitMarkerTTL := 5 * NATS_KV_MIN_TTL_NANOS; { 5s }
  stream := kv.ToStreamConfig;
  Should(stream.AllowMsgTTL).BeTrue;
  Should(stream.SubjectDeleteMarkerTTL).Be(5 * NATS_KV_MIN_TTL_NANOS);
  json := stream.ToJson;
  Should(json.Contains('"allow_msg_ttl":true')).BeTrue;
  Should(json.Contains('"subject_delete_marker_ttl":5000000000')).BeTrue;
end;

procedure TDextNatsProtocolTests.KeyValue_ValidateTTL_ShouldRejectSubSecond;
begin
  Should(
    procedure
    var
      kv: TNatsKeyValueConfig;
    begin
      kv := TNatsKeyValueConfig.CreateDefault('TTLBAD');
      kv.LimitMarkerTTL := NATS_KV_MIN_TTL_NANOS div 2;
      kv.ToStreamConfig;
    end).Throw(EDextNatsKeyValueError);
end;

procedure TDextNatsProtocolTests.StoredMsg_ShouldParseDataAndKvHeaders;
var
  msg: TNatsStoredMsg;
  json: string;
begin
  { data = "hello" (aGVsbG8=); hdrs = NATS/1.0 + KV-Operation: DEL }
  json := '{"message":{"subject":"$KV.DEMO.widget","seq":7,"data":"aGVsbG8=",' +
    '"hdrs":"TkFUUy8xLjANCktWLU9wZXJhdGlvbjogREVMDQoNCg==",' +
    '"time":"2024-01-01T00:00:00.000000000Z"}}';
  msg := TNatsStoredMsg.Parse(json);
  Should(msg.Subject).Be('$KV.DEMO.widget');
  Should(Int64(msg.Sequence)).Be(7);
  Should(TEncoding.UTF8.GetString(msg.Data)).Be('hello');
  Should(msg.Headers.GetValue(NATS_KV_OP_HEADER)).Be(NATS_KV_OP_DEL);
  Should(msg.TimeStamp.Contains('2024-01-01')).BeTrue;

  Should(
    procedure
    begin
      TNatsStoredMsg.Parse(
        '{"error":{"code":404,"err_code":10037,"description":"message not found"}}');
    end).Throw(EDextNatsJetStreamError);
end;

procedure TDextNatsProtocolTests.KeyValue_ValidateNames_ShouldRejectInvalid;
begin
  Should(
    procedure
    begin
      TNatsKeyValueConfig.CreateDefault('bad.bucket').ToStreamConfig;
    end).Throw(EDextNatsKeyValueError);

  Should(
    procedure
    var
      kv: TDextNatsKeyValue;
    begin
      kv := TDextNatsKeyValue.Create(nil, 'x');
      kv.Free;
    end).Throw(EDextNatsKeyValueError);
end;

procedure TDextNatsProtocolTests.KeyValue_ValidateSearchKey_ShouldRejectBadWildcards;
var
  client: TDextNatsClient;
  js: TDextNatsJetStreamContext;
  kv: TDextNatsKeyValue;
  handler: TNatsKeyValueWatchHandler;
begin
  { ValidateSearchKey runs before StartWatch I/O — no server required. }
  client := TDextNatsClient.Create;
  js := TDextNatsJetStreamContext.Create(client);
  kv := TDextNatsKeyValue.Create(js, 'FILTERS');
  handler := procedure(const AEntry: TNatsKeyValueEntry)
    begin
    end;
  try
    Should(
      procedure
      begin
        kv.WatchFiltered(['a>b'], handler);
      end).Throw(EDextNatsKeyValueError);
    Should(
      procedure
      begin
        kv.WatchFiltered(['.leading'], handler);
      end).Throw(EDextNatsKeyValueError);
    Should(
      procedure
      begin
        kv.WatchFiltered(['trailing.'], handler);
      end).Throw(EDextNatsKeyValueError);
    Should(
      procedure
      begin
        kv.WatchFiltered(['has..dot'], handler);
      end).Throw(EDextNatsKeyValueError);
    { ListKeysFiltered shares ValidateSearchKey (no server I/O on reject). }
    Should(
      procedure
      begin
        kv.ListKeysFiltered(['a>b']);
      end).Throw(EDextNatsKeyValueError);
    Should(
      procedure
      begin
        kv.Keys(['.leading']);
      end).Throw(EDextNatsKeyValueError);
  finally
    kv.Free;
    js.Free;
    client.Free;
  end;
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
  { Soft live checks on enriched INFO surface (fields optional per server build). }
  Should(FClient.ServerInfo.MaxPayload).BeGreaterThan(0);
  Should(FClient.ServerInfo.Version).NotBeEmpty;
end;

procedure TDextNatsIntegrationTests.Disconnect_ShouldJoinThreadsCleanly;
var
  i: Integer;
begin
  if not EnsureServerOrFail then
    Exit;
  // Repeated connect/disconnect exercises the recv-thread vs closesocket race
  // that previously raised EDextSocketError 10004 (WSAEINTR) under the debugger.
  for i := 1 to 5 do
  begin
    Should(FClient.Connected).BeTrue;
    FClient.Disconnect;
    Should(FClient.Connected).BeFalse;
    if i < 5 then
      FClient.Connect(NatsTestHost, NatsTestPort);
  end;
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

procedure TDextNatsIntegrationTests.UnsubscribeSubject_ShouldCancelAllOnSubject;
var
  subject: string;
  hits: Integer;
  received: TEvent;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.unsubsubj');
  hits := 0;
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        received.SetEvent;
      end);
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        received.SetEvent;
      end);

    FClient.Publish(subject, 'a');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Sleep(200); // both non-queue subscribers receive a copy
    Should(hits).Be(2);

    FClient.UnsubscribeSubject(subject);
    FClient.Flush(2000);
    received.ResetEvent;
    FClient.Publish(subject, 'b');
    Should(received.WaitFor(500) = wrSignaled).BeFalse;
    Should(hits).Be(2);
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

procedure TDextNatsIntegrationTests.HealthCheck_WithFlush_ShouldReportHealthyWhenLive;
var
  Check: TNatsHealthCheck;
  Res: TNatsHealthResult;
begin
  if not EnsureServerOrFail then
    Exit;

  Check := TNatsHealthCheck.Create(FClient, TNatsHealthCheckOptions.CreateWithFlush(1000));
  try
    Res := Check.CheckHealth;
    Should(Ord(Res.Status)).Be(Ord(nhsHealthy));
    Should(Res.Description.Contains('responsive')).BeTrue;
  finally
    Check.Free;
  end;
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

procedure TDextNatsIntegrationTests.RequestAsyncBuilder_ShouldAwaitReply;
var
  subject: string;
  reply: TNatsMsg;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.reqasync.await');
  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
      if AMsg.HasReplyTo then
        FClient.Publish(AMsg.ReplyTo, 'builder-ok');
    end);

  reply := FClient.RequestAsync(subject, BytesOfUtf8('q'), 3000).Await;
  Should(reply.AsString).Be('builder-ok');
end;

procedure TDextNatsIntegrationTests.RequestAsyncBuilder_Timeout_ShouldRaise;
var
  subject: string;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.reqasync.await.timeout');
  // Silent subscriber avoids 503 no-responders so Request's timeout path runs.
  FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
    end);

  Should(
    procedure
    begin
      FClient.RequestAsync(subject, BytesOfUtf8('q'), 300).Await;
    end).Throw(EDextNatsTimeoutError);
end;

procedure TDextNatsIntegrationTests.FlushAsync_ShouldAwait;
begin
  if not EnsureServerOrFail then
    Exit;
  Should(FClient.FlushAsync(3000).Await).BeTrue;
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

procedure TDextNatsIntegrationTests.Events_OnDisconnected_ShouldFire;
var
  disconnected, reconnected: TEvent;
begin
  if LiveSkippedByEnv then
    Exit;

  // MaxPingsOutstanding=0 forces PingLoop to close the socket; RecvLoop reconnects.
  RecreateClientForStalePingReconnect(400, 8 * 1024 * 1024);
  disconnected := TEvent.Create(nil, True, False, '');
  reconnected := TEvent.Create(nil, True, False, '');
  try
    FClient.OnDisconnected :=
      procedure
      begin
        StabilizePingAfterForcedDisconnect;
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
    Should(reconnected.WaitFor(10000) = wrSignaled).BeTrue;
    Should(FClient.Connected).BeTrue;
  finally
    disconnected.Free;
    reconnected.Free;
  end;
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

procedure TDextNatsJetStreamTests.OrderedConsumer_ShouldDeliverInOrder;
var
  stream, subject: string;
  streamCfg: TNatsStreamConfig;
  opts: TNatsOrderedConsumerOptions;
  ordered: TDextNatsOrderedConsumer;
  lock: TCriticalSection;
  got: TEvent;
  payloads: IList<string>;
  seqs: IList<UInt64>;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_ORD_S');
  subject := JsUniqueSubject(stream);
  payloads := TCollections.CreateList<string>;
  seqs := TCollections.CreateList<UInt64>;
  lock := TCriticalSection.Create;
  got := TEvent.Create(nil, True, False, '');
  try
    streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
    streamCfg.Storage := ssMemory;
    FJs.CreateStream(streamCfg);
    try
      for i := 1 to 5 do
        FJs.Publish(subject, Format('ord-%d', [i]));

      opts := TNatsOrderedConsumerOptions.CreateDefault(subject);
      opts.NamePrefix := 'dextord_' + UniqueName('p');
      ordered := FJs.SubscribeOrdered(stream,
        procedure(const AMsg: TNatsJsMsg)
        begin
          lock.Enter;
          try
            payloads.Add(AMsg.AsString);
            seqs.Add(AMsg.StreamSequence);
            if payloads.Count >= 5 then
              got.SetEvent;
          finally
            lock.Leave;
          end;
        end,
        opts);
      try
        Should(ordered.Active).BeTrue;
        Should(ordered.ConsumerName <> '').BeTrue;
        Should(got.WaitFor(8000) = wrSignaled).BeTrue;
        Should(payloads.Count).Be(5);
        for i := 0 to 4 do
        begin
          Should(payloads[i]).Be(Format('ord-%d', [i + 1]));
          if i > 0 then
            Should(seqs[i]).Be(seqs[i - 1] + 1);
        end;
        Should(ordered.LastStreamSequence).Be(UInt64(seqs[4]));
      finally
        ordered.Free;
      end;
    finally
      FJs.DeleteStream(stream);
    end;
  finally
    got.Free;
    lock.Free;
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

procedure TDextNatsJetStreamTests.Stream_List_ShouldIncludeCreatedStream;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
  names: IList<string>;
  infos: IList<TNatsStreamInfo>;
  foundName, foundInfo: Boolean;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_LIST');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  FJs.CreateStream(cfg);
  try
    names := FJs.ListStreamNames;
    foundName := False;
    for i := 0 to names.Count - 1 do
      if names[i] = stream then
      begin
        foundName := True;
        Break;
      end;
    Should(foundName).BeTrue;

    names := FJs.ListStreamNames(subject);
    foundName := False;
    for i := 0 to names.Count - 1 do
      if names[i] = stream then
      begin
        foundName := True;
        Break;
      end;
    Should(foundName).BeTrue;

    infos := FJs.ListStreams(subject);
    foundInfo := False;
    for i := 0 to infos.Count - 1 do
      if infos[i].Name = stream then
      begin
        foundInfo := True;
        Break;
      end;
    Should(foundInfo).BeTrue;
  finally
    FJs.DeleteStream(stream);
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

procedure TDextNatsJetStreamTests.Consumer_List_ShouldIncludeCreatedConsumer;
var
  stream, consumer, subject: string;
  streamCfg: TNatsStreamConfig;
  consumerCfg: TNatsConsumerConfig;
  names: IList<string>;
  infos: IList<TNatsConsumerInfo>;
  foundName, foundInfo: Boolean;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_CLIST');
  consumer := UniqueName('DEXT_JS_CLISTC');
  subject := JsUniqueSubject(stream);
  streamCfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  streamCfg.Storage := ssMemory;
  FJs.CreateStream(streamCfg);
  try
    consumerCfg := TNatsConsumerConfig.CreateDefault(consumer, subject);
    FJs.CreateConsumer(stream, consumerCfg);

    names := FJs.ListConsumerNames(stream);
    foundName := False;
    for i := 0 to names.Count - 1 do
      if names[i] = consumer then
      begin
        foundName := True;
        Break;
      end;
    Should(foundName).BeTrue;

    infos := FJs.ListConsumers(stream);
    foundInfo := False;
    for i := 0 to infos.Count - 1 do
      if infos[i].Name = consumer then
      begin
        foundInfo := True;
        Break;
      end;
    Should(foundInfo).BeTrue;
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

procedure TDextNatsJetStreamTests.Stream_CreateWithCompression_ShouldRoundTrip;
var
  stream, subject: string;
  cfg: TNatsStreamConfig;
  info: TNatsStreamInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_COMP');
  subject := JsUniqueSubject(stream);
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  { File storage required for S2 compression on typical nats-server builds. }
  cfg.Storage := ssFile;
  cfg.Compression := scS2;
  try
    info := FJs.CreateStream(cfg);
  except
    on E: EDextNatsJetStreamError do
    begin
      LiveSoftSkipOrFail(
        Format('Stream compression unsupported by server (%s). Need nats-server with S2.',
          [E.Message]));
      Exit;
    end;
  end;
  try
    Should(Ord(info.Config.Compression)).Be(Ord(scS2));
    info := FJs.GetStreamInfo(stream);
    Should(Ord(info.Config.Compression)).Be(Ord(scS2));
    cfg.Description := 'compressed';
    cfg.Compression := scS2;
    info := FJs.UpdateStream(cfg);
    Should(info.Config.Description).Be('compressed');
    Should(Ord(info.Config.Compression)).Be(Ord(scS2));
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Stream_CreateWithRePublish_ShouldRoundTrip;
var
  stream, subject, dest: string;
  cfg: TNatsStreamConfig;
  info: TNatsStreamInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  stream := UniqueName('DEXT_JS_RP');
  subject := JsUniqueSubject(stream);
  dest := subject + '.rp';
  cfg := TNatsStreamConfig.CreateDefault(stream, [subject]);
  cfg.Storage := ssMemory;
  cfg.RePublish.Source := subject;
  cfg.RePublish.Destination := dest;
  try
    info := FJs.CreateStream(cfg);
  except
    on E: EDextNatsJetStreamError do
    begin
      LiveSoftSkipOrFail(
        Format('Stream republish unsupported by server (%s).', [E.Message]));
      Exit;
    end;
  end;
  try
    Should(info.Config.RePublish.IsSet).BeTrue;
    Should(info.Config.RePublish.Destination).Be(dest);
    info := FJs.GetStreamInfo(stream);
    Should(info.Config.RePublish.Source).Be(subject);
    Should(info.Config.RePublish.Destination).Be(dest);
  finally
    FJs.DeleteStream(stream);
  end;
end;

procedure TDextNatsJetStreamTests.Stream_CreateWithMirror_ShouldRoundTrip;
var
  origin, mirror, subject: string;
  cfg, mcfg: TNatsStreamConfig;
  info: TNatsStreamInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  origin := UniqueName('DEXT_JS_MORI');
  mirror := UniqueName('DEXT_JS_MIRR');
  subject := JsUniqueSubject(origin);
  cfg := TNatsStreamConfig.CreateDefault(origin, [subject]);
  cfg.Storage := ssMemory;
  FJs.CreateStream(cfg);
  try
    mcfg := TNatsStreamConfig.CreateDefault(mirror, []);
    mcfg.Storage := ssMemory;
    mcfg.Mirror.Name := origin;
    mcfg.MirrorDirect := True;
    try
      info := FJs.CreateStream(mcfg);
    except
      on E: EDextNatsJetStreamError do
      begin
        LiveSoftSkipOrFail(
          Format('Stream mirror unsupported by server (%s).', [E.Message]));
        Exit;
      end;
    end;
    try
      Should(info.Config.Mirror.IsSet).BeTrue;
      Should(info.Config.Mirror.Name).Be(origin);
      info := FJs.GetStreamInfo(mirror);
      Should(info.Config.Mirror.Name).Be(origin);
    finally
      FJs.DeleteStream(mirror);
    end;
  finally
    FJs.DeleteStream(origin);
  end;
end;

{ TDextNatsKeyValueTests }

function TDextNatsKeyValueTests.UniqueBucket(const APrefix: string): string;
begin
  { Bucket names allow only [A-Za-z0-9_-] }
  Result := APrefix + '_' + IntToHex(Random(MaxInt), 8);
end;

function TDextNatsKeyValueTests.EnsureJetStreamOrFail: Boolean;
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

procedure TDextNatsKeyValueTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
  FJs := nil;
end;

procedure TDextNatsKeyValueTests.TearDown;
begin
  FreeAndNil(FJs);
  FreeAndNil(FClient);
end;

procedure TDextNatsKeyValueTests.Bucket_CreatePutGetDelete_ShouldRoundTrip;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  entry: TNatsKeyValueEntry;
  rev: UInt64;
  status: TNatsKeyValueStatus;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKV');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    Should(TDextNatsKeyValue.BucketExists(FJs, bucket)).BeTrue;
    rev := kv.Put('widget-blue', '42');
    Should(Int64(rev) > 0).BeTrue;
    entry := kv.Get('widget-blue');
    Should(entry.AsString).Be('42');
    Should(entry.IsPut).BeTrue;
    Should(Int64(entry.Revision)).Be(Int64(rev));
    status := kv.Status;
    Should(status.Bucket).Be(bucket);
    Should(status.StreamName).Be('KV_' + bucket);
    Should(Int64(status.Values) >= 1).BeTrue;

    kv.Delete('widget-blue');
    Should(kv.TryGet('widget-blue', entry)).BeFalse;
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
  Should(TDextNatsKeyValue.BucketExists(FJs, bucket)).BeFalse;
end;

procedure TDextNatsKeyValueTests.Bucket_Purge_ShouldHideKey;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  entry: TNatsKeyValueEntry;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVP');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Put('widget-red', 'secret');
    kv.Purge('widget-red');
    Should(kv.TryGet('widget-red', entry)).BeFalse;
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.PurgeDeletes_ShouldRemoveMarkersWhenForced;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  opts: TNatsKeyValuePurgeDeletesOptions;
  hist: IList<TNatsKeyValueEntry>;
  entry: TNatsKeyValueEntry;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVPD');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Put('keep', '1');
    kv.Put('gone', '2');
    kv.Delete('gone');
    hist := kv.History('gone');
    Should(hist.Count > 0).BeTrue;

    opts := TNatsKeyValuePurgeDeletesOptions.CreateDefault;
    { Negative = remove all markers regardless of age (nats.go). }
    opts.DeleteMarkersOlderThan := -1;
    kv.PurgeDeletes(opts);

    Should(kv.TryGet('keep', entry)).BeTrue;
    Should(entry.AsString).Be('1');
    Should(kv.TryGet('gone', entry)).BeFalse;
    hist := kv.History('gone');
    Should(hist.Count).Be(0);
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.BucketExists_Missing_ShouldBeFalse;
begin
  if not EnsureJetStreamOrFail then
    Exit;
  Should(TDextNatsKeyValue.BucketExists(FJs, UniqueBucket('DEXTKVMISS'))).BeFalse;
end;

procedure TDextNatsKeyValueTests.Get_MissingKey_ShouldRaise;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVG');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    Should(
      procedure
      begin
        kv.Get('no-such-key');
      end).Throw(EDextNatsKeyNotFound);
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Keys_ShouldListLiveKeysOnly;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  keys: IList<string>;
  i: Integer;
  sawA, sawB: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVK');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Put('alpha', '1');
    kv.Put('beta', '2');
    kv.Delete('alpha');
    keys := kv.Keys;
    Should(keys.Count).Be(1);
    Should(keys[0]).Be('beta');
    keys := kv.ListKeys;
    sawA := False;
    sawB := False;
    for i := 0 to keys.Count - 1 do
    begin
      if keys[i] = 'alpha' then
        sawA := True;
      if keys[i] = 'beta' then
        sawB := True;
    end;
    Should(sawA).BeFalse;
    Should(sawB).BeTrue;
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.ListKeysFiltered_ShouldMatchWildcardAndMultiFilters;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  keys: IList<string>;
  i: Integer;
  sawBlue, sawRed, sawGadget, sawTool: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVLKF');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Put('widget.blue', '41');
    kv.Put('widget.red', '7');
    kv.Put('gadget.pro', '99');
    kv.Put('tool.kit', '1');
    kv.Delete('tool.kit');

    keys := kv.ListKeysFiltered(['widget.*']);
    sawBlue := False;
    sawRed := False;
    sawGadget := False;
    sawTool := False;
    for i := 0 to keys.Count - 1 do
    begin
      if keys[i] = 'widget.blue' then
        sawBlue := True;
      if keys[i] = 'widget.red' then
        sawRed := True;
      if keys[i] = 'gadget.pro' then
        sawGadget := True;
      if keys[i] = 'tool.kit' then
        sawTool := True;
    end;
    Should(keys.Count).Be(2);
    Should(sawBlue).BeTrue;
    Should(sawRed).BeTrue;
    Should(sawGadget).BeFalse;
    Should(sawTool).BeFalse;

    { Multi-filter via consumer filter_subjects; Keys overload aliases ListKeysFiltered. }
    keys := kv.Keys(['widget.*', 'gadget.>']);
    sawBlue := False;
    sawRed := False;
    sawGadget := False;
    sawTool := False;
    for i := 0 to keys.Count - 1 do
    begin
      if keys[i] = 'widget.blue' then
        sawBlue := True;
      if keys[i] = 'widget.red' then
        sawRed := True;
      if keys[i] = 'gadget.pro' then
        sawGadget := True;
      if keys[i] = 'tool.kit' then
        sawTool := True;
    end;
    Should(keys.Count).Be(3);
    Should(sawBlue).BeTrue;
    Should(sawRed).BeTrue;
    Should(sawGadget).BeTrue;
    Should(sawTool).BeFalse;

    { Empty filters = all live keys (same as Keys). }
    keys := kv.ListKeysFiltered([]);
    Should(keys.Count).Be(3);
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.History_ShouldReturnRevisions;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  hist: IList<TNatsKeyValueEntry>;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVH');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Put('counter', 'one');
    kv.Put('counter', 'two');
    kv.Put('counter', 'three');
    hist := kv.History('counter');
    Should(hist.Count).Be(3);
    Should(hist[0].AsString).Be('one');
    Should(hist[1].AsString).Be('two');
    Should(hist[2].AsString).Be('three');
    Should(hist[0].IsPut).BeTrue;
    Should(Int64(hist[2].Revision) > Int64(hist[0].Revision)).BeTrue;
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.GetRevision_ShouldReturnHistoricalPutAndTombstone;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  rev1, rev2, delRev: UInt64;
  entry: TNatsKeyValueEntry;
  hist: IList<TNatsKeyValueEntry>;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVREV');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    rev1 := kv.Put('item', 'alpha');
    rev2 := kv.Put('item', 'beta');
    entry := kv.GetRevision('item', rev1);
    Should(entry.AsString).Be('alpha');
    Should(entry.Revision).Be(rev1);
    Should(entry.IsPut).BeTrue;
    entry := kv.GetRevision('item', rev2);
    Should(entry.AsString).Be('beta');
    Should(entry.Revision).Be(rev2);
    kv.Delete('item');
    Should(kv.TryGet('item', entry)).BeFalse;
    { Historical PUT remains readable after delete (unlike Get). }
    Should(kv.TryGetRevision('item', rev2, entry)).BeTrue;
    Should(entry.AsString).Be('beta');
    hist := kv.History('item');
    Should(hist.Count >= 3).BeTrue;
    delRev := hist[hist.Count - 1].Revision;
    entry := kv.GetRevision('item', delRev);
    Should(entry.IsPut).BeFalse;
    Should(Ord(entry.Operation)).Be(Ord(kvoDelete));
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.GetRevision_WrongKeyOrMissing_ShouldRaise;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  rev: UInt64;
  entry: TNatsKeyValueEntry;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVREVN');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    rev := kv.Put('alpha', 'one');
    kv.Put('beta', 'two');
    Should(
      procedure
      begin
        kv.GetRevision('beta', rev);
      end).Throw(EDextNatsKeyNotFound);
    Should(kv.TryGetRevision('alpha', rev + 999999, entry)).BeFalse;
    Should(
      procedure
      begin
        kv.GetRevision('alpha', 0);
      end).Throw(EDextNatsKeyNotFound);
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Entry_EndOfInitialMarker_ShouldNotBePut;
var
  entry: TNatsKeyValueEntry;
begin
  entry := TNatsKeyValueEntry.EndOfInitialMarker;
  Should(entry.IsEndOfInitial).BeTrue;
  Should(entry.EndOfInitial).BeTrue;
  Should(entry.IsPut).BeFalse;
  Should(entry.Key).Be('');
  Should(entry.AsString).Be('');
end;

procedure TDextNatsKeyValueTests.WatchOptions_ShouldDefaultFalse;
var
  opts: TNatsKeyValueWatchOptions;
begin
  opts := TNatsKeyValueWatchOptions.CreateDefault;
  Should(opts.MetaOnly).BeFalse;
  Should(opts.UpdatesOnly).BeFalse;
  Should(opts.IncludeHistory).BeFalse;
  Should(opts.IgnoreDeletes).BeFalse;
  Should(Int64(opts.ResumeFromRevision)).Be(0);
end;

procedure TDextNatsKeyValueTests.WatchOptions_IncludeHistoryWithUpdatesOnly_ShouldRaise;
var
  opts: TNatsKeyValueWatchOptions;
  raised: Boolean;
begin
  opts := TNatsKeyValueWatchOptions.CreateDefault;
  opts.IncludeHistory := True;
  opts.UpdatesOnly := True;
  raised := False;
  try
    opts.Validate;
  except
    on E: EDextNatsKeyValueError do
      raised := True;
  end;
  Should(raised).BeTrue;
end;

procedure TDextNatsKeyValueTests.PurgeDeletesOptions_ShouldDefaultZero;
var
  opts: TNatsKeyValuePurgeDeletesOptions;
begin
  opts := TNatsKeyValuePurgeDeletesOptions.CreateDefault;
  Should(opts.DeleteMarkersOlderThan).Be(0);
  Should(NATS_KV_DEFAULT_PURGE_DELETES_OLDER_THAN_NANOS >
    NATS_KV_MIN_TTL_NANOS).BeTrue;
end;

procedure TDextNatsKeyValueTests.WatchAll_ShouldDeliverCurrentAndUpdates;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  lock: TCriticalSection;
  got: IList<string>;
  gotInitial, gotUpdate: TEvent;
  i: Integer;
  sawFoo, sawBar, sawFoo2: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVW');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotInitial := TEvent.Create(nil, True, False, '');
  gotUpdate := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    kv.Put('foo', '1');
    kv.Put('bar', '2');

    watcher := kv.WatchAll(
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        if not AEntry.IsPut then
          Exit;
        lock.Enter;
        try
          got.Add(AEntry.Key + '=' + AEntry.AsString);
          if got.Count >= 2 then
            gotInitial.SetEvent;
          if AEntry.Key = 'foo' then
          begin
            if AEntry.AsString = '9' then
              gotUpdate.SetEvent;
          end;
        finally
          lock.Leave;
        end;
      end);

    Should(gotInitial.WaitFor(5000) = wrSignaled).BeTrue;
    kv.Put('foo', '9');
    Should(gotUpdate.WaitFor(5000) = wrSignaled).BeTrue;

    sawFoo := False;
    sawBar := False;
    sawFoo2 := False;
    lock.Enter;
    try
      for i := 0 to got.Count - 1 do
      begin
        if got[i] = 'foo=1' then
          sawFoo := True;
        if got[i] = 'bar=2' then
          sawBar := True;
        if got[i] = 'foo=9' then
          sawFoo2 := True;
      end;
    finally
      lock.Leave;
    end;
    Should(sawFoo).BeTrue;
    Should(sawBar).BeTrue;
    Should(sawFoo2).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotInitial.Free;
    gotUpdate.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchAll_ShouldSignalEndOfInitial;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  lock: TCriticalSection;
  gotKeys: IList<string>;
  markerAt: Integer;
  gotMarker, gotUpdate: TEvent;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVEOI');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  gotKeys := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  gotUpdate := TEvent.Create(nil, True, False, '');
  watcher := nil;
  markerAt := -1;
  try
    kv.Put('a', '1');
    kv.Put('b', '2');

    watcher := kv.WatchAll(
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        lock.Enter;
        try
          if AEntry.IsEndOfInitial then
          begin
            if markerAt < 0 then
              markerAt := gotKeys.Count;
            gotMarker.SetEvent;
          end
          else if AEntry.IsPut then
          begin
            gotKeys.Add(AEntry.Key + '=' + AEntry.AsString);
            if AEntry.Key = 'a' then
              if AEntry.AsString = '9' then
                gotUpdate.SetEvent;
          end;
        finally
          lock.Leave;
        end;
      end);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    lock.Enter;
    try
      Should(markerAt >= 0).BeTrue;
      { Marker after both snapshot puts (order of a/b not guaranteed). }
      Should(markerAt >= 2).BeTrue;
      for i := 0 to markerAt - 1 do
        Should((gotKeys[i] = 'a=1') or (gotKeys[i] = 'b=2')).BeTrue;
    finally
      lock.Leave;
    end;

    kv.Put('a', '9');
    Should(gotUpdate.WaitFor(5000) = wrSignaled).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    gotUpdate.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchAll_EmptyBucket_ShouldSignalEndOfInitial;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  gotMarker: TEvent;
  sawEntry: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVEMP');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  sawEntry := False;
  try
    watcher := kv.WatchAll(
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        if AEntry.IsEndOfInitial then
          gotMarker.SetEvent
        else
          sawEntry := True;
      end);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    Should(sawEntry).BeFalse;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchAll_UpdatesOnly_ShouldSkipInitialAndMarker;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  opts: TNatsKeyValueWatchOptions;
  lock: TCriticalSection;
  got: IList<string>;
  gotUpdate: TEvent;
  sawMarker: Boolean;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVUO');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotUpdate := TEvent.Create(nil, True, False, '');
  watcher := nil;
  sawMarker := False;
  try
    kv.Put('seed', 'old');
    opts := TNatsKeyValueWatchOptions.CreateDefault;
    opts.UpdatesOnly := True;

    watcher := kv.WatchAll(
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        lock.Enter;
        try
          if AEntry.IsEndOfInitial then
            sawMarker := True
          else if AEntry.IsPut then
          begin
            got.Add(AEntry.Key + '=' + AEntry.AsString);
            if (AEntry.Key = 'seed') and (AEntry.AsString = 'new') then
              gotUpdate.SetEvent;
          end;
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(watcher.InitialDone).BeTrue;
    Sleep(300);
    kv.Put('seed', 'new');
    Should(gotUpdate.WaitFor(5000) = wrSignaled).BeTrue;

    lock.Enter;
    try
      Should(sawMarker).BeFalse;
      Should(got.Count > 0).BeTrue;
      for i := 0 to got.Count - 1 do
        Should(got[i] <> 'seed=old').BeTrue;
      Should(got[got.Count - 1]).Be('seed=new');
    finally
      lock.Leave;
    end;
  finally
    if watcher <> nil then
      watcher.Free;
    gotUpdate.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchAll_MetaOnly_ShouldOmitValues;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  opts: TNatsKeyValueWatchOptions;
  lock: TCriticalSection;
  gotKey: string;
  gotLen: Integer;
  gotMarker: TEvent;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVMO');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  gotKey := '';
  gotLen := -1;
  try
    kv.Put('meta', 'secret-value');
    opts := TNatsKeyValueWatchOptions.CreateDefault;
    opts.MetaOnly := True;

    watcher := kv.WatchAll(
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        if AEntry.IsEndOfInitial then
        begin
          gotMarker.SetEvent;
          Exit;
        end;
        if not AEntry.IsPut then
          Exit;
        lock.Enter;
        try
          gotKey := AEntry.Key;
          gotLen := Length(AEntry.Value);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    lock.Enter;
    try
      Should(gotKey).Be('meta');
      Should(gotLen).Be(0);
    finally
      lock.Leave;
    end;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchAll_IncludeHistory_ShouldReplayRevisions;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  opts: TNatsKeyValueWatchOptions;
  lock: TCriticalSection;
  got: IList<string>;
  gotMarker: TEvent;
  i: Integer;
  saw1, saw2, saw3: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVIH');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 10;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    kv.Put('k', 'v1');
    kv.Put('k', 'v2');
    kv.Put('k', 'v3');
    opts := TNatsKeyValueWatchOptions.CreateDefault;
    opts.IncludeHistory := True;

    watcher := kv.Watch('k',
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        lock.Enter;
        try
          if AEntry.IsEndOfInitial then
            gotMarker.SetEvent
          else if AEntry.IsPut then
            got.Add(AEntry.AsString);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    saw1 := False;
    saw2 := False;
    saw3 := False;
    lock.Enter;
    try
      Should(got.Count >= 3).BeTrue;
      for i := 0 to got.Count - 1 do
      begin
        if got[i] = 'v1' then
          saw1 := True;
        if got[i] = 'v2' then
          saw2 := True;
        if got[i] = 'v3' then
          saw3 := True;
      end;
    finally
      lock.Leave;
    end;
    Should(saw1).BeTrue;
    Should(saw2).BeTrue;
    Should(saw3).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchAll_IgnoreDeletes_ShouldSkipDeleteMarkers;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  opts: TNatsKeyValueWatchOptions;
  lock: TCriticalSection;
  gotOps: IList<string>;
  gotMarker, gotLiveDel: TEvent;
  i: Integer;
  sawDel: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVID');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  gotOps := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  gotLiveDel := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    kv.Put('keep', '1');
    kv.Put('gone', '2');
    kv.Delete('gone');
    opts := TNatsKeyValueWatchOptions.CreateDefault;
    opts.IgnoreDeletes := True;

    watcher := kv.WatchAll(
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        lock.Enter;
        try
          if AEntry.IsEndOfInitial then
            gotMarker.SetEvent
          else if AEntry.Operation = kvoDelete then
          begin
            gotOps.Add('DEL:' + AEntry.Key);
            gotLiveDel.SetEvent;
          end
          else if AEntry.IsPut then
            gotOps.Add('PUT:' + AEntry.Key);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    kv.Delete('keep');
    { With IgnoreDeletes, the live delete must not reach the handler. }
    Should(gotLiveDel.WaitFor(800) = wrTimeout).BeTrue;

    sawDel := False;
    lock.Enter;
    try
      for i := 0 to gotOps.Count - 1 do
        if gotOps[i].StartsWith('DEL:') then
          sawDel := True;
    finally
      lock.Leave;
    end;
    Should(sawDel).BeFalse;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    gotLiveDel.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Watch_ResumeFromRevision_ShouldSkipEarlierRevisions;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  opts: TNatsKeyValueWatchOptions;
  lock: TCriticalSection;
  got: IList<string>;
  gotMarker: TEvent;
  rev1, rev2, rev3: UInt64;
  i: Integer;
  sawV1, sawV2, sawV3: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVRF');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 10;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    rev1 := kv.Put('k', 'v1');
    rev2 := kv.Put('k', 'v2');
    rev3 := kv.Put('k', 'v3');
    Should(Int64(rev2) > Int64(rev1)).BeTrue;
    Should(Int64(rev3) > Int64(rev2)).BeTrue;

    opts := TNatsKeyValueWatchOptions.CreateDefault;
    opts.ResumeFromRevision := rev2;

    watcher := kv.Watch('k',
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        lock.Enter;
        try
          if AEntry.IsEndOfInitial then
            gotMarker.SetEvent
          else if AEntry.IsPut then
            got.Add(AEntry.AsString);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    sawV1 := False;
    sawV2 := False;
    sawV3 := False;
    lock.Enter;
    try
      Should(got.Count >= 2).BeTrue;
      for i := 0 to got.Count - 1 do
      begin
        if got[i] = 'v1' then
          sawV1 := True;
        if got[i] = 'v2' then
          sawV2 := True;
        if got[i] = 'v3' then
          sawV3 := True;
      end;
    finally
      lock.Leave;
    end;
    { Resume at rev2 must not replay v1. }
    Should(sawV1).BeFalse;
    Should(sawV2).BeTrue;
    Should(sawV3).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.WatchFiltered_ShouldMatchWildcardKeys;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  watcher: TDextNatsKeyValueWatcher;
  lock: TCriticalSection;
  got: IList<string>;
  gotMarker: TEvent;
  i, j, attempt: Integer;
  sawBlue, sawRed, sawGadget: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVWF');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    kv.Put('widget.blue', '41');
    kv.Put('widget.red', '7');
    kv.Put('gadget.pro', '99');

    watcher := kv.WatchFiltered(['widget.*'],
      procedure(const AEntry: TNatsKeyValueEntry)
      begin
        lock.Enter;
        try
          if AEntry.IsEndOfInitial then
            gotMarker.SetEvent
          else if AEntry.IsPut then
            got.Add(AEntry.Key + '=' + AEntry.AsString);
        finally
          lock.Leave;
        end;
      end);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    sawBlue := False;
    sawRed := False;
    sawGadget := False;
    lock.Enter;
    try
      Should(got.Count).Be(2);
      for i := 0 to got.Count - 1 do
      begin
        if got[i] = 'widget.blue=41' then
          sawBlue := True;
        if got[i] = 'widget.red=7' then
          sawRed := True;
        if got[i].StartsWith('gadget.') then
          sawGadget := True;
      end;
    finally
      lock.Leave;
    end;
    Should(sawBlue).BeTrue;
    Should(sawRed).BeTrue;
    Should(sawGadget).BeFalse;

    kv.Put('widget.blue', '42');
    kv.Put('gadget.pro', '100');
    { Live update for matching key only. }
    sawBlue := False;
    sawGadget := False;
    for attempt := 1 to 50 do
    begin
      lock.Enter;
      try
        sawBlue := False;
        sawGadget := False;
        for j := 0 to got.Count - 1 do
        begin
          if got[j] = 'widget.blue=42' then
            sawBlue := True;
          if got[j] = 'gadget.pro=100' then
            sawGadget := True;
        end;
      finally
        lock.Leave;
      end;
      if sawBlue then
        Break;
      Sleep(100);
    end;
    Should(sawBlue).BeTrue;
    Should(sawGadget).BeFalse;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    lock.Free;
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Config_ShouldRoundTripFromStream;
var
  bucket: string;
  cfg, back: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  st: TNatsKeyValueStatus;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVCFG');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Description := 'cfg-roundtrip';
  cfg.Storage := ssMemory;
  cfg.History := 5;
  cfg.MaxValueSize := 2048;
  cfg.Compression := scS2;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    back := kv.Config;
    Should(back.Bucket).Be(bucket);
    Should(back.Description).Be('cfg-roundtrip');
    Should(back.History).Be(5);
    Should(back.MaxValueSize).Be(2048);
    Should(Ord(back.Storage)).Be(Ord(ssMemory));
    Should(Ord(back.Compression)).Be(Ord(scS2));
    st := kv.Status;
    Should(st.History).Be(5);
    Should(st.Config.Description).Be('cfg-roundtrip');
    Should(Ord(st.Config.Compression)).Be(Ord(scS2));
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Create_ShouldPutOnlyIfAbsent;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  entry: TNatsKeyValueEntry;
  rev: UInt64;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVC');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    rev := kv.Create('widget-blue', 'first');
    Should(Int64(rev) > 0).BeTrue;
    entry := kv.Get('widget-blue');
    Should(entry.AsString).Be('first');
    Should(Int64(entry.Revision)).Be(Int64(rev));
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Create_ExistingKey_ShouldRaiseKeyExists;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVCE');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Create('widget-blue', 'first');
    Should(
      procedure
      begin
        kv.Create('widget-blue', 'second');
      end).Throw(EDextNatsKeyExists);
    Should(kv.Get('widget-blue').AsString).Be('first');
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Create_AfterDelete_ShouldSucceed;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  rev: UInt64;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVCD');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    kv.Create('lock', 'held');
    kv.Delete('lock');
    rev := kv.Create('lock', 'held-again');
    Should(Int64(rev) > 0).BeTrue;
    Should(kv.Get('lock').AsString).Be('held-again');
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Update_ShouldSucceedWhenRevisionMatches;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  entry: TNatsKeyValueEntry;
  rev1, rev2: UInt64;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVU');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    rev1 := kv.Put('counter', '41');
    rev2 := kv.Update('counter', '40', rev1);
    Should(Int64(rev2) > Int64(rev1)).BeTrue;
    entry := kv.Get('counter');
    Should(entry.AsString).Be('40');
    Should(Int64(entry.Revision)).Be(Int64(rev2));
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Update_WrongRevision_ShouldRaiseMismatch;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  rev: UInt64;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVUM');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 5;
  kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
  try
    rev := kv.Put('counter', '41');
    kv.Put('counter', '42'); { bump revision }
    Should(
      procedure
      begin
        kv.Update('counter', '40', rev);
      end).Throw(EDextNatsKeyRevisionMismatch);
    Should(kv.Get('counter').AsString).Be('42');
  finally
    kv.Free;
    TDextNatsKeyValue.DeleteBucket(FJs, bucket);
  end;
end;

procedure TDextNatsKeyValueTests.Create_WithPerKeyTTL_ShouldExpire;
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  entry: TNatsKeyValueEntry;
  rev: UInt64;
  deadline: TDateTime;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTKVTTL');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  cfg.History := 1;
  cfg.LimitMarkerTTL := 2 * NATS_KV_MIN_TTL_NANOS; { marker floor / enable allow_msg_ttl }
  kv := nil;
  try
    try
      kv := TDextNatsKeyValue.CreateBucket(FJs, cfg);
    except
      on E: EDextNatsJetStreamError do
      begin
        { Older nats-server without ADR-48 / API level 1 }
        LiveSoftSkipOrFail(
          Format('Per-key TTL unsupported by server (%s). Need nats-server 2.11+.', [E.Message]));
        Exit;
      end;
    end;
    rev := kv.Create('session', 'ephemeral', 2 * NATS_KV_MIN_TTL_NANOS);
    Should(Int64(rev) > 0).BeTrue;
    entry := kv.Get('session');
    Should(entry.AsString).Be('ephemeral');

    deadline := Now + (4 / SecsPerDay); { wait up to ~4s for 2s TTL }
    while (Now < deadline) and kv.TryGet('session', entry) do
      Sleep(200);
    Should(kv.TryGet('session', entry)).BeFalse;
  finally
    kv.Free;
    try
      TDextNatsKeyValue.DeleteBucket(FJs, bucket);
    except
    end;
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
  // Defensive: Explicit suite may also be enabled via DEXT_NATS_RUN_BENCH=1.
  if not NatsStressEnabled then
    Exit;
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
  if not NatsStressEnabled then
    Exit;
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
  if not NatsStressEnabled then
    Exit;
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
  if not NatsStressEnabled then
    Exit;
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
  if not NatsStressEnabled then
    Exit;
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

{ TDextNatsBenchmarkTests }

function TDextNatsBenchmarkTests.TryConnectLiveOrSoftSkip: Boolean;
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

function TDextNatsBenchmarkTests.EnsureServerOrFail: Boolean;
begin
  Result := TryConnectLiveOrSoftSkip;
end;

procedure TDextNatsBenchmarkTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
end;

procedure TDextNatsBenchmarkTests.TearDown;
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

procedure TDextNatsBenchmarkTests.Encode_Throughput_ShouldReportOpsPerSec;
const
  Iterations = 200000;
var
  i: Integer;
  payload, frame, msgWire: TBytes;
  parser: TDextNatsFrameParser;
  parsed: TNatsFrame;
  sw: TStopwatch;
  ms: Int64;
  opsPerSec: Double;
  parsedOk: Integer;
begin
  // Defensive: Explicit suite may be enabled via DEXT_NATS_RUN_STRESS alone.
  if not NatsBenchEnabled then
    Exit;

  payload := BytesOfUtf8('bench-payload');

  sw := TStopwatch.StartNew;
  frame := nil;
  for i := 1 to Iterations do
    frame := NatsEncodePub('bench.subject', '', payload);
  sw.Stop;
  ms := sw.ElapsedMilliseconds;
  if ms < 1 then
    ms := 1;
  opsPerSec := Iterations * 1000.0 / ms;
  SafeWriteLn(Format(
    'BENCH Encode_Throughput: %d PUB encodes in %d ms = %.0f ops/sec',
    [Iterations, ms, opsPerSec]));
  Should(Utf8OfBytes(frame).StartsWith('PUB bench.subject ')).BeTrue;
  // Soft floor only — not a CI gate.
  Should(opsPerSec > 1000).BeTrue;

  msgWire := BytesOfUtf8(
    Format('MSG bench.subject 1 %d', [Length(payload)]) + #13#10 +
    Utf8OfBytes(payload) + #13#10);
  parser := TDextNatsFrameParser.Create;
  try
    parsedOk := 0;
    sw := TStopwatch.StartNew;
    for i := 1 to Iterations do
    begin
      FeedParserBytes(parser, msgWire, 0, Length(msgWire));
      if parser.TryReadFrame(parsed) then
        Inc(parsedOk);
    end;
    sw.Stop;
    ms := sw.ElapsedMilliseconds;
    if ms < 1 then
      ms := 1;
    opsPerSec := Iterations * 1000.0 / ms;
    SafeWriteLn(Format(
      'BENCH Encode_Throughput: %d MSG parse roundtrips in %d ms = %.0f frames/sec',
      [Iterations, ms, opsPerSec]));
    Should(parsedOk).Be(Iterations);
    Should(opsPerSec > 1000).BeTrue;
  finally
    parser.Free;
  end;
end;

procedure TDextNatsBenchmarkTests.PubSub_Throughput_ShouldReportMsgsPerSec;
const
  MessageCount = 20000;
  Payload = 'bench';
var
  subject: string;
  received: Integer;
  done: TEvent;
  i: Integer;
  sw: TStopwatch;
  ms: Int64;
  msgsPerSec: Double;
begin
  if not NatsBenchEnabled then
    Exit;
  if not EnsureServerOrFail then
    Exit;

  subject := 'dext.nats.bench.pubsub.' + IntToHex(Random(MaxInt), 8);
  received := 0;
  done := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        if TInterlocked.Increment(received) >= MessageCount then
          done.SetEvent;
      end);

    // Brief settle so the SUB is registered before the publish burst.
    Sleep(50);

    sw := TStopwatch.StartNew;
    for i := 1 to MessageCount do
      FClient.Publish(subject, Payload);
    Should(done.WaitFor(60000) = wrSignaled).BeTrue;
    sw.Stop;
    ms := sw.ElapsedMilliseconds;
    if ms < 1 then
      ms := 1;
    msgsPerSec := MessageCount * 1000.0 / ms;
    SafeWriteLn(Format(
      'BENCH PubSub_Throughput: %d msgs in %d ms = %.0f msgs/sec (publish+deliver)',
      [MessageCount, ms, msgsPerSec]));

    Should(received).Be(MessageCount);
    // Soft floor only — catastrophic-regression guard, not a CI perf gate.
    Should(msgsPerSec > 100).BeTrue;
  finally
    done.Free;
  end;
end;

{ TDextNatsDiTests }

procedure TDextNatsDiTests.TearDown;
begin
  TDextServices.DefaultProvider := nil;
end;

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

procedure TDextNatsDiTests.BindNatsOptions_FromConfiguration_ShouldMapHostPortTls;
var
  Config: IConfigurationRoot;
  Opts: TDextNatsOptions;
  Services: TDextServices;
  Provider: IServiceProvider;
  Client: TDextNatsClient;
begin
  Config := TDextConfiguration.New
    .AddValues([
      TPair<string, string>.Create('Nats:Host', 'cfg.nats.local'),
      TPair<string, string>.Create('Nats:Port', '4333'),
      TPair<string, string>.Create('Nats:Name', 'from-config'),
      TPair<string, string>.Create('Nats:EnableMetrics', 'true'),
      TPair<string, string>.Create('Nats:TLS:Enabled', 'true'),
      TPair<string, string>.Create('Nats:TLS:VerifyServerCertificate', 'false')
    ])
    .Build;

  Opts := BindNatsOptions(Config);
  Should(Opts.Host).Be('cfg.nats.local');
  Should(Opts.Port).Be(4333);
  Should(Opts.Name).Be('from-config');
  Should(Opts.EnableMetrics).BeTrue;
  Should(Opts.TLS.Enabled).BeTrue;
  Should(Opts.TLS.VerifyServerCertificate).BeFalse;

  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap, Config);
  Provider := Services.BuildServiceProvider;
  Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(Provider);
  Should(Client.Options.Host).Be('cfg.nats.local');
  Should(Client.Options.Port).Be(4333);
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

procedure TDextNatsDiTests.HealthCheck_Options_ShouldDefaultToConnectedOnly;
var
  Opts: TNatsHealthCheckOptions;
  Client: TDextNatsClient;
  Check: TNatsHealthCheck;
begin
  Opts := TNatsHealthCheckOptions.CreateDefault;
  Should(Opts.FlushTimeoutMs).Be(0);
  Opts := TNatsHealthCheckOptions.CreateWithFlush;
  Should(Opts.FlushTimeoutMs).Be(NATS_HEALTH_FLUSH_TIMEOUT_MS);
  Opts := TNatsHealthCheckOptions.CreateWithFlush(0);
  Should(Opts.FlushTimeoutMs).Be(NATS_HEALTH_FLUSH_TIMEOUT_MS);
  Opts := TNatsHealthCheckOptions.CreateWithFlush(250);
  Should(Opts.FlushTimeoutMs).Be(250);

  Client := TDextNatsClient.Create(TDextNatsOptions.CreateDefault);
  try
    Check := TNatsHealthCheck.Create(Client);
    try
      Should(Check.Options.FlushTimeoutMs).Be(0);
    finally
      Check.Free;
    end;
  finally
    Client.Free;
  end;
end;

procedure TDextNatsDiTests.HealthCheck_WithFlush_ShouldStayUnhealthyWhenDisconnected;
var
  Client: TDextNatsClient;
  Check: TNatsHealthCheck;
  Res: TNatsHealthResult;
  sw: TStopwatch;
begin
  // Deep probe must not call Flush (or hang) when the socket is down.
  Client := TDextNatsClient.Create(TDextNatsOptions.CreateDefault);
  try
    Check := TNatsHealthCheck.Create(Client, TNatsHealthCheckOptions.CreateWithFlush(500));
    try
      sw := TStopwatch.StartNew;
      Res := Check.CheckHealth;
      sw.Stop;
      Should(Ord(Res.Status)).Be(Ord(nhsUnhealthy));
      Should(Res.Description.Contains('disconnected')).BeTrue;
      Should(sw.ElapsedMilliseconds < 200).BeTrue;
    finally
      Check.Free;
    end;
  finally
    Client.Free;
  end;
end;

procedure TDextNatsDiTests.AddNatsHealthCheck_WithFlushOptions_ShouldApplyTimeout;
var
  Services: TDextServices;
  Provider: IServiceProvider;
  Check: TNatsHealthCheck;
begin
  Services := TDextServices.New;
  AddNatsClient(Services.Unwrap);
  AddNatsHealthCheck(Services.Unwrap, TNatsHealthCheckOptions.CreateWithFlush(750));
  Provider := Services.BuildServiceProvider;
  Check := TDextServices.GetRequiredServiceObject<TNatsHealthCheck>(Provider);
  try
    Should(Check.Options.FlushTimeoutMs).Be(750);
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

{ TDextNatsServicesTests }

function TDextNatsServicesTests.UniqueServiceName(const APrefix: string): string;
begin
  Result := APrefix + '_' + IntToHex(Random(MaxInt), 8);
end;

function TDextNatsServicesTests.EnsureServerOrFail: Boolean;
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
        Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
          'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [NatsTestHost, NatsTestPort, E.Message]));
      Exit;
    end;
  end;
  Result := True;
end;

procedure TDextNatsServicesTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
end;

procedure TDextNatsServicesTests.TearDown;
begin
  FreeAndNil(FClient);
end;

procedure TDextNatsServicesTests.ControlSubject_ShouldBuildAllKindAndInstance;
begin
  Should(NatsServiceControlSubject(svPing)).Be('$SRV.PING');
  Should(NatsServiceControlSubject(svInfo, 'Echo')).Be('$SRV.INFO.Echo');
  Should(NatsServiceControlSubject(svStats, 'Echo', 'abc123')).Be('$SRV.STATS.Echo.abc123');
  Should(NatsServiceVerbToString(svPing)).Be('PING');
end;

procedure TDextNatsServicesTests.ControlSubject_IdWithoutName_ShouldRaise;
begin
  Should(
    procedure
    begin
      NatsServiceControlSubject(svPing, '', 'only-id');
    end).Throw(EDextNatsServiceError);
end;

procedure TDextNatsServicesTests.NameAndSemVer_ShouldValidate;
begin
  Should(NatsServiceIsValidName('EchoService')).BeTrue;
  Should(NatsServiceIsValidName('echo_1-2')).BeTrue;
  Should(NatsServiceIsValidName('')).BeFalse;
  Should(NatsServiceIsValidName('bad.name')).BeFalse;
  Should(NatsServiceIsValidSemVer('1.0.0')).BeTrue;
  Should(NatsServiceIsValidSemVer('1.2.3-alpha.1+build')).BeTrue;
  Should(NatsServiceIsValidSemVer('1.0')).BeFalse;
  Should(NatsServiceIsValidSemVer('')).BeFalse;
end;

procedure TDextNatsServicesTests.Subject_ShouldRejectSpacesAndMisplacedGt;
begin
  Should(NatsServiceIsValidSubject('svc.echo')).BeTrue;
  Should(NatsServiceIsValidSubject('svc.>')).BeTrue;
  Should(NatsServiceIsValidSubject('svc echo')).BeFalse;
  Should(NatsServiceIsValidSubject('svc.>x')).BeFalse;
end;

procedure TDextNatsServicesTests.Config_InvalidName_ShouldRaise;
var
  cfg: TNatsServiceConfig;
begin
  cfg := TNatsServiceConfig.CreateDefault('bad name', '1.0.0');
  Should(
    procedure
    begin
      cfg.Validate;
    end).Throw(EDextNatsServiceError);

  cfg := TNatsServiceConfig.CreateDefault('Good', 'not-semver');
  Should(
    procedure
    begin
      cfg.Validate;
    end).Throw(EDextNatsServiceError);
end;

procedure TDextNatsServicesTests.PingJson_ShouldIncludeTypeAndIdentity;
var
  client: TDextNatsClient;
  svc: TDextNatsService;
  cfg: TNatsServiceConfig;
  json: string;
begin
  client := TDextNatsClient.Create;
  try
    cfg := TNatsServiceConfig.CreateDefault('UnitPing', '1.2.3');
    cfg.Description := 'unit';
    svc := TDextNatsService.AddService(client, cfg);
    try
      json := svc.PingJson;
      Should(json.Contains('"name":"UnitPing"')).BeTrue;
      Should(json.Contains('"version":"1.2.3"')).BeTrue;
      Should(json.Contains('"id":"' + svc.Id + '"')).BeTrue;
      Should(json.Contains('"' + NATS_SRV_PING_RESPONSE_TYPE + '"')).BeTrue;
      Should(json.Contains('"metadata":{')).BeTrue;

      json := svc.InfoJson;
      Should(json.Contains(NATS_SRV_INFO_RESPONSE_TYPE)).BeTrue;
      Should(json.Contains('"description":"unit"')).BeTrue;
      Should(json.Contains('"endpoints":[')).BeTrue;
    finally
      svc.Free;
    end;
  finally
    client.Free;
  end;
end;

procedure TDextNatsServicesTests.JoinSubject_ShouldPrefixAndAllowEmpty;
begin
  Should(NatsServiceJoinSubject('numbers', 'add')).Be('numbers.add');
  Should(NatsServiceJoinSubject('numbers.v1', 'mul')).Be('numbers.v1.mul');
  Should(NatsServiceJoinSubject('', 'bare')).Be('bare');
  Should(NatsServiceJoinSubject('only', '')).Be('only');
end;

procedure TDextNatsServicesTests.AddGroup_ShouldPrefixEndpointSubjectsInInfo;
var
  client: TDextNatsClient;
  svc: TDextNatsService;
  grp, nested: TDextNatsServiceGroup;
  cfg: TNatsServiceConfig;
  gCfg: TNatsGroupConfig;
  json: string;
begin
  client := TDextNatsClient.Create;
  try
    cfg := TNatsServiceConfig.CreateDefault('UnitGroup', '1.0.0');
    svc := TDextNatsService.AddService(client, cfg);
    try
      grp := svc.AddGroup('numbers');
      Should(grp.Prefix).Be('numbers');
      grp.AddEndpoint('add',
        procedure(const ARequest: TNatsServiceRequest)
        begin
          ARequest.Respond('ok');
        end);

      nested := grp.AddGroup('v1');
      Should(nested.Prefix).Be('numbers.v1');
      nested.AddEndpoint('mul',
        procedure(const ARequest: TNatsServiceRequest)
        begin
          ARequest.Respond('ok');
        end);

      gCfg := TNatsGroupConfig.CreateDefault('orders.inventory');
      gCfg.QueueGroup := 'q-inv';
      grp := svc.AddGroup(gCfg);
      Should(grp.Prefix).Be('orders.inventory');
      grp.AddEndpoint('check',
        procedure(const ARequest: TNatsServiceRequest)
        begin
          ARequest.Respond('ok');
        end);

      json := svc.InfoJson;
      Should(json.Contains('"subject":"numbers.add"')).BeTrue;
      Should(json.Contains('"subject":"numbers.v1.mul"')).BeTrue;
      Should(json.Contains('"subject":"orders.inventory.check"')).BeTrue;
      Should(json.Contains('"queue_group":"q-inv"')).BeTrue;
      Should(json.Contains('"name":"add"')).BeTrue;
      Should(json.Contains('"name":"mul"')).BeTrue;
      Should(json.Contains('"name":"check"')).BeTrue;
    finally
      svc.Free;
    end;
  finally
    client.Free;
  end;
end;

procedure TDextNatsServicesTests.GroupConfig_InvalidPrefix_ShouldRaise;
var
  cfg: TNatsGroupConfig;
begin
  cfg := TNatsGroupConfig.CreateDefault('bad prefix');
  Should(
    procedure
    begin
      cfg.Validate;
    end).Throw(EDextNatsServiceError);
end;

procedure TDextNatsServicesTests.AddService_PingDiscovery_ShouldRespond;
var
  cfg: TNatsServiceConfig;
  svc: TDextNatsService;
  name: string;
  reply: TNatsMsg;
  body: string;
begin
  if not EnsureServerOrFail then
    Exit;

  name := UniqueServiceName('DextSrv');
  cfg := TNatsServiceConfig.CreateDefault(name, '1.0.0');
  cfg.Description := 'live ping';
  svc := TDextNatsService.AddService(FClient, cfg);
  try
    FClient.Flush(2000);
    reply := FClient.Request(NatsServiceControlSubject(svPing, name, svc.Id), '', 2000);
    body := reply.AsString;
    Should(body.Contains('"name":"' + name + '"')).BeTrue;
    Should(body.Contains('"id":"' + svc.Id + '"')).BeTrue;
    Should(body.Contains(NATS_SRV_PING_RESPONSE_TYPE)).BeTrue;

    reply := FClient.Request(NatsServiceControlSubject(svInfo, name), '', 2000);
    body := reply.AsString;
    Should(body.Contains(NATS_SRV_INFO_RESPONSE_TYPE)).BeTrue;
    Should(body.Contains('"description":"live ping"')).BeTrue;
  finally
    svc.Free;
  end;
end;

procedure TDextNatsServicesTests.Endpoint_ShouldEchoAndStopUnsubscribes;
var
  cfg: TNatsServiceConfig;
  epCfg: TNatsEndpointConfig;
  svc: TDextNatsService;
  name, subject: string;
  reply: TNatsMsg;
  pingSubj: string;
begin
  if not EnsureServerOrFail then
    Exit;

  name := UniqueServiceName('DextEcho');
  subject := 'dext.svc.' + name.ToLowerInvariant + '.echo';
  cfg := TNatsServiceConfig.CreateDefault(name, '0.1.0');
  svc := TDextNatsService.AddService(FClient, cfg);
  try
    epCfg := TNatsEndpointConfig.CreateDefault('echo', subject);
    svc.AddEndpoint(epCfg,
      procedure(const ARequest: TNatsServiceRequest)
      begin
        ARequest.Respond(ARequest.Data);
      end);
    FClient.Flush(2000);

    reply := FClient.Request(subject, 'hello-svc', 2000);
    Should(reply.AsString).Be('hello-svc');

    reply := FClient.Request(NatsServiceControlSubject(svStats, name, svc.Id), '', 2000);
    Should(reply.AsString.Contains(NATS_SRV_STATS_RESPONSE_TYPE)).BeTrue;
    Should(reply.AsString.Contains('"num_requests":1')).BeTrue;

    pingSubj := NatsServiceControlSubject(svPing, name, svc.Id);
    svc.Stop;
    FClient.Flush(2000);

    Should(
      procedure
      begin
        FClient.Request(pingSubj, '', 1000);
      end).Throw(EDextNatsNoResponders);
  finally
    svc.Free;
  end;
end;

procedure TDextNatsServicesTests.AddGroup_NestedEndpoint_ShouldRespond;
var
  cfg: TNatsServiceConfig;
  svc: TDextNatsService;
  grp, nested: TDextNatsServiceGroup;
  name, subject: string;
  reply: TNatsMsg;
begin
  if not EnsureServerOrFail then
    Exit;

  name := UniqueServiceName('DextGrp');
  cfg := TNatsServiceConfig.CreateDefault(name, '0.2.0');
  svc := TDextNatsService.AddService(FClient, cfg);
  try
    grp := svc.AddGroup('dext.calc');
    nested := grp.AddGroup('v1');
    nested.AddEndpoint('add',
      procedure(const ARequest: TNatsServiceRequest)
      begin
        ARequest.Respond('sum=' + ARequest.AsString);
      end);
    FClient.Flush(2000);

    subject := 'dext.calc.v1.add';
    reply := FClient.Request(subject, '2+3', 2000);
    Should(reply.AsString).Be('sum=2+3');

    reply := FClient.Request(NatsServiceControlSubject(svInfo, name, svc.Id), '', 2000);
    Should(reply.AsString.Contains('"subject":"dext.calc.v1.add"')).BeTrue;
  finally
    svc.Free;
  end;
end;

{ TDextNatsObjectStoreTests }

function TDextNatsObjectStoreTests.UniqueBucket(const APrefix: string): string;
begin
  { Bucket names allow only [A-Za-z0-9_-] }
  Result := APrefix + '_' + IntToHex(Random(MaxInt), 8);
end;

function TDextNatsObjectStoreTests.EnsureJetStreamOrFail: Boolean;
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

  FOs := TDextNatsObjectStoreContext.Create(FClient);
  Result := True;
end;

procedure TDextNatsObjectStoreTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
  FOs := nil;
end;

procedure TDextNatsObjectStoreTests.TearDown;
begin
  FreeAndNil(FOs);
  FreeAndNil(FClient);
end;

procedure TDextNatsObjectStoreTests.ObjectInfo_ParseToJson_ShouldRoundTrip;
var
  info, parsed: TNatsObjectInfo;
  json: string;
begin
  info := Default(TNatsObjectInfo);
  info.Name := 'invoice.pdf';
  info.Description := 'demo';
  info.Headers := nil;
  info.Headers.Add('content-type', 'application/pdf');
  info.Metadata := TCollections.CreateDictionary<string, string>;
  info.Metadata.AddOrSetValue('order', 'ord_8w2k');
  info.Bucket := 'INVOICES';
  info.Nuid := 'ABCDEFGHIJKLMNOPQRSTUV';
  info.Size := 12;
  info.Chunks := 1;
  info.Digest := 'SHA-256=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
  info.ChunkSize := 128 * 1024;
  json := info.ToJson;
  Should(json.Contains('"headers"')).BeTrue;
  Should(json.Contains('"metadata"')).BeTrue;
  parsed := TNatsObjectInfo.Parse(json);
  Should(parsed.Name).Be(info.Name);
  Should(parsed.Description).Be(info.Description);
  Should(parsed.Headers.GetValue('content-type')).Be('application/pdf');
  Should(Assigned(parsed.Metadata)).BeTrue;
  Should(parsed.Metadata['order']).Be('ord_8w2k');
  Should(parsed.Bucket).Be(info.Bucket);
  Should(parsed.Nuid).Be(info.Nuid);
  Should(parsed.Size).Be(info.Size);
  Should(Integer(parsed.Chunks)).Be(Integer(info.Chunks));
  Should(parsed.Digest).Be(info.Digest);
  Should(parsed.ChunkSize).Be(info.ChunkSize);
  Should(parsed.Deleted).BeFalse;
  Should(parsed.IsLink).BeFalse;
end;

procedure TDextNatsObjectStoreTests.ObjectInfo_Link_ParseToJson_ShouldRoundTrip;
var
  info, parsed: TNatsObjectInfo;
  bucketLink: TNatsObjectInfo;
  json: string;
begin
  info := Default(TNatsObjectInfo);
  info.Name := 'label.png';
  info.Bucket := 'LABELS';
  info.Nuid := 'LINKNUID0123456789012';
  info.Link := TNatsObjectLink.Create('INVOICES', 'invoice.pdf');
  json := info.ToJson;
  Should(json.Contains('"options"')).BeTrue;
  Should(json.Contains('"link"')).BeTrue;
  Should(json.Contains('"INVOICES"')).BeTrue;
  Should(json.Contains('"invoice.pdf"')).BeTrue;
  parsed := TNatsObjectInfo.Parse(json);
  Should(parsed.IsLink).BeTrue;
  Should(parsed.IsBucketLink).BeFalse;
  Should(parsed.Link.Bucket).Be('INVOICES');
  Should(parsed.Link.Name).Be('invoice.pdf');
  Should(parsed.Name).Be('label.png');
  Should(parsed.Nuid).Be(info.Nuid);

  bucketLink := Default(TNatsObjectInfo);
  bucketLink.Name := 'cfg';
  bucketLink.Bucket := 'LABELS';
  bucketLink.Nuid := 'BUCKNUID0123456789012';
  bucketLink.Link := TNatsObjectLink.CreateBucket('CONFIG');
  json := bucketLink.ToJson;
  Should(json.Contains('"CONFIG"')).BeTrue;
  parsed := TNatsObjectInfo.Parse(json);
  Should(parsed.IsLink).BeTrue;
  Should(parsed.IsBucketLink).BeTrue;
  Should(parsed.Link.Bucket).Be('CONFIG');
  Should(parsed.Link.Name).Be('');
end;

procedure TDextNatsObjectStoreTests.ObjectInfo_EndOfInitialMarker_ShouldBeEmpty;
var
  info: TNatsObjectInfo;
begin
  info := TNatsObjectInfo.EndOfInitialMarker;
  Should(info.IsEndOfInitial).BeTrue;
  Should(info.EndOfInitial).BeTrue;
  Should(info.Name).Be('');
  Should(info.IsLink).BeFalse;
  Should(info.Deleted).BeFalse;
end;

procedure TDextNatsObjectStoreTests.WatchOptions_ShouldDefaultFalse;
var
  opts: TNatsObjectStoreWatchOptions;
begin
  opts := TNatsObjectStoreWatchOptions.CreateDefault;
  Should(opts.MetaOnly).BeFalse;
  Should(opts.UpdatesOnly).BeFalse;
  Should(opts.IncludeHistory).BeFalse;
  Should(opts.IgnoreDeletes).BeFalse;
end;

procedure TDextNatsObjectStoreTests.WatchOptions_IncludeHistoryWithUpdatesOnly_ShouldRaise;
var
  opts: TNatsObjectStoreWatchOptions;
  raised: Boolean;
begin
  opts := TNatsObjectStoreWatchOptions.CreateDefault;
  opts.IncludeHistory := True;
  opts.UpdatesOnly := True;
  raised := False;
  try
    opts.Validate;
  except
    on E: EDextNatsObjectStoreError do
      raised := True;
  end;
  Should(raised).BeTrue;
end;

procedure TDextNatsObjectStoreTests.GetOptions_ShouldDefaultShowDeletedFalse;
var
  opts: TNatsObjectStoreGetOptions;
begin
  opts := TNatsObjectStoreGetOptions.CreateDefault;
  Should(opts.ShowDeleted).BeFalse;
end;

procedure TDextNatsObjectStoreTests.ListOptions_ShouldDefaultShowDeletedFalse;
var
  opts: TNatsObjectStoreListOptions;
begin
  opts := TNatsObjectStoreListOptions.CreateDefault;
  Should(opts.ShowDeleted).BeFalse;
end;

procedure TDextNatsObjectStoreTests.ObjectStoreConfig_ShouldMapToStreamConfig;
var
  os: TNatsObjectStoreConfig;
  stream: TNatsStreamConfig;
begin
  os := TNatsObjectStoreConfig.CreateDefault('INVOICES');
  os.Description := 'billing blobs';
  os.MaxBytes := 1048576;
  os.MaxAge := 3600000000000; { 1h }
  os.Storage := ssMemory;
  os.NumReplicas := 1;
  os.ChunkSize := 0;
  os.Compression := scS2;
  os.Placement.Cluster := 'east';
  os.Placement.Tags := ['obj:ssd'];
  stream := os.ToStreamConfig;
  Should(stream.Name).Be('OBJ_INVOICES');
  Should(Length(stream.Subjects)).Be(2);
  Should(stream.Subjects[0]).Be('$O.INVOICES.C.>');
  Should(stream.Subjects[1]).Be('$O.INVOICES.M.>');
  Should(stream.Description).Be('billing blobs');
  Should(stream.MaxBytes).Be(1048576);
  Should(stream.MaxAge).Be(3600000000000);
  Should(Ord(stream.Storage)).Be(Ord(ssMemory));
  Should(stream.NumReplicas).Be(1);
  Should(Ord(stream.Discard)).Be(Ord(sdNew));
  Should(stream.AllowRollup).BeTrue;
  Should(stream.AllowDirect).BeTrue;
  Should(Ord(stream.Compression)).Be(Ord(scS2));
  Should(stream.Placement.Cluster).Be('east');
  Should(Length(stream.Placement.Tags)).Be(1);
  Should(stream.Placement.Tags[0]).Be('obj:ssd');
  Should(os.EffectiveChunkSize).Be(NATS_OBJ_DEFAULT_CHUNK_SIZE);

  os.MaxBytes := 0;
  stream := os.ToStreamConfig;
  Should(stream.MaxBytes).Be(-1);
end;

procedure TDextNatsObjectStoreTests.Store_CreatePutGetDelete_ShouldRoundTrip;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  putInfo, getInfo: TNatsObjectInfo;
  payload, got: TBytes;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJ');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.ChunkSize := 8; // force multi-chunk for a small payload
  store := FOs.CreateStore(cfg);
  try
    SetLength(payload, 20);
    for i := 0 to High(payload) do
      payload[i] := Byte(i + 1);

    putInfo := store.Put('docs/a.bin', payload);
    Should(putInfo.Name).Be('docs/a.bin');
    Should(putInfo.Bucket).Be(bucket);
    Should(putInfo.Size).Be(UInt64(Length(payload)));
    Should(Integer(putInfo.Chunks)).Be(3); // 20 bytes / 8-byte chunks
    Should(putInfo.Digest.StartsWith('SHA-256=')).BeTrue;

    got := store.Get('docs/a.bin', getInfo);
    Should(Length(got)).Be(Length(payload));
    Should(CompareMem(@got[0], @payload[0], Length(payload))).BeTrue;
    Should(getInfo.Digest).Be(putInfo.Digest);

    store.Delete('docs/a.bin');
    Should(
      procedure
      begin
        store.Get('docs/a.bin');
      end).Throw(EDextNatsObjectStoreError);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Store_PutGet_StreamAndFile_ShouldRoundTrip;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  putInfo, getInfo, fileInfo, metaInfo: TNatsObjectInfo;
  payload, got: TBytes;
  src, dst: TMemoryStream;
  meta: TNatsObjectMeta;
  inFile, outFile: string;
  i: Integer;
  fs: TFileStream;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJST');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.ChunkSize := 16;
  store := FOs.CreateStore(cfg);
  try
    SetLength(payload, 40);
    for i := 0 to High(payload) do
      payload[i] := Byte((i * 3) + 1);

    src := TMemoryStream.Create;
    dst := TMemoryStream.Create;
    try
      src.WriteBuffer(payload[0], Length(payload));
      src.Position := 0;
      putInfo := store.Put('stream.bin', src);
      Should(putInfo.Size).Be(UInt64(Length(payload)));
      Should(Integer(putInfo.Chunks)).Be(3);

      getInfo := store.Get('stream.bin', dst);
      Should(getInfo.Digest).Be(putInfo.Digest);
      Should(dst.Size).Be(Length(payload));
      SetLength(got, dst.Size);
      dst.Position := 0;
      dst.ReadBuffer(got[0], Length(got));
      Should(CompareMem(@got[0], @payload[0], Length(payload))).BeTrue;
    finally
      dst.Free;
      src.Free;
    end;

    meta := TNatsObjectMeta.Create('meta.bin');
    meta.Description := 'from-stream-meta';
    meta.Headers.Add('X-Doc', 'yes');
    src := TMemoryStream.Create;
    try
      src.WriteBuffer(payload[0], Length(payload));
      src.Position := 0;
      metaInfo := store.Put(meta, src);
      Should(metaInfo.Name).Be('meta.bin');
      Should(metaInfo.Description).Be('from-stream-meta');
      Should(metaInfo.Headers.GetValue('X-Doc')).Be('yes');
    finally
      src.Free;
    end;

    inFile := TPath.Combine(TPath.GetTempPath, 'dext_os_put_' +
      IntToHex(Random(MaxInt), 8) + '.bin');
    outFile := TPath.Combine(TPath.GetTempPath, 'dext_os_get_' +
      IntToHex(Random(MaxInt), 8) + '.bin');
    fs := TFileStream.Create(inFile, fmCreate);
    try
      fs.WriteBuffer(payload[0], Length(payload));
    finally
      fs.Free;
    end;
    try
      fileInfo := store.PutFile('file.bin', inFile);
      Should(fileInfo.Size).Be(UInt64(Length(payload)));
      getInfo := store.GetFile('file.bin', outFile);
      Should(getInfo.Digest).Be(fileInfo.Digest);

      fs := TFileStream.Create(outFile, fmOpenRead or fmShareDenyWrite);
      try
        SetLength(got, fs.Size);
        if Length(got) > 0 then
          fs.ReadBuffer(got[0], Length(got));
      finally
        fs.Free;
      end;
      Should(Length(got)).Be(Length(payload));
      Should(CompareMem(@got[0], @payload[0], Length(payload))).BeTrue;

      fileInfo := store.PutFile(inFile);
      Should(fileInfo.Name).Be(ExtractFileName(inFile));
      got := store.Get(fileInfo.Name);
      Should(Length(got)).Be(Length(payload));
    finally
      if FileExists(inFile) then
        DeleteFile(inFile);
      if FileExists(outFile) then
        DeleteFile(outFile);
    end;
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.GetResult_ShouldStreamChunksLazily;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  putInfo: TNatsObjectInfo;
  payload, got: TBytes;
  reader: TDextNatsObjectResult;
  buf: array[0..4] of Byte;
  n, total, i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJLR');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.ChunkSize := 8;
  store := FOs.CreateStore(cfg);
  try
    SetLength(payload, 25);
    for i := 0 to High(payload) do
      payload[i] := Byte(i + 1);
    putInfo := store.Put('lazy.bin', payload);
    Should(Integer(putInfo.Chunks)).Be(4);

    reader := store.GetResult('lazy.bin');
    try
      Should(reader.Info.Digest).Be(putInfo.Digest);
      Should(reader.Size).Be(Int64(Length(payload)));
      Should(reader.Position).Be(0);
      SetLength(got, Length(payload));
      total := 0;
      while total < Length(payload) do
      begin
        n := reader.Read(buf[0], Length(buf));
        Should(n).BeGreaterThan(0);
        Move(buf[0], got[total], n);
        Inc(total, n);
      end;
      Should(total).Be(Length(payload));
      Should(reader.Position).Be(Int64(Length(payload)));
      Should(reader.Read(buf[0], Length(buf))).Be(0);
      Should(CompareMem(@got[0], @payload[0], Length(payload))).BeTrue;
      Should(reader.Failed).BeFalse;
    finally
      reader.Free;
    end;
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.GetResult_ShowDeleted_ShouldEofEmpty;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  opts: TNatsObjectStoreGetOptions;
  reader: TDextNatsObjectResult;
  buf: array[0..7] of Byte;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJLD');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('tomb.bin', TEncoding.UTF8.GetBytes('soon-gone'));
    store.Delete('tomb.bin');

    opts := TNatsObjectStoreGetOptions.CreateDefault;
    opts.ShowDeleted := True;
    reader := store.GetResult('tomb.bin', opts);
    try
      Should(reader.Info.Deleted).BeTrue;
      Should(reader.Info.Name).Be('tomb.bin');
      Should(reader.Size).Be(0);
      Should(reader.Read(buf[0], Length(buf))).Be(0);
      Should(reader.Failed).BeFalse;
    finally
      reader.Free;
    end;
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Store_PutOverwrite_ShouldReturnLatest;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  got: TBytes;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJ2');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('x', TEncoding.UTF8.GetBytes('one'));
    store.Put('x', TEncoding.UTF8.GetBytes('two'));
    got := store.Get('x');
    Should(TEncoding.UTF8.GetString(got)).Be('two');
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Get_MissingObject_ShouldRaise;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJ3');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    Should(
      procedure
      begin
        store.Get('missing');
      end).Throw(EDextNatsObjectStoreError);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.GetInfo_ShowDeleted_ShouldReturnTombstone;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  opts: TNatsObjectStoreGetOptions;
  info: TNatsObjectInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJSD');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('tomb.txt', TEncoding.UTF8.GetBytes('bye'));
    store.Delete('tomb.txt');

    Should(
      procedure
      begin
        store.GetInfo('tomb.txt');
      end).Throw(EDextNatsObjectStoreError);

    opts := TNatsObjectStoreGetOptions.CreateDefault;
    opts.ShowDeleted := True;
    info := store.GetInfo('tomb.txt', opts);
    Should(info.Name).Be('tomb.txt');
    Should(info.Deleted).BeTrue;
    Should(info.Size).Be(UInt64(0));
    Should(Integer(info.Chunks)).Be(0);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Get_ShowDeleted_ShouldReturnEmptyAfterDelete;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  opts: TNatsObjectStoreGetOptions;
  info: TNatsObjectInfo;
  got: TBytes;
  ms: TBytesStream;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJGD');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('gone.bin', TEncoding.UTF8.GetBytes('payload'));
    store.Delete('gone.bin');

    Should(
      procedure
      begin
        store.Get('gone.bin');
      end).Throw(EDextNatsObjectStoreError);

    opts := TNatsObjectStoreGetOptions.CreateDefault;
    opts.ShowDeleted := True;
    got := store.Get('gone.bin', info, opts);
    Should(Length(got)).Be(0);
    Should(info.Deleted).BeTrue;
    Should(info.Name).Be('gone.bin');

    ms := TBytesStream.Create;
    try
      info := store.Get('gone.bin', ms, opts);
      Should(ms.Size).Be(0);
      Should(info.Deleted).BeTrue;
    finally
      ms.Free;
    end;
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Put_AfterDelete_ShouldOverwrite;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  putInfo, getInfo: TNatsObjectInfo;
  got: TBytes;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  { Put has no public ShowDeleted option (nats.go); it already sees tombstones
    via TryGetInfo so a re-Put after Delete replaces the name cleanly. }
  bucket := UniqueBucket('DEXTOBJPD');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('reuse.txt', TEncoding.UTF8.GetBytes('old'));
    store.Delete('reuse.txt');
    putInfo := store.Put('reuse.txt', TEncoding.UTF8.GetBytes('new'));
    Should(putInfo.Deleted).BeFalse;
    Should(putInfo.Size).Be(UInt64(3));
    got := store.Get('reuse.txt', getInfo);
    Should(TEncoding.UTF8.GetString(got)).Be('new');
    Should(getInfo.Deleted).BeFalse;
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Store_ListAndKeys_ShouldReturnLiveObjects;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  infos, withDeleted: IList<TNatsObjectInfo>;
  names: IList<string>;
  listOpts: TNatsObjectStoreListOptions;
  i: Integer;
  sawA, sawB, sawDeleted: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJL');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('docs/a.txt', TEncoding.UTF8.GetBytes('aaa'));
    store.Put('docs/b.txt', TEncoding.UTF8.GetBytes('bbb'));
    store.Put('gone.bin', TEncoding.UTF8.GetBytes('zzz'));
    store.Delete('gone.bin');

    infos := store.List;
    Should(infos.Count).Be(2);
    sawA := False;
    sawB := False;
    for i := 0 to infos.Count - 1 do
    begin
      Should(infos[i].Deleted).BeFalse;
      if infos[i].Name = 'docs/a.txt' then
      begin
        sawA := True;
        Should(infos[i].Size).Be(UInt64(3));
      end
      else if infos[i].Name = 'docs/b.txt' then
        sawB := True;
    end;
    Should(sawA).BeTrue;
    Should(sawB).BeTrue;

    names := store.Keys;
    Should(names.Count).Be(2);
    sawA := False;
    sawB := False;
    for i := 0 to names.Count - 1 do
    begin
      if names[i] = 'docs/a.txt' then
        sawA := True
      else if names[i] = 'docs/b.txt' then
        sawB := True;
    end;
    Should(sawA).BeTrue;
    Should(sawB).BeTrue;

    withDeleted := store.ListObjects(True);
    Should(withDeleted.Count).Be(3);
    sawDeleted := False;
    for i := 0 to withDeleted.Count - 1 do
      if (withDeleted[i].Name = 'gone.bin') and withDeleted[i].Deleted then
        sawDeleted := True;
    Should(sawDeleted).BeTrue;

    listOpts := TNatsObjectStoreListOptions.CreateDefault;
    listOpts.ShowDeleted := True;
    withDeleted := store.List(listOpts);
    Should(withDeleted.Count).Be(3);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Store_List_EmptyBucket_ShouldReturnEmpty;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJE');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    Should(store.List.Count).Be(0);
    Should(store.Keys.Count).Be(0);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_ShouldDeliverCurrentAndUpdates;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  bucket: string;
  lock: TCriticalSection;
  got: IList<string>;
  gotInitial, gotUpdate: TEvent;
  i: Integer;
  sawA, sawB, sawA2: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJW');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotInitial := TEvent.Create(nil, True, False, '');
  gotUpdate := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    store.Put('a.txt', TEncoding.UTF8.GetBytes('one'));
    store.Put('b.txt', TEncoding.UTF8.GetBytes('two'));

    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        if AInfo.IsEndOfInitial or AInfo.Deleted then
          Exit;
        lock.Enter;
        try
          got.Add(AInfo.Name + '=' + IntToStr(Integer(AInfo.Size)));
          if got.Count >= 2 then
            gotInitial.SetEvent;
          if (AInfo.Name = 'a.txt') and (AInfo.Size = 5) then
            gotUpdate.SetEvent;
        finally
          lock.Leave;
        end;
      end);

    Should(gotInitial.WaitFor(5000) = wrSignaled).BeTrue;
    store.Put('a.txt', TEncoding.UTF8.GetBytes('three'));
    Should(gotUpdate.WaitFor(5000) = wrSignaled).BeTrue;

    sawA := False;
    sawB := False;
    sawA2 := False;
    lock.Enter;
    try
      for i := 0 to got.Count - 1 do
      begin
        if got[i] = 'a.txt=3' then
          sawA := True;
        if got[i] = 'b.txt=3' then
          sawB := True;
        if got[i] = 'a.txt=5' then
          sawA2 := True;
      end;
    finally
      lock.Leave;
    end;
    Should(sawA).BeTrue;
    Should(sawB).BeTrue;
    Should(sawA2).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotInitial.Free;
    gotUpdate.Free;
    lock.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_ShouldSignalEndOfInitial;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  bucket: string;
  lock: TCriticalSection;
  gotNames: IList<string>;
  markerAt: Integer;
  gotMarker, gotUpdate: TEvent;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJEOI');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  lock := TCriticalSection.Create;
  gotNames := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  gotUpdate := TEvent.Create(nil, True, False, '');
  watcher := nil;
  markerAt := -1;
  try
    store.Put('a.txt', TEncoding.UTF8.GetBytes('one'));
    store.Put('b.txt', TEncoding.UTF8.GetBytes('two'));

    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        lock.Enter;
        try
          if AInfo.IsEndOfInitial then
          begin
            if markerAt < 0 then
              markerAt := gotNames.Count;
            gotMarker.SetEvent;
          end
          else if not AInfo.Deleted then
          begin
            gotNames.Add(AInfo.Name + '=' + IntToStr(Integer(AInfo.Size)));
            if (AInfo.Name = 'a.txt') and (AInfo.Size = 5) then
              gotUpdate.SetEvent;
          end;
        finally
          lock.Leave;
        end;
      end);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    lock.Enter;
    try
      Should(markerAt >= 0).BeTrue;
      Should(markerAt >= 2).BeTrue;
      for i := 0 to markerAt - 1 do
        Should((gotNames[i] = 'a.txt=3') or (gotNames[i] = 'b.txt=3')).BeTrue;
    finally
      lock.Leave;
    end;

    store.Put('a.txt', TEncoding.UTF8.GetBytes('three'));
    Should(gotUpdate.WaitFor(5000) = wrSignaled).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    gotUpdate.Free;
    lock.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_EmptyBucket_ShouldSignalEndOfInitial;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  bucket: string;
  gotMarker: TEvent;
  sawEntry: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJEMP');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  sawEntry := False;
  try
    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        if AInfo.IsEndOfInitial then
          gotMarker.SetEvent
        else
          sawEntry := True;
      end);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    Should(sawEntry).BeFalse;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_UpdatesOnly_ShouldSkipInitialAndMarker;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  opts: TNatsObjectStoreWatchOptions;
  bucket: string;
  lock: TCriticalSection;
  got: IList<string>;
  gotUpdate: TEvent;
  sawMarker: Boolean;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJUO');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  lock := TCriticalSection.Create;
  got := TCollections.CreateList<string>;
  gotUpdate := TEvent.Create(nil, True, False, '');
  watcher := nil;
  sawMarker := False;
  try
    store.Put('seed.txt', TEncoding.UTF8.GetBytes('old-value')); { size 9 }
    opts := TNatsObjectStoreWatchOptions.CreateDefault;
    opts.UpdatesOnly := True;

    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        lock.Enter;
        try
          if AInfo.IsEndOfInitial then
            sawMarker := True
          else if not AInfo.Deleted then
          begin
            got.Add(AInfo.Name + '=' + IntToStr(Integer(AInfo.Size)));
            if (AInfo.Name = 'seed.txt') and (AInfo.Size = 1) then
              gotUpdate.SetEvent;
          end;
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(watcher.InitialDone).BeTrue;
    Sleep(300);
    store.Put('seed.txt', TEncoding.UTF8.GetBytes('x')); { size 1 }
    Should(gotUpdate.WaitFor(5000) = wrSignaled).BeTrue;

    lock.Enter;
    try
      Should(sawMarker).BeFalse;
      Should(got.Count > 0).BeTrue;
      for i := 0 to got.Count - 1 do
        Should(got[i] <> 'seed.txt=9').BeTrue;
      Should(got[got.Count - 1]).Be('seed.txt=1');
    finally
      lock.Leave;
    end;
  finally
    if watcher <> nil then
      watcher.Free;
    gotUpdate.Free;
    lock.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_MetaOnly_ShouldOmitPayload;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  opts: TNatsObjectStoreWatchOptions;
  bucket: string;
  lock: TCriticalSection;
  gotName: string;
  gotSize: Int64;
  gotMarker: TEvent;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJMO');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  lock := TCriticalSection.Create;
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  gotName := '';
  gotSize := -1;
  try
    store.Put('meta.bin', TEncoding.UTF8.GetBytes('secret-payload'));
    opts := TNatsObjectStoreWatchOptions.CreateDefault;
    opts.MetaOnly := True;

    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        if AInfo.IsEndOfInitial then
        begin
          gotMarker.SetEvent;
          Exit;
        end;
        if AInfo.Deleted then
          Exit;
        lock.Enter;
        try
          gotName := AInfo.Name;
          gotSize := Int64(AInfo.Size);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    lock.Enter;
    try
      Should(gotName).Be('meta.bin');
      { headers_only: ObjectInfo JSON omitted → Size stays 0 }
      Should(gotSize).Be(0);
    finally
      lock.Leave;
    end;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    lock.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_IgnoreDeletes_ShouldSkipDeleted;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  opts: TNatsObjectStoreWatchOptions;
  bucket: string;
  lock: TCriticalSection;
  gotNames: IList<string>;
  gotMarker, gotLiveDel: TEvent;
  i: Integer;
  sawGone: Boolean;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJID');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  lock := TCriticalSection.Create;
  gotNames := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  gotLiveDel := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    store.Put('keep.bin', TEncoding.UTF8.GetBytes('1'));
    store.Put('gone.bin', TEncoding.UTF8.GetBytes('2'));
    store.Delete('gone.bin');
    opts := TNatsObjectStoreWatchOptions.CreateDefault;
    opts.IgnoreDeletes := True;

    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        lock.Enter;
        try
          if AInfo.IsEndOfInitial then
            gotMarker.SetEvent
          else if AInfo.Deleted then
          begin
            gotNames.Add('DEL:' + AInfo.Name);
            gotLiveDel.SetEvent;
          end
          else if AInfo.Name <> '' then
            gotNames.Add('PUT:' + AInfo.Name);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    store.Delete('keep.bin');
    Should(gotLiveDel.WaitFor(800) = wrTimeout).BeTrue;

    sawGone := False;
    lock.Enter;
    try
      for i := 0 to gotNames.Count - 1 do
      begin
        if Pos('gone.bin', gotNames[i]) > 0 then
          sawGone := True;
        if gotNames[i] = 'PUT:keep.bin' then
          Break;
      end;
      Should(gotNames.IndexOf('PUT:keep.bin') >= 0).BeTrue;
    finally
      lock.Leave;
    end;
    Should(sawGone).BeFalse;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    gotLiveDel.Free;
    lock.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.WatchAll_IncludeHistory_ShouldReplayMeta;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  watcher: TDextNatsObjectStoreWatcher;
  opts: TNatsObjectStoreWatchOptions;
  bucket: string;
  lock: TCriticalSection;
  gotNames: IList<string>;
  gotMarker: TEvent;
  sawA, sawB: Boolean;
  i: Integer;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  { Object Store meta uses subject rollup, so IncludeHistory rarely sees multiple
    revisions per object — still exercise deliver_policy=all + EndOfInitial. }
  bucket := UniqueBucket('DEXTOBJIH');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Storage := ssMemory;
  store := FOs.CreateStore(cfg);
  lock := TCriticalSection.Create;
  gotNames := TCollections.CreateList<string>;
  gotMarker := TEvent.Create(nil, True, False, '');
  watcher := nil;
  try
    store.Put('a.bin', TEncoding.UTF8.GetBytes('a'));
    store.Put('b.bin', TEncoding.UTF8.GetBytes('b'));
    opts := TNatsObjectStoreWatchOptions.CreateDefault;
    opts.IncludeHistory := True;

    watcher := store.WatchAll(
      procedure(const AInfo: TNatsObjectInfo)
      begin
        lock.Enter;
        try
          if AInfo.IsEndOfInitial then
            gotMarker.SetEvent
          else if (not AInfo.Deleted) and (AInfo.Name <> '') then
            gotNames.Add(AInfo.Name);
        finally
          lock.Leave;
        end;
      end,
      opts);

    Should(gotMarker.WaitFor(5000) = wrSignaled).BeTrue;
    Should(watcher.InitialDone).BeTrue;
    sawA := False;
    sawB := False;
    lock.Enter;
    try
      for i := 0 to gotNames.Count - 1 do
      begin
        if gotNames[i] = 'a.bin' then
          sawA := True;
        if gotNames[i] = 'b.bin' then
          sawB := True;
      end;
    finally
      lock.Leave;
    end;
    Should(sawA).BeTrue;
    Should(sawB).BeTrue;
  finally
    if watcher <> nil then
      watcher.Free;
    gotMarker.Free;
    lock.Free;
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.UpdateMeta_ShouldChangeDescriptionHeadersAndRename;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  meta: TNatsObjectMeta;
  info: TNatsObjectInfo;
  got: TBytes;
  nuid: string;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJU');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    info := store.Put('old-name.bin', TEncoding.UTF8.GetBytes('payload'));
    nuid := info.Nuid;

    meta := TNatsObjectMeta.Create('new-name.bin');
    meta.Description := 'renamed invoice';
    meta.Headers := nil;
    meta.Headers.Add('content-type', 'application/octet-stream');
    meta.Metadata := TCollections.CreateDictionary<string, string>;
    meta.Metadata.AddOrSetValue('k', 'v');
    info := store.UpdateMeta('old-name.bin', meta);

    Should(info.Name).Be('new-name.bin');
    Should(info.Description).Be('renamed invoice');
    Should(info.Nuid).Be(nuid);
    Should(info.Headers.GetValue('content-type')).Be('application/octet-stream');
    Should(Assigned(info.Metadata)).BeTrue;
    Should(info.Metadata['k']).Be('v');

    got := store.Get('new-name.bin', info);
    Should(TEncoding.UTF8.GetString(got)).Be('payload');
    Should(info.Description).Be('renamed invoice');
    Should(info.Nuid).Be(nuid);

    Should(
      procedure
      begin
        store.Get('old-name.bin');
      end).Throw(EDextNatsObjectStoreError);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.UpdateMeta_DeletedOrConflict_ShouldRaise;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  metaDeleted, metaMissing, metaConflict: TNatsObjectMeta;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJN');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('a.bin', TEncoding.UTF8.GetBytes('a'));
    store.Put('b.bin', TEncoding.UTF8.GetBytes('b'));
    store.Delete('a.bin');

    metaDeleted := TNatsObjectMeta.Create('a.bin');
    metaDeleted.Description := 'gone';
    Should(
      procedure
      begin
        store.UpdateMeta('a.bin', metaDeleted);
      end).Throw(EDextNatsObjectStoreError);

    metaMissing := TNatsObjectMeta.Create('nope.bin');
    Should(
      procedure
      begin
        store.UpdateMeta('missing.bin', metaMissing);
      end).Throw(EDextNatsObjectStoreError);

    { Rename onto a live name must fail. }
    store.Put('c.bin', TEncoding.UTF8.GetBytes('c'));
    metaConflict := TNatsObjectMeta.Create('b.bin');
    metaConflict.Description := 'takeover';
    Should(
      procedure
      begin
        store.UpdateMeta('c.bin', metaConflict);
      end).Throw(EDextNatsObjectStoreError);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.Seal_ShouldRejectFurtherMutations;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  meta: TNatsObjectMeta;
  got: TBytes;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJS');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    store.Put('keep.bin', TEncoding.UTF8.GetBytes('keep'));
    Should(store.IsSealed).BeFalse;
    store.Seal;
    Should(store.IsSealed).BeTrue;
    store.Seal; // idempotent

    got := store.Get('keep.bin');
    Should(TEncoding.UTF8.GetString(got)).Be('keep');

    Should(
      procedure
      begin
        store.Put('more.bin', TEncoding.UTF8.GetBytes('nope'));
      end).Throw(EDextNatsJetStreamError);

    meta := TNatsObjectMeta.Create('keep.bin');
    meta.Description := 'sealed';
    Should(
      procedure
      begin
        store.UpdateMeta('keep.bin', meta);
      end).Throw(EDextNatsJetStreamError);

    Should(
      procedure
      begin
        store.Delete('keep.bin');
      end).Throw(EDextNatsJetStreamError);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.UpdateStore_ShouldChangeDescriptionMaxBytesAndTTL;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  info: TNatsStreamInfo;
  js: TDextNatsJetStreamContext;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJU');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  cfg.Description := 'initial';
  cfg.MaxBytes := 2 * 1024 * 1024;
  store := FOs.CreateStore(cfg);
  store.Free;

  cfg.Description := 'updated-os';
  cfg.MaxBytes := 4 * 1024 * 1024;
  cfg.MaxAge := 7200000000000; { 2h TTL }
  store := FOs.UpdateObjectStore(cfg);
  try
    js := FOs.JetStream;
    info := js.GetStreamInfo('OBJ_' + bucket);
    Should(info.Config.Description).Be('updated-os');
    Should(info.Config.MaxBytes).Be(4 * 1024 * 1024);
    Should(info.Config.MaxAge).Be(7200000000000);
    Should(info.Config.AllowRollup).BeTrue;
    Should(info.Config.AllowDirect).BeTrue;
    Should(Length(info.Config.Subjects)).Be(2);

    { Put still works after config update }
    store.Put('alive.bin', TEncoding.UTF8.GetBytes('ok'));
    Should(TEncoding.UTF8.GetString(store.Get('alive.bin'))).Be('ok');
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.UpdateStore_MissingBucket_ShouldRaise;
var
  cfg: TNatsObjectStoreConfig;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  cfg := TNatsObjectStoreConfig.CreateDefault(UniqueBucket('DEXTOBJX'));
  cfg.Description := 'missing';
  Should(
    procedure
    begin
      FOs.UpdateStore(cfg).Free;
    end).Throw(EDextNatsJetStreamError);
end;

procedure TDextNatsObjectStoreTests.AddLink_Get_ShouldFollowSameBucket;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  target, linkInfo, gotInfo: TNatsObjectInfo;
  payload, got: TBytes;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJS');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    payload := TEncoding.UTF8.GetBytes('invoice-bytes');
    target := store.Put('invoice.pdf', payload);
    linkInfo := store.AddLink('label.png', target);
    Should(linkInfo.IsLink).BeTrue;
    Should(linkInfo.Link.Bucket).Be(bucket);
    Should(linkInfo.Link.Name).Be('invoice.pdf');

    gotInfo := store.GetInfo('label.png');
    Should(gotInfo.IsLink).BeTrue;
    Should(gotInfo.Name).Be('label.png');

    got := store.Get('label.png', gotInfo);
    Should(TEncoding.UTF8.GetString(got)).Be('invoice-bytes');
    Should(gotInfo.IsLink).BeFalse;
    Should(gotInfo.Name).Be('invoice.pdf');
    Should(gotInfo.Digest).Be(target.Digest);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

procedure TDextNatsObjectStoreTests.AddLink_CrossBucket_Get_ShouldFollow;
var
  cfgA, cfgB: TNatsObjectStoreConfig;
  storeA, storeB: TDextNatsObjectStore;
  bucketA, bucketB: string;
  target, linkInfo, gotInfo: TNatsObjectInfo;
  got: TBytes;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucketA := UniqueBucket('DEXTOBJA');
  bucketB := UniqueBucket('DEXTOBJB');
  cfgA := TNatsObjectStoreConfig.CreateDefault(bucketA);
  cfgB := TNatsObjectStoreConfig.CreateDefault(bucketB);
  storeA := FOs.CreateStore(cfgA);
  storeB := FOs.CreateStore(cfgB);
  try
    target := storeA.Put('shared.json', TEncoding.UTF8.GetBytes('{"ok":true}'));
    linkInfo := storeB.AddLink('alias.json', target);
    Should(linkInfo.Link.Bucket).Be(bucketA);
    Should(linkInfo.Link.Name).Be('shared.json');

    got := storeB.Get('alias.json', gotInfo);
    Should(TEncoding.UTF8.GetString(got)).Be('{"ok":true}');
    Should(gotInfo.Bucket).Be(bucketA);
    Should(gotInfo.Name).Be('shared.json');
  finally
    storeB.Free;
    storeA.Free;
    FOs.DeleteStore(bucketB);
    FOs.DeleteStore(bucketA);
  end;
end;

procedure TDextNatsObjectStoreTests.AddBucketLink_Get_ShouldRaise;
var
  cfgA, cfgB: TNatsObjectStoreConfig;
  storeA, storeB: TDextNatsObjectStore;
  bucketA, bucketB: string;
  linkInfo: TNatsObjectInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucketA := UniqueBucket('DEXTOBJA');
  bucketB := UniqueBucket('DEXTOBJB');
  cfgA := TNatsObjectStoreConfig.CreateDefault(bucketA);
  cfgB := TNatsObjectStoreConfig.CreateDefault(bucketB);
  storeA := FOs.CreateStore(cfgA);
  storeB := FOs.CreateStore(cfgB);
  try
    linkInfo := storeB.AddBucketLink('other-store', storeA);
    Should(linkInfo.IsBucketLink).BeTrue;
    Should(linkInfo.Link.Bucket).Be(bucketA);
    Should(linkInfo.Link.Name).Be('');

    Should(
      procedure
      begin
        storeB.Get('other-store');
      end).Throw(EDextNatsObjectStoreError);
  finally
    storeB.Free;
    storeA.Free;
    FOs.DeleteStore(bucketB);
    FOs.DeleteStore(bucketA);
  end;
end;

procedure TDextNatsObjectStoreTests.AddLink_DeletedOrLinkTarget_ShouldRaise;
var
  cfg: TNatsObjectStoreConfig;
  store: TDextNatsObjectStore;
  bucket: string;
  target, linkInfo, deletedTarget: TNatsObjectInfo;
begin
  if not EnsureJetStreamOrFail then
    Exit;

  bucket := UniqueBucket('DEXTOBJS');
  cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
  store := FOs.CreateStore(cfg);
  try
    target := store.Put('real.bin', TEncoding.UTF8.GetBytes('data'));
    linkInfo := store.AddLink('alias.bin', target);

    Should(
      procedure
      begin
        store.AddLink('alias2.bin', linkInfo);
      end).Throw(EDextNatsObjectStoreError);

    deletedTarget := Default(TNatsObjectInfo);
    deletedTarget.Name := 'real.bin';
    deletedTarget.Bucket := bucket;
    deletedTarget.Deleted := True;
    Should(
      procedure
      begin
        store.AddLink('gone.bin', deletedTarget);
      end).Throw(EDextNatsObjectStoreError);

    store.Put('occupied.bin', TEncoding.UTF8.GetBytes('taken'));
    Should(
      procedure
      begin
        store.AddLink('occupied.bin', target);
      end).Throw(EDextNatsObjectStoreError);
  finally
    store.Free;
    FOs.DeleteStore(bucket);
  end;
end;

end.
