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
{                                                                           }
{***************************************************************************}
{                                                                           }
{  JetStream layer for the NATS client. Stream admin (create/update/info/   }
{  delete, ListStreams / ListStreamNames), dedup'd publish with a           }
{  Nats-Msg-Id header, pull-consumer admin (create/info/delete,             }
{  ListConsumers / ListConsumerNames), Fetch, push SubscribePush on         }
{  deliver_subject, ordered SubscribeOrdered (ADR-17 push helper), and      }
{  Ack/Nak/Term/InProgress — all built on plain request/reply and PUB       }
{  against $JS.API.* subjects.                                              }
{  TDextNatsJetStreamContext wraps an already-connected TDextNatsClient     }
{  (composition); it neither owns nor frees the client.                     }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Collections,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats;

type
  /// <summary>Raised when a JetStream API call replies with a JSON "error" object.</summary>
  EDextNatsJetStreamError = class(EDextNatsException)
  public
    /// <summary>HTTP-style status code reported by the JetStream API (e.g. 400, 404).</summary>
    Code: Integer;
    /// <summary>NATS-specific error code (e.g. 10059 for "stream not found").</summary>
    ErrCode: Integer;
    constructor CreateFromApi(ACode, AErrCode: Integer; const ADescription: string);
  end;

  /// <summary>Message retention policy for a JetStream stream.</summary>
  TNatsStreamRetention = (srLimits, srInterest, srWorkQueue);
  /// <summary>Storage backend for a JetStream stream.</summary>
  TNatsStreamStorage = (ssFile, ssMemory);
  /// <summary>What the server does when a stream's limits are reached.</summary>
  TNatsStreamDiscard = (sdOld, sdNew);
  /// <summary>
  ///   On-disk message block compression (STREAM.CREATE "compression").
  ///   <c>scNone</c> omits the field; <c>scS2</c> emits <c>"s2"</c> (nats.go S2Compression).
  /// </summary>
  TNatsStoreCompression = (scNone, scS2);

  /// <summary>
  ///   Cluster placement directives (STREAM.CREATE "placement").
  ///   Emitted only when <see cref="IsSet"/> (cluster and/or tags).
  /// </summary>
  TNatsPlacement = record
    /// <summary>Target cluster name (JSON "cluster").</summary>
    Cluster: string;
    /// <summary>Server tags that must all match (JSON "tags").</summary>
    Tags: TArray<string>;
    /// <summary>True when Cluster is non-empty or Tags has at least one entry.</summary>
    function IsSet: Boolean;
  end;

  /// <summary>Consumer acknowledgement policy.</summary>
  TNatsAckPolicy = (apNone, apAll, apExplicit);
  /// <summary>Which messages a consumer starts from.</summary>
  TNatsDeliverPolicy = (dpAll, dpLast, dpNew, dpByStartSequence, dpByStartTime, dpLastPerSubject);
  /// <summary>Replay timing for a consumer.</summary>
  TNatsReplayPolicy = (rpInstant, rpOriginal);

  /// <summary>Configuration used to create or describe a JetStream stream.</summary>
  TNatsStreamConfig = record
    Name: string;
    /// <summary>Optional human-readable description (STREAM.CREATE "description").</summary>
    Description: string;
    Subjects: TArray<string>;
    Retention: TNatsStreamRetention;
    Storage: TNatsStreamStorage;
    MaxConsumers, MaxMsgSize, NumReplicas: Integer;
    MaxMsgs, MaxBytes: Int64;
    MaxAge: Int64; // nanoseconds, 0 = unlimited
    Discard: TNatsStreamDiscard;
    DuplicateWindow: Int64; // nanoseconds, default 120e9 (2 minutes, NATS server default)
    /// <summary>Max messages retained per subject (KV history). 0 = omit (server default).</summary>
    MaxMsgsPerSubject: Int64;
    /// <summary>When True, emits allow_direct (required for DIRECT.GET / fast KV reads).</summary>
    AllowDirect: Boolean;
    /// <summary>When True, emits deny_delete (KV buckets refuse raw stream message deletes).</summary>
    DenyDelete: Boolean;
    /// <summary>When True, emits allow_rollup_hdrs (KV purge rollup markers).</summary>
    AllowRollup: Boolean;
    /// <summary>When True, emits allow_msg_ttl (NATS 2.11+; required for Nats-TTL publishes).</summary>
    AllowMsgTTL: Boolean;
    /// <summary>subject_delete_marker_ttl in nanoseconds (min 1s). 0 = omit. Enables limit markers.</summary>
    SubjectDeleteMarkerTTL: Int64;
    /// <summary>When True, emits sealed (stream rejects further publishes / subject changes).</summary>
    Sealed: Boolean;
    /// <summary>Storage compression; <c>scNone</c> omits JSON "compression".</summary>
    Compression: TNatsStoreCompression;
    /// <summary>Optional placement; omitted from JSON when <see cref="TNatsPlacement.IsSet"/> is False.</summary>
    Placement: TNatsPlacement;
    /// <summary>Sensible defaults: limits retention, file storage, unlimited limits, 2 minute dedup window.</summary>
    class function CreateDefault(const AName: string; const ASubjects: TArray<string>): TNatsStreamConfig; static;
    /// <summary>Serializes the record to the JSON body expected by STREAM.CREATE / STREAM.UPDATE.</summary>
    function ToJson: string;
  end;

  /// <summary>Stream metadata and usage counters returned by STREAM.CREATE/STREAM.INFO.</summary>
  TNatsStreamInfo = record
    Name: string;
    Messages, Bytes, FirstSeq, LastSeq: UInt64;
    ConsumerCount: Integer;
    /// <summary>Parsed stream config from the API "config" object (subjects, sealed, limits, …).</summary>
    Config: TNatsStreamConfig;
    /// <summary>Parses a stream_create_response/stream_info_response JSON payload.
    /// Raises EDextNatsJetStreamError if the payload carries an "error" object.</summary>
    class function Parse(const AJson: string): TNatsStreamInfo; static;
  end;

  /// <summary>Configuration used to create a JetStream consumer (pull by default).</summary>
  TNatsConsumerConfig = record
    /// <summary>Durable name; when set, the consumer survives client disconnects.</summary>
    DurableName: string;
    /// <summary>Ephemeral consumer name (used when DurableName is empty).</summary>
    Name: string;
    Description: string;
    /// <summary>Optional subject filter within the stream.</summary>
    FilterSubject: string;
    /// <summary>
    ///   When non-empty, creates a <b>push</b> consumer that delivers to this subject.
    ///   Empty = pull consumer (use <see cref="TDextNatsJetStreamContext.Fetch"/>).
    /// </summary>
    DeliverSubject: string;
    /// <summary>Optional queue group for push consumers (load-balanced delivery).</summary>
    DeliverGroup: string;
    DeliverPolicy: TNatsDeliverPolicy;
    /// <summary>Optional start sequence when DeliverPolicy = dpByStartSequence.</summary>
    OptStartSeq: UInt64;
    AckPolicy: TNatsAckPolicy;
    /// <summary>How long the server waits for an Ack before redelivery, in nanoseconds. 0 = server default.</summary>
    AckWait: Int64;
    MaxDeliver: Integer;
    MaxAckPending: Integer;
    /// <summary>Pull-only: max outstanding Fetch waits. Ignored when DeliverSubject is set.</summary>
    MaxWaiting: Integer;
    ReplayPolicy: TNatsReplayPolicy;
    /// <summary>
    ///   When True, the consumer delivers headers only (empty payload).
    ///   Used by KV <c>MetaOnly</c> watches.
    /// </summary>
    HeadersOnly: Boolean;
    /// <summary>
    ///   When True, enables push flow-control (JSON <c>flow_control</c>).
    ///   Client must reply to status-100 FC requests on <c>ReplyTo</c>.
    /// </summary>
    FlowControl: Boolean;
    /// <summary>Idle heartbeat interval in nanoseconds (JSON <c>idle_heartbeat</c>). 0 = omit.</summary>
    IdleHeartbeat: Int64;
    /// <summary>
    ///   Ephemeral idle lifetime in nanoseconds (JSON <c>inactive_threshold</c>).
    ///   0 = omit. Ordered consumers default to five minutes.
    /// </summary>
    InactiveThreshold: Int64;
    /// <summary>When True, emits <c>mem_storage:true</c> (consumer state in memory).</summary>
    MemoryStorage: Boolean;
    /// <summary>
    ///   Consumer replicas (JSON <c>num_replicas</c>). 0 = omit (server default);
    ///   ordered consumers force 1.
    /// </summary>
    NumReplicas: Integer;
    /// <summary>Defaults for a durable pull consumer: deliver all, ack_policy=explicit.</summary>
    class function CreateDefault(const ADurableName: string = '';
      const AFilterSubject: string = ''): TNatsConsumerConfig; static;
    /// <summary>Serializes the consumer config object (the "config" field of CONSUMER.CREATE).</summary>
    function ToJson: string;
  end;

  /// <summary>Consumer metadata returned by CONSUMER.CREATE / CONSUMER.INFO.</summary>
  TNatsConsumerInfo = record
    StreamName: string;
    Name: string;
    DurableName: string;
    FilterSubject: string;
    DeliverSubject: string;
    DeliverGroup: string;
    NumPending: UInt64;
    NumAckPending: Integer;
    NumRedelivered: Integer;
    NumWaiting: Integer;
    /// <summary>Parses a consumer_create_response / consumer_info_response JSON payload.</summary>
    class function Parse(const AJson: string): TNatsConsumerInfo; static;
  end;

  /// <summary>Per-call options for a dedup'd JetStream publish.</summary>
  TNatsJetStreamPublishOptions = record
    /// <summary>Server-side dedup key, sent as the Nats-Msg-Id header. Empty = no dedup.</summary>
    MsgId: string;
    /// <summary>Sent as Nats-Expected-Stream if non-empty; the server rejects the publish
    /// if the subject does not resolve to this stream.</summary>
    ExpectedStream: string;
    /// <summary>Sent as Nats-Expected-Last-Sequence if &gt; 0; optimistic-concurrency guard.</summary>
    ExpectedLastSequence: UInt64;
    /// <summary>Sent as Nats-Expected-Last-Subject-Sequence if set (including 0 for "create if absent").
    /// Use with <see cref="ExpectedLastSubjectSequenceSet"/>.</summary>
    ExpectedLastSubjectSequence: UInt64;
    /// <summary>When True, emits Nats-Expected-Last-Subject-Sequence (0 is a valid CAS value).</summary>
    ExpectedLastSubjectSequenceSet: Boolean;
    /// <summary>Sent as Nats-Expected-Last-Msg-Id if non-empty; optimistic-concurrency guard.</summary>
    ExpectedLastMsgId: string;
    /// <summary>Extra headers merged into the publish (e.g. KV-Operation / Nats-Rollup).</summary>
    ExtraHeaders: TNatsHeaders;
    /// <summary>
    ///   Per-message TTL as the Nats-TTL header (nanoseconds). 0 = omit.
    ///   Requires stream allow_msg_ttl; minimum 1 second when set (ADR-43).
    /// </summary>
    MsgTTL: Int64;
    /// <summary>Request timeout in milliseconds; 0 = use the client's RequestTimeoutMs.</summary>
    TimeoutMs: Integer;
    /// <summary>Sensible defaults: no dedup key, no expectations, client default timeout.</summary>
    class function CreateDefault: TNatsJetStreamPublishOptions; static;
  end;

  /// <summary>A stored JetStream message returned by STREAM.MSG.GET (payload base64-decoded).</summary>
  TNatsStoredMsg = record
    Subject: string;
    Sequence: UInt64;
    Data: TBytes;
    Headers: TNatsHeaders;
    TimeStamp: string;
    /// <summary>Parses a stream_msg_get_response JSON payload.</summary>
    class function Parse(const AJson: string): TNatsStoredMsg; static;
  end;

  /// <summary>Acknowledgement returned by the server for a JetStream publish.</summary>
  TNatsPublishAck = record
    Stream: string;
    Sequence: UInt64;
    Duplicate: Boolean;
    Domain: string;
    /// <summary>Parses a PubAck JSON payload. Raises EDextNatsJetStreamError on {"error":...}.</summary>
    class function Parse(const AJson: string): TNatsPublishAck; static;
  end;

  /// <summary>A JetStream message returned by <see cref="TDextNatsJetStreamContext.Fetch"/>
  /// or delivered to a push subscription handler.</summary>
  TNatsJsMsg = record
    Subject: string;
    ReplyTo: string;
    Payload: TBytes;
    Headers: TNatsHeaders;
    StatusCode: Integer;
    Stream: string;
    Consumer: string;
    StreamSequence: UInt64;
    ConsumerSequence: UInt64;
    /// <summary>Server timestamp from the Ack subject, in nanoseconds since Unix epoch when present.</summary>
    Timestamp: Int64;
    NumPending: Integer;
    /// <summary>Decodes the payload as a UTF-8 string.</summary>
    function AsString: string;
    /// <summary>Builds a JS message from a raw NATS message, parsing metadata from ReplyTo / headers.</summary>
    class function FromNatsMsg(const AMsg: TNatsMsg): TNatsJsMsg; static;
  end;

  /// <summary>Handler for messages delivered by a JetStream push consumer.</summary>
  TNatsJsMsgHandler = reference to procedure(const AMsg: TNatsJsMsg);

  /// <summary>
  ///   Active SUB on a push consumer deliver subject. Does not own the JetStream context or client.
  ///   Call <see cref="Unsubscribe"/> or Free to stop delivery (sends UNSUB).
  /// </summary>
  TDextNatsJetStreamPushSubscription = class
  private
    FClient: TDextNatsClient;
    FSid: Integer;
    FDeliverSubject: string;
    FActive: Boolean;
  public
    constructor Create(AClient: TDextNatsClient; ASid: Integer; const ADeliverSubject: string);
    destructor Destroy; override;
    /// <summary>Unsubscribes from the deliver subject. Safe to call more than once.</summary>
    procedure Unsubscribe;
    property Sid: Integer read FSid;
    property DeliverSubject: string read FDeliverSubject;
    property Active: Boolean read FActive;
  end;

  TDextNatsJetStreamContext = class;

  /// <summary>Callback for messages from <see cref="TDextNatsJetStreamContext.SubscribeOrdered"/>.</summary>
  TNatsOrderedConsumerHandler = TNatsJsMsgHandler;
  /// <summary>Optional error callback when ordered-consumer reset fails terminally.</summary>
  TNatsOrderedConsumerErrorHandler = reference to procedure(const AErrorMessage: string);

  /// <summary>
  ///   Options for an ADR-17 style ordered push consumer (nats.go classic <c>OrderedConsumer()</c>).
  ///   Ephemeral, ack_policy=none, flow_control + idle heartbeats, mem_storage, recreate on gap/missed HB.
  /// </summary>
  TNatsOrderedConsumerOptions = record
    /// <summary>Optional filter within the stream (empty = whole stream).</summary>
    FilterSubject: string;
    /// <summary>Initial deliver policy (default <c>dpAll</c>). Recreates always use by_start_sequence.</summary>
    DeliverPolicy: TNatsDeliverPolicy;
    /// <summary>When <see cref="DeliverPolicy"/> is <c>dpByStartSequence</c>, first start sequence.</summary>
    OptStartSeq: UInt64;
    /// <summary>When True, headers-only deliveries (no payload).</summary>
    HeadersOnly: Boolean;
    /// <summary>Consumer name prefix (<c>prefix_1</c>, <c>prefix_2</c>, …). Empty = auto nuid.</summary>
    NamePrefix: string;
    /// <summary>Idle heartbeat nanoseconds. 0 = five seconds (nats.go default).</summary>
    IdleHeartbeat: Int64;
    /// <summary>Inactive threshold nanoseconds. 0 = five minutes (server deletes idle ephemeral).</summary>
    InactiveThreshold: Int64;
    /// <summary>
    ///   Max recreate attempts after gap / missed heartbeat. 0 or negative = unlimited
    ///   (nats.go <c>MaxResetAttempts</c> default).
    /// </summary>
    MaxResetAttempts: Integer;
    /// <summary>Optional terminal-error callback (exhausted resets / create failure).</summary>
    OnError: TNatsOrderedConsumerErrorHandler;
    /// <summary>Defaults: deliver all, 5s heartbeat, 5min inactive, unlimited resets.</summary>
    class function CreateDefault(const AFilterSubject: string = ''): TNatsOrderedConsumerOptions; static;
  end;

  /// <summary>
  ///   Client-managed ordered push consumer (ADR-17 / classic nats.go OrderedConsumer).
  ///   Owns the deliver SUB and ephemeral consumer name; does not own the JetStream context.
  ///   Call <see cref="Stop"/> or Free to tear down. Handlers run on the receive thread —
  ///   do not block with Request/Fetch. Gap / missed-heartbeat recovery runs on a helper thread.
  /// </summary>
  TDextNatsOrderedConsumer = class
  private
    FJs: TDextNatsJetStreamContext;
    FStreamName: string;
    FOptions: TNatsOrderedConsumerOptions;
    FHandler: TNatsOrderedConsumerHandler;
    FLock: TCriticalSection;
    FPush: TDextNatsJetStreamPushSubscription;
    FConsumerName: string;
    FDeliverSubject: string;
    FSerial: Integer;
    FExpectedDseq: UInt64;
    FLastStreamSeq: UInt64;
    FLastConsumerSeq: UInt64;
    FActive: Boolean;
    FStopping: Boolean;
    FResetPending: Boolean;
    FResetCount: Integer;
    FLastActivityMs: UInt64;
    FIdleHeartbeatNs: Int64;
    FWake: TEvent;
    FMonitor: TThread;
    procedure TouchActivity;
    procedure RequestReset;
    procedure MonitorLoop;
    procedure TeardownPushAndConsumer;
    function BuildConsumerConfig(ASerial: Integer; const ADeliver: string;
      ARecreate: Boolean; ALastStreamSeq: UInt64): TNatsConsumerConfig;
    procedure InstallDelivery(ASerial: Integer);
    function TryReset(AInitial: Boolean = False): Boolean;
    procedure FailTerminal(const AErrorMessage: string);
    procedure HandleRawMsg(ASerial: Integer; const AMsg: TNatsMsg);
    function GetActive: Boolean;
    function GetConsumerName: string;
    function GetLastStreamSequence: UInt64;
    function GetSerial: Integer;
    function GetResetCount: Integer;
  public
    constructor Create(AJs: TDextNatsJetStreamContext; const AStreamName: string;
      AHandler: TNatsOrderedConsumerHandler; const AOptions: TNatsOrderedConsumerOptions);
    destructor Destroy; override;
    /// <summary>Stops delivery, deletes the ephemeral consumer (best effort), joins the monitor thread.</summary>
    procedure Stop;
    property Active: Boolean read GetActive;
    property StreamName: string read FStreamName;
    /// <summary>Current ephemeral consumer name (<c>prefix_N</c>), empty when inactive.</summary>
    property ConsumerName: string read GetConsumerName;
    /// <summary>Last delivered stream sequence (0 before the first message).</summary>
    property LastStreamSequence: UInt64 read GetLastStreamSequence;
    /// <summary>Current consumer generation (increments on each create/recreate).</summary>
    property Serial: Integer read GetSerial;
    /// <summary>How many gap/HB-driven recreates have completed successfully.</summary>
    property ResetCount: Integer read GetResetCount;
  end;

  /// <summary>
  ///   Thin JetStream wrapper around an already-connected <see cref="TDextNatsClient"/>.
  ///   Stream admin, dedup'd publish, pull/push consumer admin, Fetch, SubscribePush,
  ///   SubscribeOrdered, and Ack helpers.
  ///   Does not own the wrapped client; the caller remains responsible for its lifetime.
  /// </summary>
  TDextNatsJetStreamContext = class
  private
    FClient: TDextNatsClient;
    FApiPrefix: string;
    /// <summary>Issues a plain (no-headers) JetStream API request and returns the raw reply body.</summary>
    function ApiRequest(const ASubjectSuffix, ABody: string; ATimeoutMs: Integer = 0): string;
    procedure PublishAckPayload(const AReplyTo, APayload: string);
  public
    /// <summary>Wraps AClient. AApiPrefix defaults to "$JS.API." (no custom JetStream domain).</summary>
    constructor Create(AClient: TDextNatsClient; const AApiPrefix: string = '$JS.API.');

    /// <summary>Creates a stream from AConfig. Raises EDextNatsJetStreamError on failure
    /// (e.g. the stream already exists with a different configuration).</summary>
    function CreateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
    /// <summary>Updates an existing stream's configuration (STREAM.UPDATE).</summary>
    function UpdateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
    /// <summary>Fetches current metadata for AStreamName. Raises EDextNatsJetStreamError if it does not exist.</summary>
    function GetStreamInfo(const AStreamName: string): TNatsStreamInfo;
    /// <summary>True if AStreamName exists. False only for a "stream not found" API error;
    /// any other failure propagates.</summary>
    function StreamExists(const AStreamName: string): Boolean;
    /// <summary>Deletes AStreamName and all of its messages. Raises EDextNatsJetStreamError on failure.</summary>
    function DeleteStream(const AStreamName: string): Boolean;
    /// <summary>
    ///   Lists all stream names via paged <c>$JS.API.STREAM.NAMES</c>.
    ///   When ASubjectFilter is non-empty, only streams matching that subject are returned.
    /// </summary>
    function ListStreamNames(const ASubjectFilter: string = ''): IList<string>;
    /// <summary>
    ///   Lists full stream info via paged <c>$JS.API.STREAM.LIST</c>.
    ///   When ASubjectFilter is non-empty, only streams matching that subject are returned.
    /// </summary>
    function ListStreams(const ASubjectFilter: string = ''): IList<TNatsStreamInfo>;

    /// <summary>Retrieves the last stored message for ASubject via STREAM.MSG.GET (last_by_subj).</summary>
    function GetLastMessage(const AStreamName, ASubject: string; ATimeoutMs: Integer = 0): TNatsStoredMsg;
    /// <summary>Retrieves a stored message by stream sequence via STREAM.MSG.GET.</summary>
    function GetMessage(const AStreamName: string; ASequence: UInt64; ATimeoutMs: Integer = 0): TNatsStoredMsg;

    /// <summary>Creates a consumer on AStreamName from AConfig (pull when DeliverSubject empty; push otherwise).</summary>
    function CreateConsumer(const AStreamName: string; const AConfig: TNatsConsumerConfig): TNatsConsumerInfo;
    /// <summary>Fetches current metadata for a consumer. Raises EDextNatsJetStreamError if missing.</summary>
    function GetConsumerInfo(const AStreamName, AConsumerName: string): TNatsConsumerInfo;
    /// <summary>Deletes a consumer. Raises EDextNatsJetStreamError on failure.</summary>
    function DeleteConsumer(const AStreamName, AConsumerName: string): Boolean;
    /// <summary>Lists consumer names on AStreamName via paged <c>$JS.API.CONSUMER.NAMES.{stream}</c>.</summary>
    function ListConsumerNames(const AStreamName: string): IList<string>;
    /// <summary>Lists full consumer info on AStreamName via paged <c>$JS.API.CONSUMER.LIST.{stream}</c>.</summary>
    function ListConsumers(const AStreamName: string): IList<TNatsConsumerInfo>;

    /// <summary>
    ///   Pulls up to ABatch messages from a pull consumer. AExpiresMs is how long the server
    ///   may hold the request open (sent as nanoseconds in the NEXT request body).
    ///   Control messages (100/404/408/409) end the wait and are not included in the result.
    /// </summary>
    function Fetch(const AStreamName, AConsumerName: string; ABatch: Integer = 1;
      AExpiresMs: Integer = 5000): IList<TNatsJsMsg>;

    /// <summary>
    ///   Subscribes to an existing push consumer's deliver subject (looked up via CONSUMER.INFO).
    ///   Uses DeliverGroup as the NATS queue group when set. Caller must Free the subscription
    ///   (or call Unsubscribe). Handlers run on the client's receive thread — do not block with Request.
    /// </summary>
    function SubscribePush(const AStreamName, AConsumerName: string;
      AHandler: TNatsJsMsgHandler): TDextNatsJetStreamPushSubscription; overload;
    /// <summary>Subscribes directly to ADeliverSubject (optional queue group for deliver_group).</summary>
    function SubscribePush(const ADeliverSubject: string; AHandler: TNatsJsMsgHandler;
      const AQueueGroup: string = ''): TDextNatsJetStreamPushSubscription; overload;

    /// <summary>
    ///   Creates an ADR-17 ordered push consumer on AStreamName and delivers messages in stream
    ///   order via AHandler. Recreates the ephemeral consumer on consumer-sequence gaps and
    ///   missed idle heartbeats. Caller must Free (or <see cref="TDextNatsOrderedConsumer.Stop"/>).
    /// </summary>
    function SubscribeOrdered(const AStreamName: string; AHandler: TNatsOrderedConsumerHandler;
      const AOptions: TNatsOrderedConsumerOptions): TDextNatsOrderedConsumer; overload;
    /// <summary>Ordered consumer with <see cref="TNatsOrderedConsumerOptions.CreateDefault"/>.</summary>
    function SubscribeOrdered(const AStreamName: string;
      AHandler: TNatsOrderedConsumerHandler): TDextNatsOrderedConsumer; overload;

    /// <summary>Acknowledges a fetched message (+ACK on ReplyTo).</summary>
    procedure Ack(const AMsg: TNatsJsMsg); overload;
    /// <summary>Acknowledges by ReplyTo subject.</summary>
    procedure Ack(const AReplyTo: string); overload;
    /// <summary>Negative-acknowledges a message (+NAK). Optional delay in milliseconds before redelivery.</summary>
    procedure Nak(const AMsg: TNatsJsMsg; ADelayMs: Integer = 0); overload;
    procedure Nak(const AReplyTo: string; ADelayMs: Integer = 0); overload;
    /// <summary>Terminates delivery of a message (+TERM); it will not be redelivered.</summary>
    procedure Term(const AMsg: TNatsJsMsg); overload;
    procedure Term(const AReplyTo: string); overload;
    /// <summary>Signals work-in-progress (+WPI) so the AckWait timer is reset.</summary>
    procedure InProgress(const AMsg: TNatsJsMsg); overload;
    procedure InProgress(const AReplyTo: string); overload;

    /// <summary>Publishes APayload to ASubject with JetStream dedup/expectation headers taken
    /// from AOptions, and returns the parsed acknowledgement.</summary>
    function Publish(const ASubject: string; const APayload: TBytes;
      const AOptions: TNatsJetStreamPublishOptions): TNatsPublishAck; overload;
    /// <summary>Convenience overload: publishes raw bytes with an optional Nats-Msg-Id dedup key.</summary>
    function Publish(const ASubject: string; const APayload: TBytes; const AMsgId: string = ''): TNatsPublishAck; overload;
    /// <summary>Convenience overload: UTF-8 encodes AMessage with an optional Nats-Msg-Id dedup key.</summary>
    function Publish(const ASubject, AMessage: string; const AMsgId: string = ''): TNatsPublishAck; overload;
    /// <summary>Publishes with an explicit header set (merged over default options).</summary>
    function PublishWithHeaders(const ASubject: string; const APayload: TBytes;
      const AHeaders: TNatsHeaders; ATimeoutMs: Integer = 0): TNatsPublishAck;

    /// <summary>The wrapped client. Its lifetime remains the caller's responsibility.</summary>
    property Client: TDextNatsClient read FClient;
    /// <summary>JetStream API subject prefix (default "$JS.API.").</summary>
    property ApiPrefix: string read FApiPrefix;
  end;

implementation

uses
  System.NetEncoding,
  Dext.Core.Span,
  Dext.Json.Utf8;

const
  NATS_JS_NS_PER_MS = Int64(1000000);
  /// <summary>nats.go orderedHeartbeatsInterval (5s) in nanoseconds.</summary>
  NATS_JS_ORDERED_HB_NS = Int64(5) * 1000 * NATS_JS_NS_PER_MS;
  /// <summary>Five-minute inactive_threshold for throwaway ordered ephemerals.</summary>
  NATS_JS_ORDERED_INACTIVE_NS = Int64(5) * 60 * 1000 * NATS_JS_NS_PER_MS;
  /// <summary>nats.go ordered AckWait (22h); unused with ack_policy=none but required by ADR-17.</summary>
  NATS_JS_ORDERED_ACK_WAIT_NS = Int64(22) * 60 * 60 * 1000 * NATS_JS_NS_PER_MS;
  /// <summary>Missed-heartbeat multiplier (nats.go hbcThresh).</summary>
  NATS_JS_ORDERED_HB_THRESH = 2;

type
  /// <summary>Growable byte sink for JetStream admin JSON (mirrors Protocol TNatsByteWriter).</summary>
  PJsByteWriter = ^TJsByteWriter;
  TJsByteWriter = record
  private
    FBuf: TBytes;
    FLen: Integer;
  public
    procedure Reset;
    procedure EnsureCapacity(ANeeded: Integer);
    procedure WriteBytes(AData: Pointer; ALength: Integer);
    function ToBytes: TBytes;
  end;

procedure JsUtf8WriteToByteWriter(AContext, AData: Pointer; ALength: Integer);
begin
  if (ALength > 0) and (AContext <> nil) then
    PJsByteWriter(AContext)^.WriteBytes(AData, ALength);
end;

procedure TJsByteWriter.Reset;
begin
  FLen := 0;
end;

procedure TJsByteWriter.EnsureCapacity(ANeeded: Integer);
var
  cap, newCap: Integer;
begin
  if ANeeded <= 0 then
    Exit;
  cap := Length(FBuf);
  if FLen + ANeeded <= cap then
    Exit;
  newCap := cap;
  if newCap < 256 then
    newCap := 256;
  while FLen + ANeeded > newCap do
    newCap := newCap * 2;
  SetLength(FBuf, newCap);
end;

procedure TJsByteWriter.WriteBytes(AData: Pointer; ALength: Integer);
begin
  if (ALength <= 0) or (AData = nil) then
    Exit;
  EnsureCapacity(ALength);
  Move(AData^, FBuf[FLen], ALength);
  Inc(FLen, ALength);
end;

function TJsByteWriter.ToBytes: TBytes;
begin
  SetLength(Result, FLen);
  if FLen > 0 then
    Move(FBuf[0], Result[0], FLen);
end;

procedure NatsJSSkipValue(var AReader: TUtf8JsonReader);
begin
  if AReader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
    AReader.Skip;
end;

/// <summary>Raises when the reader is positioned on the error object's StartObject.</summary>
procedure NatsJSRaiseFromErrorObject(var AReader: TUtf8JsonReader);
var
  code, errCode: Integer;
  description: string;
begin
  code := 0;
  errCode := 0;
  description := '';
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        code := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('err_code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        errCode := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('description') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        description := AReader.GetString
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.Read then
      NatsJSSkipValue(AReader);
  end;
  raise EDextNatsJetStreamError.CreateFromApi(code, errCode, description);
end;

procedure NatsJSHandlePropValue(var AReader: TUtf8JsonReader);
begin
  if AReader.Read then
    NatsJSSkipValue(AReader);
end;

function NatsJSOpenReader(const AJson, AEmptyMsg: string; out ABytes: TBytes): TUtf8JsonReader;
var
  span: TByteSpan;
begin
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create(AEmptyMsg);
  ABytes := TEncoding.UTF8.GetBytes(AJson);
  if Length(ABytes) = 0 then
    raise EDextNatsProtocolError.Create(AEmptyMsg);
  span := TByteSpan.Create(@ABytes[0], Length(ABytes));
  Result := TUtf8JsonReader.Create(span);
end;

function NatsJSParseSuccessResponse(const AJson: string): Boolean;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
begin
  Result := False;
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream API response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('error') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            NatsJSRaiseFromErrorObject(reader)
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('success') then
      begin
        if reader.Read then
        begin
          if reader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
            Result := reader.GetBoolean
          else
            NatsJSSkipValue(reader);
        end;
      end
      else
        NatsJSHandlePropValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
end;

function NatsJSIsFetchControl(const AMsg: TNatsMsg): Boolean;
begin
  case AMsg.StatusCode of
    100, 404, 408, 409:
      Result := True;
  else
    Result := (Length(AMsg.Payload) = 0) and (AMsg.StatusCode <> 0);
  end;
end;

function NatsJSBuildPagedListRequest(AOffset: Integer; const ASubjectFilter: string): string;
var
  w: TJsByteWriter;
  jw: TUtf8JsonWriter;
begin
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, JsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('offset');
  jw.WriteNumber(AOffset);
  if ASubjectFilter <> '' then
  begin
    jw.WritePropertyName('subject');
    jw.WriteString(ASubjectFilter);
  end;
  jw.WriteEndObject;
  Result := TEncoding.UTF8.GetString(w.ToBytes);
end;

procedure NatsJSParseStreamConfigObject(var AReader: TUtf8JsonReader; out AConfig: TNatsStreamConfig); forward;

/// <summary>Reader is positioned on StartObject of a stream_info / stream_create response body.</summary>
procedure NatsJSParseStreamInfoObject(var AReader: TUtf8JsonReader; out AInfo: TNatsStreamInfo);
begin
  AInfo := Default(TNatsStreamInfo);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('error') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType = TJsonTokenType.StartObject then
          NatsJSRaiseFromErrorObject(AReader)
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('config') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType = TJsonTokenType.StartObject then
        begin
          NatsJSParseStreamConfigObject(AReader, AInfo.Config);
          if AInfo.Name = '' then
            AInfo.Name := AInfo.Config.Name;
        end
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('state') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType = TJsonTokenType.StartObject then
        begin
          while AReader.Read do
          begin
            if AReader.TokenType = TJsonTokenType.EndObject then
              Break;
            if AReader.TokenType <> TJsonTokenType.PropertyName then
              Continue;
            if AReader.ValueSpanEquals('messages') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
                AInfo.Messages := UInt64(AReader.GetInt64)
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('bytes') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
                AInfo.Bytes := UInt64(AReader.GetInt64)
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('first_seq') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
                AInfo.FirstSeq := UInt64(AReader.GetInt64)
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('last_seq') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
                AInfo.LastSeq := UInt64(AReader.GetInt64)
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('consumer_count') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
                AInfo.ConsumerCount := AReader.GetInt32
              else
                NatsJSSkipValue(AReader);
            end
            else
              NatsJSHandlePropValue(AReader);
          end;
        end
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else
      NatsJSHandlePropValue(AReader);
  end;
end;

/// <summary>Reader is positioned on StartObject of a consumer_info / consumer_create response body.</summary>
procedure NatsJSParseConsumerInfoObject(var AReader: TUtf8JsonReader; out AInfo: TNatsConsumerInfo);
begin
  { Default — not FillChar — so managed string fields are finalized across ListConsumers pages. }
  AInfo := Default(TNatsConsumerInfo);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('error') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType = TJsonTokenType.StartObject then
          NatsJSRaiseFromErrorObject(AReader)
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('stream_name') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AInfo.StreamName := AReader.GetString
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('name') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AInfo.Name := AReader.GetString
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('num_pending') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AInfo.NumPending := UInt64(AReader.GetInt64)
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('num_ack_pending') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AInfo.NumAckPending := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('num_redelivered') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AInfo.NumRedelivered := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('num_waiting') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AInfo.NumWaiting := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('config') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType = TJsonTokenType.StartObject then
        begin
          while AReader.Read do
          begin
            if AReader.TokenType = TJsonTokenType.EndObject then
              Break;
            if AReader.TokenType <> TJsonTokenType.PropertyName then
              Continue;
            if AReader.ValueSpanEquals('durable_name') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
                AInfo.DurableName := AReader.GetString
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('filter_subject') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
                AInfo.FilterSubject := AReader.GetString
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('deliver_subject') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
                AInfo.DeliverSubject := AReader.GetString
              else
                NatsJSSkipValue(AReader);
            end
            else if AReader.ValueSpanEquals('deliver_group') then
            begin
              if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
                AInfo.DeliverGroup := AReader.GetString
              else
                NatsJSSkipValue(AReader);
            end
            else
              NatsJSHandlePropValue(AReader);
          end;
        end
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else
      NatsJSHandlePropValue(AReader);
  end;
  if AInfo.Name = '' then
    AInfo.Name := AInfo.DurableName;
end;

/// <summary>
///   Parses one paged JetStream names response (STREAM.NAMES / CONSUMER.NAMES).
///   Appends string items from AArrayProp ("streams" or "consumers"). Returns count added.
/// </summary>
function NatsJSAppendPagedNames(const AJson, AArrayProp: string; out ATotal, AOffset, ALimit: Integer;
  const ANames: IList<string>): Integer;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
  pageCount: Integer;
begin
  ATotal := 0;
  AOffset := 0;
  ALimit := 0;
  pageCount := 0;
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream API response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('error') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            NatsJSRaiseFromErrorObject(reader)
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('total') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          ATotal := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('offset') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          AOffset := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('limit') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          ALimit := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals(AArrayProp) then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartArray then
          begin
            while reader.Read do
            begin
              if reader.TokenType = TJsonTokenType.EndArray then
                Break;
              if reader.TokenType = TJsonTokenType.StringValue then
              begin
                ANames.Add(reader.GetString);
                Inc(pageCount);
              end
              else
                NatsJSSkipValue(reader);
            end;
          end
          else
            NatsJSSkipValue(reader);
        end;
      end
      else
        NatsJSHandlePropValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
  Result := pageCount;
end;

/// <summary>Parses one STREAM.LIST page; appends TNatsStreamInfo items. Returns count added.</summary>
function NatsJSAppendPagedStreamInfos(const AJson: string; out ATotal, AOffset, ALimit: Integer;
  const AInfos: IList<TNatsStreamInfo>): Integer;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
  pageCount: Integer;
  info: TNatsStreamInfo;
begin
  ATotal := 0;
  AOffset := 0;
  ALimit := 0;
  pageCount := 0;
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream API response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('error') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            NatsJSRaiseFromErrorObject(reader)
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('total') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          ATotal := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('offset') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          AOffset := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('limit') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          ALimit := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('streams') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartArray then
          begin
            while reader.Read do
            begin
              if reader.TokenType = TJsonTokenType.EndArray then
                Break;
              if reader.TokenType = TJsonTokenType.StartObject then
              begin
                NatsJSParseStreamInfoObject(reader, info);
                AInfos.Add(info);
                Inc(pageCount);
              end
              else
                NatsJSSkipValue(reader);
            end;
          end
          else
            NatsJSSkipValue(reader);
        end;
      end
      else
        NatsJSHandlePropValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
  Result := pageCount;
end;

/// <summary>Parses one CONSUMER.LIST page; appends TNatsConsumerInfo items. Returns count added.</summary>
function NatsJSAppendPagedConsumerInfos(const AJson: string; out ATotal, AOffset, ALimit: Integer;
  const AInfos: IList<TNatsConsumerInfo>): Integer;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
  pageCount: Integer;
  info: TNatsConsumerInfo;
begin
  ATotal := 0;
  AOffset := 0;
  ALimit := 0;
  pageCount := 0;
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream API response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('error') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            NatsJSRaiseFromErrorObject(reader)
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('total') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          ATotal := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('offset') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          AOffset := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('limit') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          ALimit := reader.GetInt32
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('consumers') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartArray then
          begin
            while reader.Read do
            begin
              if reader.TokenType = TJsonTokenType.EndArray then
                Break;
              if reader.TokenType = TJsonTokenType.StartObject then
              begin
                NatsJSParseConsumerInfoObject(reader, info);
                AInfos.Add(info);
                Inc(pageCount);
              end
              else
                NatsJSSkipValue(reader);
            end;
          end
          else
            NatsJSSkipValue(reader);
        end;
      end
      else
        NatsJSHandlePropValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
  Result := pageCount;
end;

{ EDextNatsJetStreamError }

constructor EDextNatsJetStreamError.CreateFromApi(ACode, AErrCode: Integer; const ADescription: string);
begin
  Code := ACode;
  ErrCode := AErrCode;
  inherited CreateFmt('NATS JetStream API error %d (code %d): %s', [AErrCode, ACode, ADescription]);
end;

{ TNatsPlacement }

function TNatsPlacement.IsSet: Boolean;
begin
  Result := (Cluster <> '') or (Length(Tags) > 0);
end;

{ TNatsStreamConfig }

class function TNatsStreamConfig.CreateDefault(const AName: string; const ASubjects: TArray<string>): TNatsStreamConfig;
begin
  FillChar(Result, SizeOf(Result), 0);
  Result.Name := AName;
  Result.Subjects := ASubjects;
  Result.Retention := srLimits;
  Result.Storage := ssFile;
  Result.MaxConsumers := -1;
  Result.MaxMsgs := -1;
  Result.MaxBytes := -1;
  Result.MaxAge := 0;
  Result.MaxMsgSize := -1;
  Result.Discard := sdOld;
  Result.NumReplicas := 1;
  Result.DuplicateWindow := 120000000000;
  Result.Compression := scNone;
end;

function TNatsStreamConfig.ToJson: string;
var
  w: TJsByteWriter;
  jw: TUtf8JsonWriter;
  i: Integer;
  retentionStr, storageStr, discardStr: string;
begin
  case Retention of
    srLimits: retentionStr := 'limits';
    srInterest: retentionStr := 'interest';
    srWorkQueue: retentionStr := 'workqueue';
  else
    retentionStr := 'limits';
  end;

  case Storage of
    ssFile: storageStr := 'file';
    ssMemory: storageStr := 'memory';
  else
    storageStr := 'file';
  end;

  case Discard of
    sdOld: discardStr := 'old';
    sdNew: discardStr := 'new';
  else
    discardStr := 'old';
  end;

  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, JsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('name');
  jw.WriteString(Name);
  if Description <> '' then
  begin
    jw.WritePropertyName('description');
    jw.WriteString(Description);
  end;
  jw.WritePropertyName('subjects');
  jw.WriteStartArray;
  for i := 0 to High(Subjects) do
    jw.WriteString(Subjects[i]);
  jw.WriteEndArray;
  jw.WritePropertyName('retention');
  jw.WriteString(retentionStr);
  jw.WritePropertyName('storage');
  jw.WriteString(storageStr);
  jw.WritePropertyName('max_consumers');
  jw.WriteNumber(MaxConsumers);
  jw.WritePropertyName('max_msgs');
  jw.WriteNumber(MaxMsgs);
  jw.WritePropertyName('max_bytes');
  jw.WriteNumber(MaxBytes);
  jw.WritePropertyName('max_age');
  jw.WriteNumber(MaxAge);
  jw.WritePropertyName('max_msg_size');
  jw.WriteNumber(MaxMsgSize);
  jw.WritePropertyName('discard');
  jw.WriteString(discardStr);
  jw.WritePropertyName('num_replicas');
  jw.WriteNumber(NumReplicas);
  jw.WritePropertyName('duplicate_window');
  jw.WriteNumber(DuplicateWindow);
  if MaxMsgsPerSubject > 0 then
  begin
    jw.WritePropertyName('max_msgs_per_subject');
    jw.WriteNumber(MaxMsgsPerSubject);
  end;
  if AllowDirect then
  begin
    jw.WritePropertyName('allow_direct');
    jw.WriteBoolean(True);
  end;
  if DenyDelete then
  begin
    jw.WritePropertyName('deny_delete');
    jw.WriteBoolean(True);
  end;
  if AllowRollup then
  begin
    jw.WritePropertyName('allow_rollup_hdrs');
    jw.WriteBoolean(True);
  end;
  if AllowMsgTTL then
  begin
    jw.WritePropertyName('allow_msg_ttl');
    jw.WriteBoolean(True);
  end;
  if SubjectDeleteMarkerTTL > 0 then
  begin
    jw.WritePropertyName('subject_delete_marker_ttl');
    jw.WriteNumber(SubjectDeleteMarkerTTL);
  end;
  if Sealed then
  begin
    jw.WritePropertyName('sealed');
    jw.WriteBoolean(True);
  end;
  if Compression = scS2 then
  begin
    jw.WritePropertyName('compression');
    jw.WriteString('s2');
  end;
  if Placement.IsSet then
  begin
    jw.WritePropertyName('placement');
    jw.WriteStartObject;
    jw.WritePropertyName('cluster');
    jw.WriteString(Placement.Cluster);
    if Length(Placement.Tags) > 0 then
    begin
      jw.WritePropertyName('tags');
      jw.WriteStartArray;
      for i := 0 to High(Placement.Tags) do
        jw.WriteString(Placement.Tags[i]);
      jw.WriteEndArray;
    end;
    jw.WriteEndObject;
  end;
  jw.WriteEndObject;
  Result := TEncoding.UTF8.GetString(w.ToBytes);
end;

procedure NatsJSParsePlacementObject(var AReader: TUtf8JsonReader; out APlacement: TNatsPlacement);
var
  tags: TArray<string>;
begin
  APlacement := Default(TNatsPlacement);
  SetLength(tags, 0);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    if AReader.ValueSpanEquals('cluster') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        APlacement.Cluster := AReader.GetString
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('tags') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StartArray) then
      begin
        SetLength(tags, 0);
        while AReader.Read do
        begin
          if AReader.TokenType = TJsonTokenType.EndArray then
            Break;
          if AReader.TokenType = TJsonTokenType.StringValue then
          begin
            SetLength(tags, Length(tags) + 1);
            tags[High(tags)] := AReader.GetString;
          end
          else
            NatsJSSkipValue(AReader);
        end;
        APlacement.Tags := tags;
      end
      else
        NatsJSSkipValue(AReader);
    end
    else
      NatsJSHandlePropValue(AReader);
  end;
end;

procedure NatsJSParseStreamConfigObject(var AReader: TUtf8JsonReader; out AConfig: TNatsStreamConfig);
var
  subjects: TArray<string>;
  s: string;
begin
  AConfig := Default(TNatsStreamConfig);
  AConfig.MaxConsumers := -1;
  AConfig.MaxMsgs := -1;
  AConfig.MaxBytes := -1;
  AConfig.MaxMsgSize := -1;
  AConfig.NumReplicas := 1;
  AConfig.Retention := srLimits;
  AConfig.Storage := ssFile;
  AConfig.Discard := sdOld;
  AConfig.Compression := scNone;
  SetLength(subjects, 0);

  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('name') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AConfig.Name := AReader.GetString
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('description') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AConfig.Description := AReader.GetString
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('subjects') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StartArray) then
      begin
        SetLength(subjects, 0);
        while AReader.Read do
        begin
          if AReader.TokenType = TJsonTokenType.EndArray then
            Break;
          if AReader.TokenType = TJsonTokenType.StringValue then
          begin
            SetLength(subjects, Length(subjects) + 1);
            subjects[High(subjects)] := AReader.GetString;
          end
          else
            NatsJSSkipValue(AReader);
        end;
        AConfig.Subjects := subjects;
      end
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('retention') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        s := AReader.GetString;
        if SameText(s, 'interest') then
          AConfig.Retention := srInterest
        else if SameText(s, 'workqueue') then
          AConfig.Retention := srWorkQueue
        else
          AConfig.Retention := srLimits;
      end
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('storage') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        if SameText(AReader.GetString, 'memory') then
          AConfig.Storage := ssMemory
        else
          AConfig.Storage := ssFile;
      end
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('discard') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        if SameText(AReader.GetString, 'new') then
          AConfig.Discard := sdNew
        else
          AConfig.Discard := sdOld;
      end
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_consumers') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.MaxConsumers := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_msgs') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.MaxMsgs := AReader.GetInt64
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_bytes') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.MaxBytes := AReader.GetInt64
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_age') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.MaxAge := AReader.GetInt64
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_msg_size') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.MaxMsgSize := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('num_replicas') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.NumReplicas := AReader.GetInt32
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('duplicate_window') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.DuplicateWindow := AReader.GetInt64
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_msgs_per_subject') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.MaxMsgsPerSubject := AReader.GetInt64
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('allow_direct') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
          AConfig.AllowDirect := AReader.GetBoolean
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('deny_delete') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
          AConfig.DenyDelete := AReader.GetBoolean
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('allow_rollup_hdrs') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
          AConfig.AllowRollup := AReader.GetBoolean
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('allow_msg_ttl') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
          AConfig.AllowMsgTTL := AReader.GetBoolean
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('subject_delete_marker_ttl') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        AConfig.SubjectDeleteMarkerTTL := AReader.GetInt64
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('sealed') then
    begin
      if AReader.Read then
      begin
        if AReader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
          AConfig.Sealed := AReader.GetBoolean
        else
          NatsJSSkipValue(AReader);
      end;
    end
    else if AReader.ValueSpanEquals('compression') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        if SameText(AReader.GetString, 's2') then
          AConfig.Compression := scS2
        else
          AConfig.Compression := scNone;
      end
      else
        NatsJSSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('placement') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StartObject) then
        NatsJSParsePlacementObject(AReader, AConfig.Placement)
      else
        NatsJSSkipValue(AReader);
    end
    else
      NatsJSHandlePropValue(AReader);
  end;
end;

{ TNatsStreamInfo }

class function TNatsStreamInfo.Parse(const AJson: string): TNatsStreamInfo;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
begin
  Result := Default(TNatsStreamInfo);
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream API response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
    NatsJSParseStreamInfoObject(reader, Result);
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
end;

{ TNatsConsumerConfig }

class function TNatsConsumerConfig.CreateDefault(const ADurableName, AFilterSubject: string): TNatsConsumerConfig;
begin
  FillChar(Result, SizeOf(Result), 0);
  Result.DurableName := ADurableName;
  Result.Name := '';
  Result.Description := '';
  Result.FilterSubject := AFilterSubject;
  Result.DeliverSubject := '';
  Result.DeliverGroup := '';
  Result.DeliverPolicy := dpAll;
  Result.OptStartSeq := 0;
  Result.AckPolicy := apExplicit;
  Result.AckWait := 0;
  Result.MaxDeliver := -1;
  Result.MaxAckPending := 1000;
  Result.MaxWaiting := 512;
  Result.ReplayPolicy := rpInstant;
end;

function TNatsConsumerConfig.ToJson: string;
var
  w: TJsByteWriter;
  jw: TUtf8JsonWriter;
  deliverStr, ackStr, replayStr: string;
begin
  case DeliverPolicy of
    dpAll: deliverStr := 'all';
    dpLast: deliverStr := 'last';
    dpNew: deliverStr := 'new';
    dpByStartSequence: deliverStr := 'by_start_sequence';
    dpByStartTime: deliverStr := 'by_start_time';
    dpLastPerSubject: deliverStr := 'last_per_subject';
  else
    deliverStr := 'all';
  end;

  case AckPolicy of
    apNone: ackStr := 'none';
    apAll: ackStr := 'all';
    apExplicit: ackStr := 'explicit';
  else
    ackStr := 'explicit';
  end;

  case ReplayPolicy of
    rpInstant: replayStr := 'instant';
    rpOriginal: replayStr := 'original';
  else
    replayStr := 'instant';
  end;

  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, JsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  if DurableName <> '' then
  begin
    jw.WritePropertyName('durable_name');
    jw.WriteString(DurableName);
  end;
  if Name <> '' then
  begin
    jw.WritePropertyName('name');
    jw.WriteString(Name);
  end;
  if Description <> '' then
  begin
    jw.WritePropertyName('description');
    jw.WriteString(Description);
  end;
  if FilterSubject <> '' then
  begin
    jw.WritePropertyName('filter_subject');
    jw.WriteString(FilterSubject);
  end;
  if DeliverSubject <> '' then
  begin
    jw.WritePropertyName('deliver_subject');
    jw.WriteString(DeliverSubject);
  end;
  if DeliverGroup <> '' then
  begin
    jw.WritePropertyName('deliver_group');
    jw.WriteString(DeliverGroup);
  end;
  jw.WritePropertyName('deliver_policy');
  jw.WriteString(deliverStr);
  if (DeliverPolicy = dpByStartSequence) and (OptStartSeq > 0) then
  begin
    jw.WritePropertyName('opt_start_seq');
    jw.WriteNumber(Int64(OptStartSeq));
  end;
  jw.WritePropertyName('ack_policy');
  jw.WriteString(ackStr);
  if AckWait > 0 then
  begin
    jw.WritePropertyName('ack_wait');
    jw.WriteNumber(AckWait);
  end;
  jw.WritePropertyName('max_deliver');
  jw.WriteNumber(MaxDeliver);
  jw.WritePropertyName('max_ack_pending');
  jw.WriteNumber(MaxAckPending);
  // max_waiting applies to pull consumers only
  if DeliverSubject = '' then
  begin
    jw.WritePropertyName('max_waiting');
    jw.WriteNumber(MaxWaiting);
  end;
  jw.WritePropertyName('replay_policy');
  jw.WriteString(replayStr);
  if HeadersOnly then
  begin
    jw.WritePropertyName('headers_only');
    jw.WriteBoolean(True);
  end;
  if FlowControl then
  begin
    jw.WritePropertyName('flow_control');
    jw.WriteBoolean(True);
  end;
  if IdleHeartbeat > 0 then
  begin
    jw.WritePropertyName('idle_heartbeat');
    jw.WriteNumber(IdleHeartbeat);
  end;
  if InactiveThreshold > 0 then
  begin
    jw.WritePropertyName('inactive_threshold');
    jw.WriteNumber(InactiveThreshold);
  end;
  if MemoryStorage then
  begin
    jw.WritePropertyName('mem_storage');
    jw.WriteBoolean(True);
  end;
  if NumReplicas > 0 then
  begin
    jw.WritePropertyName('num_replicas');
    jw.WriteNumber(NumReplicas);
  end;
  jw.WriteEndObject;
  Result := TEncoding.UTF8.GetString(w.ToBytes);
end;

{ TNatsConsumerInfo }

class function TNatsConsumerInfo.Parse(const AJson: string): TNatsConsumerInfo;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
begin
  Result := Default(TNatsConsumerInfo);
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream API response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
    NatsJSParseConsumerInfoObject(reader, Result);
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
end;

{ TNatsJetStreamPublishOptions }

class function TNatsJetStreamPublishOptions.CreateDefault: TNatsJetStreamPublishOptions;
begin
  Result := Default(TNatsJetStreamPublishOptions);
end;

{ TNatsStoredMsg }

class function TNatsStoredMsg.Parse(const AJson: string): TNatsStoredMsg;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
  dataB64, hdrsB64, hdrBlock: string;
  statusCode: Integer;
begin
  Result := Default(TNatsStoredMsg);
  dataB64 := '';
  hdrsB64 := '';
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream STREAM.MSG.GET response', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed STREAM.MSG.GET response: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('error') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            NatsJSRaiseFromErrorObject(reader)
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('message') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
          begin
            while reader.Read do
            begin
              if reader.TokenType = TJsonTokenType.EndObject then
                Break;
              if reader.TokenType <> TJsonTokenType.PropertyName then
                Continue;
              if reader.ValueSpanEquals('subject') then
              begin
                if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
                  Result.Subject := reader.GetString
                else
                  NatsJSSkipValue(reader);
              end
              else if reader.ValueSpanEquals('seq') then
              begin
                if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
                  Result.Sequence := UInt64(reader.GetInt64)
                else
                  NatsJSSkipValue(reader);
              end
              else if reader.ValueSpanEquals('data') then
              begin
                if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
                  dataB64 := reader.GetString
                else
                  NatsJSSkipValue(reader);
              end
              else if reader.ValueSpanEquals('hdrs') then
              begin
                if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
                  hdrsB64 := reader.GetString
                else
                  NatsJSSkipValue(reader);
              end
              else if reader.ValueSpanEquals('time') then
              begin
                if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
                  Result.TimeStamp := reader.GetString
                else
                  NatsJSSkipValue(reader);
              end
              else
                NatsJSHandlePropValue(reader);
            end;
          end
          else
            NatsJSSkipValue(reader);
        end;
      end
      else
        NatsJSHandlePropValue(reader);
    end;

    if dataB64 <> '' then
      Result.Data := TNetEncoding.Base64.DecodeStringToBytes(dataB64);
    if hdrsB64 <> '' then
    begin
      hdrBlock := TEncoding.UTF8.GetString(TNetEncoding.Base64.DecodeStringToBytes(hdrsB64));
      NatsParseHeaderBlock(hdrBlock, Result.Headers, statusCode);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed STREAM.MSG.GET response: %s', [AJson]);
  end;
end;

{ TNatsPublishAck }

class function TNatsPublishAck.Parse(const AJson: string): TNatsPublishAck;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
begin
  FillChar(Result, SizeOf(Result), 0);
  try
    reader := NatsJSOpenReader(AJson, 'Empty JetStream publish acknowledgement payload', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt(
        'Malformed JetStream publish acknowledgement payload: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('error') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            NatsJSRaiseFromErrorObject(reader)
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('stream') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Stream := reader.GetString
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('seq') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          Result.Sequence := UInt64(reader.GetInt64)
        else
          NatsJSSkipValue(reader);
      end
      else if reader.ValueSpanEquals('duplicate') then
      begin
        if reader.Read then
        begin
          if reader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
            Result.Duplicate := reader.GetBoolean
          else
            NatsJSSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('domain') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Domain := reader.GetString
        else
          NatsJSSkipValue(reader);
      end
      else
        NatsJSHandlePropValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt(
        'Malformed JetStream publish acknowledgement payload: %s', [AJson]);
  end;
end;

{ TNatsJsMsg }

function TNatsJsMsg.AsString: string;
begin
  if Length(Payload) = 0 then
    Result := ''
  else
    Result := TEncoding.UTF8.GetString(Payload);
end;

class function TNatsJsMsg.FromNatsMsg(const AMsg: TNatsMsg): TNatsJsMsg;
var
  parts: TArray<string>;
  hdr: string;
begin
  FillChar(Result, SizeOf(Result), 0);
  Result.Subject := AMsg.Subject;
  Result.ReplyTo := AMsg.ReplyTo;
  Result.Payload := AMsg.Payload;
  Result.Headers := AMsg.Headers;
  Result.StatusCode := AMsg.StatusCode;

  { $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>.<pending> }
  if Result.ReplyTo.StartsWith('$JS.ACK.') then
  begin
    parts := Result.ReplyTo.Split(['.']);
    if Length(parts) >= 9 then
    begin
      Result.Stream := parts[2];
      Result.Consumer := parts[3];
      Result.StreamSequence := UInt64(StrToInt64Def(parts[5], 0));
      Result.ConsumerSequence := UInt64(StrToInt64Def(parts[6], 0));
      Result.Timestamp := StrToInt64Def(parts[7], 0);
      Result.NumPending := StrToIntDef(parts[8], 0);
    end;
  end;

  if Result.Stream = '' then
  begin
    hdr := Result.Headers.GetValue('Nats-Stream');
    if hdr <> '' then
      Result.Stream := hdr;
  end;
  if Result.Consumer = '' then
  begin
    hdr := Result.Headers.GetValue('Nats-Consumer');
    if hdr <> '' then
      Result.Consumer := hdr;
  end;
  if Result.StreamSequence = 0 then
  begin
    hdr := Result.Headers.GetValue('Nats-Sequence');
    if hdr <> '' then
      Result.StreamSequence := UInt64(StrToInt64Def(hdr, 0));
  end;
end;

{ TDextNatsJetStreamContext }

constructor TDextNatsJetStreamContext.Create(AClient: TDextNatsClient; const AApiPrefix: string);
begin
  inherited Create;
  FClient := AClient;
  FApiPrefix := AApiPrefix;
end;

function TDextNatsJetStreamContext.ApiRequest(const ASubjectSuffix, ABody: string; ATimeoutMs: Integer): string;
begin
  Result := FClient.Request(FApiPrefix + ASubjectSuffix, TEncoding.UTF8.GetBytes(ABody), ATimeoutMs).AsString;
end;

procedure TDextNatsJetStreamContext.PublishAckPayload(const AReplyTo, APayload: string);
begin
  if AReplyTo = '' then
    raise EDextNatsException.Create('JetStream Ack requires a non-empty ReplyTo subject');
  if not FClient.Connected then
    raise EDextNatsException.Create('Cannot Ack: NATS client is not connected');
  FClient.Publish(AReplyTo, APayload);
end;

function TDextNatsJetStreamContext.CreateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
begin
  Result := TNatsStreamInfo.Parse(ApiRequest('STREAM.CREATE.' + AConfig.Name, AConfig.ToJson));
end;

function TDextNatsJetStreamContext.UpdateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
begin
  Result := TNatsStreamInfo.Parse(ApiRequest('STREAM.UPDATE.' + AConfig.Name, AConfig.ToJson));
end;

function TDextNatsJetStreamContext.GetStreamInfo(const AStreamName: string): TNatsStreamInfo;
begin
  Result := TNatsStreamInfo.Parse(ApiRequest('STREAM.INFO.' + AStreamName, '{}'));
end;

function TDextNatsJetStreamContext.StreamExists(const AStreamName: string): Boolean;
begin
  try
    GetStreamInfo(AStreamName);
    Result := True;
  except
    on E: EDextNatsJetStreamError do
    begin
      if (E.ErrCode = 10059) or (E.Code = 404) then
        Result := False
      else
        raise;
    end;
  end;
end;

function TDextNatsJetStreamContext.DeleteStream(const AStreamName: string): Boolean;
begin
  Result := NatsJSParseSuccessResponse(ApiRequest('STREAM.DELETE.' + AStreamName, '{}'));
end;

function TDextNatsJetStreamContext.ListStreamNames(const ASubjectFilter: string): IList<string>;
var
  offset, total, pageOffset, limit, pageCount: Integer;
begin
  Result := TCollections.CreateList<string>;
  offset := 0;
  repeat
    pageCount := NatsJSAppendPagedNames(
      ApiRequest('STREAM.NAMES', NatsJSBuildPagedListRequest(offset, ASubjectFilter)),
      'streams', total, pageOffset, limit, Result);
    if pageCount <= 0 then
      Break;
    Inc(offset, pageCount);
  until offset >= total;
end;

function TDextNatsJetStreamContext.ListStreams(const ASubjectFilter: string): IList<TNatsStreamInfo>;
var
  offset, total, pageOffset, limit, pageCount: Integer;
begin
  Result := TCollections.CreateList<TNatsStreamInfo>;
  offset := 0;
  repeat
    pageCount := NatsJSAppendPagedStreamInfos(
      ApiRequest('STREAM.LIST', NatsJSBuildPagedListRequest(offset, ASubjectFilter)),
      total, pageOffset, limit, Result);
    if pageCount <= 0 then
      Break;
    Inc(offset, pageCount);
  until offset >= total;
end;

function TDextNatsJetStreamContext.GetLastMessage(const AStreamName, ASubject: string;
  ATimeoutMs: Integer): TNatsStoredMsg;
var
  w: TJsByteWriter;
  jw: TUtf8JsonWriter;
  body: string;
begin
  if (AStreamName = '') or (ASubject = '') then
    raise EDextNatsException.Create('GetLastMessage requires stream name and subject');
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, JsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('last_by_subj');
  jw.WriteString(ASubject);
  jw.WriteEndObject;
  body := TEncoding.UTF8.GetString(w.ToBytes);
  Result := TNatsStoredMsg.Parse(ApiRequest('STREAM.MSG.GET.' + AStreamName, body, ATimeoutMs));
end;

function TDextNatsJetStreamContext.GetMessage(const AStreamName: string; ASequence: UInt64;
  ATimeoutMs: Integer): TNatsStoredMsg;
var
  w: TJsByteWriter;
  jw: TUtf8JsonWriter;
  body: string;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('GetMessage requires a stream name');
  if ASequence = 0 then
    raise EDextNatsException.Create('GetMessage requires a non-zero sequence');
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, JsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('seq');
  jw.WriteNumber(Int64(ASequence));
  jw.WriteEndObject;
  body := TEncoding.UTF8.GetString(w.ToBytes);
  Result := TNatsStoredMsg.Parse(ApiRequest('STREAM.MSG.GET.' + AStreamName, body, ATimeoutMs));
end;

function TDextNatsJetStreamContext.CreateConsumer(const AStreamName: string;
  const AConfig: TNatsConsumerConfig): TNatsConsumerInfo;
var
  consumerPart, subject, body: string;
  w: TJsByteWriter;
  jw: TUtf8JsonWriter;
  configBytes: TBytes;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('CreateConsumer requires a stream name');

  consumerPart := AConfig.DurableName;
  if consumerPart = '' then
    consumerPart := AConfig.Name;

  if consumerPart <> '' then
    subject := Format('CONSUMER.CREATE.%s.%s', [AStreamName, consumerPart])
  else
    subject := Format('CONSUMER.CREATE.%s', [AStreamName]);

  { Config ToJson is already a JSON object; splice as raw value after "config": }
  configBytes := TEncoding.UTF8.GetBytes(AConfig.ToJson);
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, JsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('stream_name');
  jw.WriteString(AStreamName);
  jw.WritePropertyName('config');
  if Length(configBytes) > 0 then
    JsUtf8WriteToByteWriter(@w, @configBytes[0], Length(configBytes));
  jw.WriteEndObject;
  body := TEncoding.UTF8.GetString(w.ToBytes);
  Result := TNatsConsumerInfo.Parse(ApiRequest(subject, body));
end;

function TDextNatsJetStreamContext.GetConsumerInfo(const AStreamName, AConsumerName: string): TNatsConsumerInfo;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create('GetConsumerInfo requires stream and consumer names');
  Result := TNatsConsumerInfo.Parse(
    ApiRequest(Format('CONSUMER.INFO.%s.%s', [AStreamName, AConsumerName]), '{}'));
end;

function TDextNatsJetStreamContext.DeleteConsumer(const AStreamName, AConsumerName: string): Boolean;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create('DeleteConsumer requires stream and consumer names');

  Result := NatsJSParseSuccessResponse(
    ApiRequest(Format('CONSUMER.DELETE.%s.%s', [AStreamName, AConsumerName]), '{}'));
end;

function TDextNatsJetStreamContext.ListConsumerNames(const AStreamName: string): IList<string>;
var
  offset, total, pageOffset, limit, pageCount: Integer;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('ListConsumerNames requires a stream name');
  Result := TCollections.CreateList<string>;
  offset := 0;
  repeat
    pageCount := NatsJSAppendPagedNames(
      ApiRequest(Format('CONSUMER.NAMES.%s', [AStreamName]),
        NatsJSBuildPagedListRequest(offset, '')),
      'consumers', total, pageOffset, limit, Result);
    if pageCount <= 0 then
      Break;
    Inc(offset, pageCount);
  until offset >= total;
end;

function TDextNatsJetStreamContext.ListConsumers(const AStreamName: string): IList<TNatsConsumerInfo>;
var
  offset, total, pageOffset, limit, pageCount: Integer;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('ListConsumers requires a stream name');
  Result := TCollections.CreateList<TNatsConsumerInfo>;
  offset := 0;
  repeat
    pageCount := NatsJSAppendPagedConsumerInfos(
      ApiRequest(Format('CONSUMER.LIST.%s', [AStreamName]),
        NatsJSBuildPagedListRequest(offset, '')),
      total, pageOffset, limit, Result);
    if pageCount <= 0 then
      Break;
    Inc(offset, pageCount);
  until offset >= total;
end;

{ TDextNatsJetStreamPushSubscription }

constructor TDextNatsJetStreamPushSubscription.Create(AClient: TDextNatsClient; ASid: Integer;
  const ADeliverSubject: string);
begin
  inherited Create;
  FClient := AClient;
  FSid := ASid;
  FDeliverSubject := ADeliverSubject;
  FActive := True;
end;

destructor TDextNatsJetStreamPushSubscription.Destroy;
begin
  Unsubscribe;
  inherited;
end;

procedure TDextNatsJetStreamPushSubscription.Unsubscribe;
begin
  if not FActive then
    Exit;
  FActive := False;
  if Assigned(FClient) and (FSid > 0) then
  try
    FClient.Unsubscribe(FSid);
  except
  end;
end;

function JsOrderedNewNuid: string;
var
  guid: TGUID;
  s: string;
begin
  CreateGUID(guid);
  s := GUIDToString(guid);
  s := StringReplace(s, '{', '', [rfReplaceAll]);
  s := StringReplace(s, '}', '', [rfReplaceAll]);
  Result := LowerCase(StringReplace(s, '-', '', [rfReplaceAll]));
end;

{ TNatsOrderedConsumerOptions }

class function TNatsOrderedConsumerOptions.CreateDefault(
  const AFilterSubject: string): TNatsOrderedConsumerOptions;
begin
  Result := Default(TNatsOrderedConsumerOptions);
  Result.FilterSubject := AFilterSubject;
  Result.DeliverPolicy := dpAll;
  Result.OptStartSeq := 0;
  Result.HeadersOnly := False;
  Result.NamePrefix := '';
  Result.IdleHeartbeat := 0;
  Result.InactiveThreshold := 0;
  Result.MaxResetAttempts := 0;
  Result.OnError := nil;
end;

{ TDextNatsOrderedConsumer }

constructor TDextNatsOrderedConsumer.Create(AJs: TDextNatsJetStreamContext;
  const AStreamName: string; AHandler: TNatsOrderedConsumerHandler;
  const AOptions: TNatsOrderedConsumerOptions);
var
  prefix: string;
begin
  inherited Create;
  if AJs = nil then
    raise EDextNatsException.Create('SubscribeOrdered requires a JetStream context');
  if AStreamName = '' then
    raise EDextNatsException.Create('SubscribeOrdered requires a stream name');
  if not Assigned(AHandler) then
    raise EDextNatsException.Create('SubscribeOrdered requires a message handler');
  if not AJs.Client.Connected then
    raise EDextNatsException.Create('Cannot SubscribeOrdered: NATS client is not connected');

  FJs := AJs;
  FStreamName := AStreamName;
  FOptions := AOptions;
  FHandler := AHandler;
  FLock := TCriticalSection.Create;
  FWake := TEvent.Create(nil, False, False, '');
  FPush := nil;
  FConsumerName := '';
  FDeliverSubject := '';
  FSerial := 0;
  FExpectedDseq := 1;
  FLastStreamSeq := 0;
  FLastConsumerSeq := 0;
  FActive := False;
  FStopping := False;
  FResetPending := False;
  FResetCount := 0;

  FIdleHeartbeatNs := FOptions.IdleHeartbeat;
  if FIdleHeartbeatNs <= 0 then
    FIdleHeartbeatNs := NATS_JS_ORDERED_HB_NS;

  prefix := Trim(FOptions.NamePrefix);
  if prefix = '' then
    prefix := 'ord_' + JsOrderedNewNuid;
  FOptions.NamePrefix := prefix;

  TouchActivity;
  if not TryReset(True) then
  begin
    FWake.Free;
    FLock.Free;
    raise EDextNatsException.Create('SubscribeOrdered failed to create the initial consumer');
  end;

  FMonitor := TThread.CreateAnonymousThread(MonitorLoop);
  FMonitor.FreeOnTerminate := False;
  FMonitor.Start;
end;

destructor TDextNatsOrderedConsumer.Destroy;
begin
  Stop;
  FWake.Free;
  FLock.Free;
  inherited;
end;

procedure TDextNatsOrderedConsumer.TouchActivity;
begin
  FLock.Enter;
  try
    FLastActivityMs := TThread.GetTickCount64;
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsOrderedConsumer.RequestReset;
begin
  FLock.Enter;
  try
    if FStopping or (not FActive) then
      Exit;
    if FResetPending then
      Exit;
    FResetPending := True;
  finally
    FLock.Leave;
  end;
  FWake.SetEvent;
end;

function TDextNatsOrderedConsumer.GetActive: Boolean;
begin
  FLock.Enter;
  try
    Result := FActive and (not FStopping);
  finally
    FLock.Leave;
  end;
end;

function TDextNatsOrderedConsumer.GetConsumerName: string;
begin
  FLock.Enter;
  try
    Result := FConsumerName;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsOrderedConsumer.GetLastStreamSequence: UInt64;
begin
  FLock.Enter;
  try
    Result := FLastStreamSeq;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsOrderedConsumer.GetSerial: Integer;
begin
  FLock.Enter;
  try
    Result := FSerial;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsOrderedConsumer.GetResetCount: Integer;
begin
  FLock.Enter;
  try
    Result := FResetCount;
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsOrderedConsumer.FailTerminal(const AErrorMessage: string);
var
  errHandler: TNatsOrderedConsumerErrorHandler;
begin
  FLock.Enter;
  try
    FActive := False;
    errHandler := FOptions.OnError;
  finally
    FLock.Leave;
  end;
  if Assigned(errHandler) then
  try
    errHandler(AErrorMessage);
  except
  end;
end;

function TDextNatsOrderedConsumer.BuildConsumerConfig(ASerial: Integer; const ADeliver: string;
  ARecreate: Boolean; ALastStreamSeq: UInt64): TNatsConsumerConfig;
var
  inactive: Int64;
  nextSeq: UInt64;
begin
  Result := TNatsConsumerConfig.CreateDefault;
  Result.DurableName := '';
  Result.Name := Format('%s_%d', [FOptions.NamePrefix, ASerial]);
  Result.FilterSubject := FOptions.FilterSubject;
  Result.DeliverSubject := ADeliver;
  Result.DeliverGroup := '';
  Result.AckPolicy := apNone;
  Result.MaxAckPending := 0;
  Result.MaxDeliver := 1;
  Result.AckWait := NATS_JS_ORDERED_ACK_WAIT_NS;
  Result.FlowControl := True;
  Result.IdleHeartbeat := FIdleHeartbeatNs;
  inactive := FOptions.InactiveThreshold;
  if inactive <= 0 then
    inactive := NATS_JS_ORDERED_INACTIVE_NS;
  Result.InactiveThreshold := inactive;
  Result.MemoryStorage := True;
  Result.NumReplicas := 1;
  Result.HeadersOnly := FOptions.HeadersOnly;
  Result.ReplayPolicy := rpInstant;

  if ARecreate or (ALastStreamSeq > 0) then
  begin
    nextSeq := ALastStreamSeq + 1;
    if nextSeq = 0 then
      nextSeq := 1;
    Result.DeliverPolicy := dpByStartSequence;
    Result.OptStartSeq := nextSeq;
  end
  else
  begin
    Result.DeliverPolicy := FOptions.DeliverPolicy;
    case FOptions.DeliverPolicy of
      dpByStartSequence:
        begin
          if FOptions.OptStartSeq > 0 then
            Result.OptStartSeq := FOptions.OptStartSeq
          else
            Result.OptStartSeq := 1;
        end;
      dpLastPerSubject:
        if Result.FilterSubject = '' then
          Result.FilterSubject := '>';
    else
      Result.OptStartSeq := 0;
    end;
  end;
end;

procedure TDextNatsOrderedConsumer.TeardownPushAndConsumer;
var
  push: TDextNatsJetStreamPushSubscription;
  consumerName: string;
  js: TDextNatsJetStreamContext;
  stream: string;
begin
  FLock.Enter;
  try
    push := FPush;
    FPush := nil;
    consumerName := FConsumerName;
    FConsumerName := '';
    FDeliverSubject := '';
    js := FJs;
    stream := FStreamName;
  finally
    FLock.Leave;
  end;

  if push <> nil then
  try
    push.Free;
  except
  end;

  if (js <> nil) and (consumerName <> '') then
  try
    js.DeleteConsumer(stream, consumerName);
  except
  end;
end;

procedure TDextNatsOrderedConsumer.InstallDelivery(ASerial: Integer);
var
  deliver: string;
  sid: Integer;
  selfRef: TDextNatsOrderedConsumer;
begin
  deliver := FJs.Client.NewInbox;
  selfRef := Self;
  sid := FJs.Client.Subscribe(deliver,
    procedure(const AMsg: TNatsMsg)
    begin
      selfRef.HandleRawMsg(ASerial, AMsg);
    end);
  FLock.Enter;
  try
    FPush := TDextNatsJetStreamPushSubscription.Create(FJs.Client, sid, deliver);
    FDeliverSubject := deliver;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsOrderedConsumer.TryReset(AInitial: Boolean): Boolean;
var
  serial: Integer;
  deliver: string;
  cfg: TNatsConsumerConfig;
  info: TNatsConsumerInfo;
  recreate: Boolean;
  lastStreamSeq: UInt64;
  attempts, maxAttempts, sleepMs: Integer;
  delaying: Integer;
  lastError: string;
begin
  Result := False;
  FLock.Enter;
  try
    if FStopping then
      Exit;
    maxAttempts := FOptions.MaxResetAttempts;
  finally
    FLock.Leave;
  end;
  if AInitial then
    maxAttempts := 1
  else if maxAttempts = 0 then
    maxAttempts := -1; { unlimited }

  attempts := 0;
  delaying := 200;
  lastError := '';
  while True do
  begin
    FLock.Enter;
    try
      if FStopping then
        Exit;
    finally
      FLock.Leave;
    end;

    TeardownPushAndConsumer;

    FLock.Enter;
    try
      Inc(FSerial);
      serial := FSerial;
      FExpectedDseq := 1;
      FLastConsumerSeq := 0;
      lastStreamSeq := FLastStreamSeq;
      recreate := lastStreamSeq > 0;
    finally
      FLock.Leave;
    end;

    try
      InstallDelivery(serial);
      FLock.Enter;
      try
        deliver := FDeliverSubject;
      finally
        FLock.Leave;
      end;
      cfg := BuildConsumerConfig(serial, deliver, recreate, lastStreamSeq);
      info := FJs.CreateConsumer(FStreamName, cfg);
      FLock.Enter;
      try
        FConsumerName := info.Name;
        if info.Name = '' then
          FConsumerName := cfg.Name;
        FActive := True;
        FResetPending := False;
        if recreate then
          Inc(FResetCount);
        FLastActivityMs := TThread.GetTickCount64;
      finally
        FLock.Leave;
      end;
      Result := True;
      Exit;
    except
      on E: Exception do
      begin
        TeardownPushAndConsumer;
        lastError := E.Message;
        Inc(attempts);
        if (maxAttempts > 0) and (attempts >= maxAttempts) then
        begin
          if not AInitial then
            FailTerminal(Format('Ordered consumer reset failed after %d attempts: %s',
              [attempts, lastError]));
          Exit;
        end;
        sleepMs := delaying;
        if sleepMs > 2000 then
          sleepMs := 2000;
        Sleep(sleepMs);
        delaying := delaying * 2;
        if delaying > 10000 then
          delaying := 10000;
      end;
    end;
  end;
end;

procedure TDextNatsOrderedConsumer.HandleRawMsg(ASerial: Integer; const AMsg: TNatsMsg);
var
  jsMsg: TNatsJsMsg;
  handler: TNatsOrderedConsumerHandler;
  lastConsHdr: string;
  lastCons: UInt64;
  needReset: Boolean;
  empty: TBytes;
begin
  needReset := False;
  handler := nil;
  FLock.Enter;
  try
    if FStopping or (not FActive) or (ASerial <> FSerial) then
      Exit;
    FLastActivityMs := TThread.GetTickCount64;
  finally
    FLock.Leave;
  end;

  { Status 100: idle heartbeat and/or flow-control request. }
  if AMsg.StatusCode = 100 then
  begin
    if AMsg.ReplyTo <> '' then
    begin
      SetLength(empty, 0);
      try
        FJs.Client.Publish(AMsg.ReplyTo, empty);
      except
      end;
    end;

    lastConsHdr := AMsg.Headers.GetValue('Nats-Last-Consumer');
    if lastConsHdr <> '' then
    begin
      lastCons := UInt64(StrToInt64Def(lastConsHdr, 0));
      FLock.Enter;
      try
        if (ASerial = FSerial) and (FLastConsumerSeq > 0) and (lastCons <> FLastConsumerSeq) then
          needReset := True;
      finally
        FLock.Leave;
      end;
      if needReset then
        RequestReset;
    end;
    Exit;
  end;

  if NatsJSIsFetchControl(AMsg) then
    Exit;

  jsMsg := TNatsJsMsg.FromNatsMsg(AMsg);
  FLock.Enter;
  try
    if FStopping or (not FActive) or (ASerial <> FSerial) then
      Exit;
    if jsMsg.ConsumerSequence <> FExpectedDseq then
    begin
      needReset := True;
    end
    else
    begin
      FExpectedDseq := jsMsg.ConsumerSequence + 1;
      FLastStreamSeq := jsMsg.StreamSequence;
      FLastConsumerSeq := jsMsg.ConsumerSequence;
      handler := FHandler;
    end;
  finally
    FLock.Leave;
  end;

  if needReset then
  begin
    RequestReset;
    Exit;
  end;

  if Assigned(handler) then
  try
    handler(jsMsg);
  except
  end;
end;

procedure TDextNatsOrderedConsumer.MonitorLoop;
var
  waitMs: Cardinal;
  hbMs: UInt64;
  lastAct: UInt64;
  nowMs: UInt64;
  doReset: Boolean;
  stopping: Boolean;
begin
  hbMs := UInt64(FIdleHeartbeatNs div NATS_JS_NS_PER_MS);
  if hbMs < 1000 then
    hbMs := 1000;
  waitMs := Cardinal(hbMs);
  if waitMs > 5000 then
    waitMs := 5000;

  while True do
  begin
    FWake.WaitFor(waitMs);

    FLock.Enter;
    try
      stopping := FStopping;
      doReset := FResetPending and (not FStopping);
      lastAct := FLastActivityMs;
      nowMs := TThread.GetTickCount64;
      if (not doReset) and FActive and (not FStopping) then
      begin
        if (nowMs - lastAct) >= (hbMs * NATS_JS_ORDERED_HB_THRESH) then
        begin
          FResetPending := True;
          doReset := True;
        end;
      end;
    finally
      FLock.Leave;
    end;

    if stopping then
      Break;

    if doReset then
      TryReset;
  end;
end;

procedure TDextNatsOrderedConsumer.Stop;
var
  monitor: TThread;
begin
  FLock.Enter;
  try
    if FStopping then
    begin
      monitor := nil;
    end
    else
    begin
      FStopping := True;
      FActive := False;
      FResetPending := False;
      monitor := FMonitor;
      FMonitor := nil;
    end;
  finally
    FLock.Leave;
  end;

  if FWake <> nil then
    FWake.SetEvent;

  if monitor <> nil then
  begin
    monitor.WaitFor;
    monitor.Free;
  end;

  TeardownPushAndConsumer;
end;

function TDextNatsJetStreamContext.SubscribeOrdered(const AStreamName: string;
  AHandler: TNatsOrderedConsumerHandler;
  const AOptions: TNatsOrderedConsumerOptions): TDextNatsOrderedConsumer;
begin
  Result := TDextNatsOrderedConsumer.Create(Self, AStreamName, AHandler, AOptions);
end;

function TDextNatsJetStreamContext.SubscribeOrdered(const AStreamName: string;
  AHandler: TNatsOrderedConsumerHandler): TDextNatsOrderedConsumer;
begin
  Result := SubscribeOrdered(AStreamName, AHandler, TNatsOrderedConsumerOptions.CreateDefault);
end;

function TDextNatsJetStreamContext.SubscribePush(const ADeliverSubject: string;
  AHandler: TNatsJsMsgHandler; const AQueueGroup: string): TDextNatsJetStreamPushSubscription;
var
  sid: Integer;
  handler: TNatsJsMsgHandler;
begin
  if ADeliverSubject = '' then
    raise EDextNatsException.Create('SubscribePush requires a deliver subject');
  if not Assigned(AHandler) then
    raise EDextNatsException.Create('SubscribePush requires a message handler');
  if not FClient.Connected then
    raise EDextNatsException.Create('Cannot SubscribePush: NATS client is not connected');

  handler := AHandler;
  sid := FClient.Subscribe(ADeliverSubject,
    procedure(const AMsg: TNatsMsg)
    var
      jsMsg: TNatsJsMsg;
    begin
      if NatsJSIsFetchControl(AMsg) then
        Exit;
      jsMsg := TNatsJsMsg.FromNatsMsg(AMsg);
      handler(jsMsg);
    end,
    AQueueGroup);
  Result := TDextNatsJetStreamPushSubscription.Create(FClient, sid, ADeliverSubject);
end;

function TDextNatsJetStreamContext.SubscribePush(const AStreamName, AConsumerName: string;
  AHandler: TNatsJsMsgHandler): TDextNatsJetStreamPushSubscription;
var
  info: TNatsConsumerInfo;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create('SubscribePush requires stream and consumer names');

  info := GetConsumerInfo(AStreamName, AConsumerName);
  if info.DeliverSubject = '' then
    raise EDextNatsException.CreateFmt(
      'Consumer "%s" on stream "%s" has no deliver_subject (not a push consumer)',
      [AConsumerName, AStreamName]);

  Result := SubscribePush(info.DeliverSubject, AHandler, info.DeliverGroup);
end;

function TDextNatsJetStreamContext.Fetch(const AStreamName, AConsumerName: string; ABatch: Integer;
  AExpiresMs: Integer): IList<TNatsJsMsg>;
var
  batch: Integer;
  expiresMs: Integer;
  expiresNs: Int64;
  waitMs: Integer;
  inbox: string;
  sid: Integer;
  nextSubject: string;
  requestBody: string;
  done: TEvent;
  lock: TCriticalSection;
  messages: IList<TNatsJsMsg>;
  receivedCount: Integer;
  waitResult: TWaitResult;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create('Fetch requires stream and consumer names');
  if not FClient.Connected then
    raise EDextNatsException.Create('Cannot Fetch: NATS client is not connected');

  batch := ABatch;
  if batch <= 0 then
    batch := 1;
  expiresMs := AExpiresMs;
  if expiresMs < 0 then
    expiresMs := 0;
  expiresNs := Int64(expiresMs) * 1000000;
  waitMs := expiresMs + 5000;
  if waitMs < 1000 then
    waitMs := 1000;

  messages := TCollections.CreateList<TNatsJsMsg>;
  Result := messages;
  receivedCount := 0;

  done := TEvent.Create(nil, True, False, '');
  lock := TCriticalSection.Create;
  try
    inbox := FClient.NewInbox;
    sid := FClient.Subscribe(inbox,
      procedure(const AMsg: TNatsMsg)
      var
        jsMsg: TNatsJsMsg;
        isControl: Boolean;
        signal: Boolean;
      begin
        isControl := NatsJSIsFetchControl(AMsg);
        signal := False;

        lock.Enter;
        try
          if not isControl then
          begin
            jsMsg := TNatsJsMsg.FromNatsMsg(AMsg);
            messages.Add(jsMsg);
            Inc(receivedCount);
          end;
          if isControl or (receivedCount >= batch) then
            signal := True;
        finally
          lock.Leave;
        end;

        if signal then
          done.SetEvent;
      end);

    try
      FClient.Unsubscribe(sid, batch + 5);

      nextSubject := Format('%sCONSUMER.MSG.NEXT.%s.%s', [FApiPrefix, AStreamName, AConsumerName]);
      if expiresNs > 0 then
        requestBody := Format('{"batch":%d,"expires":%d}', [batch, expiresNs])
      else
        requestBody := Format('{"batch":%d}', [batch]);

      FClient.Publish(nextSubject, requestBody, inbox);

      waitResult := done.WaitFor(Cardinal(waitMs));
      case waitResult of
        wrSignaled:
          ; // ok — messages and/or a control frame arrived
        wrTimeout:
          begin
            { Soft timeout: return whatever was collected (may be empty). }
            FClient.Unsubscribe(sid, 0);
          end;
      else
        FClient.Unsubscribe(sid, 0);
        raise EDextNatsException.Create('Error waiting for JetStream Fetch response');
      end;
    except
      try
        FClient.Unsubscribe(sid, 0);
      except
      end;
      raise;
    end;
  finally
    lock.Free;
    done.Free;
  end;
end;

procedure TDextNatsJetStreamContext.Ack(const AMsg: TNatsJsMsg);
begin
  Ack(AMsg.ReplyTo);
end;

procedure TDextNatsJetStreamContext.Ack(const AReplyTo: string);
begin
  PublishAckPayload(AReplyTo, '+ACK');
end;

procedure TDextNatsJetStreamContext.Nak(const AMsg: TNatsJsMsg; ADelayMs: Integer);
begin
  Nak(AMsg.ReplyTo, ADelayMs);
end;

procedure TDextNatsJetStreamContext.Nak(const AReplyTo: string; ADelayMs: Integer);
var
  payload: string;
begin
  if ADelayMs > 0 then
    payload := Format('+NAK {"delay":%d}', [Int64(ADelayMs) * 1000000])
  else
    payload := '+NAK';
  PublishAckPayload(AReplyTo, payload);
end;

procedure TDextNatsJetStreamContext.Term(const AMsg: TNatsJsMsg);
begin
  Term(AMsg.ReplyTo);
end;

procedure TDextNatsJetStreamContext.Term(const AReplyTo: string);
begin
  PublishAckPayload(AReplyTo, '+TERM');
end;

procedure TDextNatsJetStreamContext.InProgress(const AMsg: TNatsJsMsg);
begin
  InProgress(AMsg.ReplyTo);
end;

procedure TDextNatsJetStreamContext.InProgress(const AReplyTo: string);
begin
  PublishAckPayload(AReplyTo, '+WPI');
end;

function TDextNatsJetStreamContext.Publish(const ASubject: string; const APayload: TBytes;
  const AOptions: TNatsJetStreamPublishOptions): TNatsPublishAck;
var
  headers: TNatsHeaders;
  replyMsg: TNatsMsg;
  i: Integer;
begin
  headers := nil;
  if AOptions.MsgId <> '' then
    headers.Add('Nats-Msg-Id', AOptions.MsgId);
  if AOptions.ExpectedStream <> '' then
    headers.Add('Nats-Expected-Stream', AOptions.ExpectedStream);
  if AOptions.ExpectedLastSequence > 0 then
    headers.Add('Nats-Expected-Last-Sequence', AOptions.ExpectedLastSequence.ToString);
  if AOptions.ExpectedLastSubjectSequenceSet then
    headers.Add('Nats-Expected-Last-Subject-Sequence', AOptions.ExpectedLastSubjectSequence.ToString);
  if AOptions.ExpectedLastMsgId <> '' then
    headers.Add('Nats-Expected-Last-Msg-Id', AOptions.ExpectedLastMsgId);
  if AOptions.MsgTTL > 0 then
  begin
    { ADR-43: Go duration string or integer seconds; whole seconds as "Ns". }
    if AOptions.MsgTTL < 1000000000 then
      raise EDextNatsException.Create(
        'Nats-TTL (MsgTTL) must be at least 1 second (1000000000 ns)');
    headers.Add('Nats-TTL', IntToStr(AOptions.MsgTTL div 1000000000) + 's');
  end;
  for i := 0 to High(AOptions.ExtraHeaders) do
    headers.Add(AOptions.ExtraHeaders[i].Key, AOptions.ExtraHeaders[i].Value);

  replyMsg := FClient.RequestWithHeaders(ASubject, APayload, headers, AOptions.TimeoutMs);
  Result := TNatsPublishAck.Parse(replyMsg.AsString);
end;

function TDextNatsJetStreamContext.Publish(const ASubject: string; const APayload: TBytes;
  const AMsgId: string): TNatsPublishAck;
var
  opts: TNatsJetStreamPublishOptions;
begin
  opts := TNatsJetStreamPublishOptions.CreateDefault;
  opts.MsgId := AMsgId;
  Result := Publish(ASubject, APayload, opts);
end;

function TDextNatsJetStreamContext.Publish(const ASubject, AMessage: string;
  const AMsgId: string): TNatsPublishAck;
begin
  Result := Publish(ASubject, TEncoding.UTF8.GetBytes(AMessage), AMsgId);
end;

function TDextNatsJetStreamContext.PublishWithHeaders(const ASubject: string; const APayload: TBytes;
  const AHeaders: TNatsHeaders; ATimeoutMs: Integer): TNatsPublishAck;
var
  opts: TNatsJetStreamPublishOptions;
begin
  opts := TNatsJetStreamPublishOptions.CreateDefault;
  opts.ExtraHeaders := AHeaders;
  opts.TimeoutMs := ATimeoutMs;
  Result := Publish(ASubject, APayload, opts);
end;

end.
