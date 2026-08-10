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
{  delete), dedup'd publish with a Nats-Msg-Id header, pull-consumer admin  }
{  (create/info/delete), Fetch, and Ack/Nak/Term/InProgress — all built on  }
{  plain request/reply and PUB against $JS.API.* subjects.                  }
{  TDextNatsJetStreamContext wraps an already-connected TDextNatsClient     }
{  (composition); it neither owns nor frees the client.                     }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
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

  /// <summary>Consumer acknowledgement policy.</summary>
  TNatsAckPolicy = (apNone, apAll, apExplicit);
  /// <summary>Which messages a consumer starts from.</summary>
  TNatsDeliverPolicy = (dpAll, dpLast, dpNew, dpByStartSequence, dpByStartTime, dpLastPerSubject);
  /// <summary>Replay timing for a consumer.</summary>
  TNatsReplayPolicy = (rpInstant, rpOriginal);

  /// <summary>Configuration used to create or describe a JetStream stream.</summary>
  TNatsStreamConfig = record
    Name: string;
    Subjects: TArray<string>;
    Retention: TNatsStreamRetention;
    Storage: TNatsStreamStorage;
    MaxConsumers, MaxMsgSize, NumReplicas: Integer;
    MaxMsgs, MaxBytes: Int64;
    MaxAge: Int64; // nanoseconds, 0 = unlimited
    Discard: TNatsStreamDiscard;
    DuplicateWindow: Int64; // nanoseconds, default 120e9 (2 minutes, NATS server default)
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
    DeliverPolicy: TNatsDeliverPolicy;
    /// <summary>Optional start sequence when DeliverPolicy = dpByStartSequence.</summary>
    OptStartSeq: UInt64;
    AckPolicy: TNatsAckPolicy;
    /// <summary>How long the server waits for an Ack before redelivery, in nanoseconds. 0 = server default.</summary>
    AckWait: Int64;
    MaxDeliver: Integer;
    MaxAckPending: Integer;
    MaxWaiting: Integer;
    ReplayPolicy: TNatsReplayPolicy;
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
    /// <summary>Sent as Nats-Expected-Last-Msg-Id if non-empty; optimistic-concurrency guard.</summary>
    ExpectedLastMsgId: string;
    /// <summary>Request timeout in milliseconds; 0 = use the client's RequestTimeoutMs.</summary>
    TimeoutMs: Integer;
    /// <summary>Sensible defaults: no dedup key, no expectations, client default timeout.</summary>
    class function CreateDefault: TNatsJetStreamPublishOptions; static;
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

  /// <summary>A JetStream message returned by <see cref="TDextNatsJetStreamContext.Fetch"/>.</summary>
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

  /// <summary>
  ///   Thin JetStream wrapper around an already-connected <see cref="TDextNatsClient"/>.
  ///   Stream admin, dedup'd publish, pull-consumer admin, Fetch, and Ack helpers.
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

    /// <summary>Creates a consumer on AStreamName from AConfig (pull consumer when no deliver_subject).</summary>
    function CreateConsumer(const AStreamName: string; const AConfig: TNatsConsumerConfig): TNatsConsumerInfo;
    /// <summary>Fetches current metadata for a consumer. Raises EDextNatsJetStreamError if missing.</summary>
    function GetConsumerInfo(const AStreamName, AConsumerName: string): TNatsConsumerInfo;
    /// <summary>Deletes a consumer. Raises EDextNatsJetStreamError on failure.</summary>
    function DeleteConsumer(const AStreamName, AConsumerName: string): Boolean;

    /// <summary>
    ///   Pulls up to ABatch messages from a pull consumer. AExpiresMs is how long the server
    ///   may hold the request open (sent as nanoseconds in the NEXT request body).
    ///   Control messages (100/404/408/409) end the wait and are not included in the result.
    /// </summary>
    function Fetch(const AStreamName, AConsumerName: string; ABatch: Integer = 1;
      AExpiresMs: Integer = 5000): IList<TNatsJsMsg>;

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

    /// <summary>The wrapped client. Its lifetime remains the caller's responsibility.</summary>
    property Client: TDextNatsClient read FClient;
  end;

implementation

/// <summary>Raises EDextNatsJetStreamError if AObj carries a top-level JSON "error" object;
/// otherwise does nothing. Used by every JetStream response parser in this unit.</summary>
procedure NatsJSRaiseIfError(AObj: TJSONObject);
var
  errVal: TJSONValue;
  errObj: TJSONObject;
begin
  if not Assigned(AObj) then Exit;

  errVal := AObj.GetValue('error');
  if not Assigned(errVal) or not (errVal is TJSONObject) then Exit;

  errObj := TJSONObject(errVal);
  raise EDextNatsJetStreamError.CreateFromApi(
    NatsJsonGetInt(errObj, 'code'),
    NatsJsonGetInt(errObj, 'err_code'),
    NatsJsonGetStr(errObj, 'description'));
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

{ EDextNatsJetStreamError }

constructor EDextNatsJetStreamError.CreateFromApi(ACode, AErrCode: Integer; const ADescription: string);
begin
  Code := ACode;
  ErrCode := AErrCode;
  inherited CreateFmt('NATS JetStream API error %d (code %d): %s', [AErrCode, ACode, ADescription]);
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
end;

function TNatsStreamConfig.ToJson: string;
var
  sb: TStringBuilder;
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

  sb := TStringBuilder.Create;
  try
    sb.Append('{');
    sb.Append('"name":"').Append(NatsJsonEscape(Name)).Append('",');

    sb.Append('"subjects":[');
    for i := 0 to High(Subjects) do
    begin
      if i > 0 then
        sb.Append(',');
      sb.Append('"').Append(NatsJsonEscape(Subjects[i])).Append('"');
    end;
    sb.Append('],');

    sb.Append('"retention":"').Append(retentionStr).Append('",');
    sb.Append('"storage":"').Append(storageStr).Append('",');
    sb.Append('"max_consumers":').Append(MaxConsumers).Append(',');
    sb.Append('"max_msgs":').Append(MaxMsgs).Append(',');
    sb.Append('"max_bytes":').Append(MaxBytes).Append(',');
    sb.Append('"max_age":').Append(MaxAge).Append(',');
    sb.Append('"max_msg_size":').Append(MaxMsgSize).Append(',');
    sb.Append('"discard":"').Append(discardStr).Append('",');
    sb.Append('"num_replicas":').Append(NumReplicas).Append(',');
    sb.Append('"duplicate_window":').Append(DuplicateWindow);
    sb.Append('}');
    Result := sb.ToString;
  finally
    sb.Free;
  end;
end;

{ TNatsStreamInfo }

class function TNatsStreamInfo.Parse(const AJson: string): TNatsStreamInfo;
var
  obj: TJSONObject;
  configObj, stateObj: TJSONObject;
  v: TJSONValue;
begin
  FillChar(Result, SizeOf(Result), 0);
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty JetStream API response');

  obj := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  if not Assigned(obj) then
    raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  try
    NatsJSRaiseIfError(obj);

    configObj := nil;
    v := obj.GetValue('config');
    if Assigned(v) and (v is TJSONObject) then
      configObj := TJSONObject(v);
    Result.Name := NatsJsonGetStr(configObj, 'name');

    stateObj := nil;
    v := obj.GetValue('state');
    if Assigned(v) and (v is TJSONObject) then
      stateObj := TJSONObject(v);
    Result.Messages := UInt64(NatsJsonGetInt64(stateObj, 'messages'));
    Result.Bytes := UInt64(NatsJsonGetInt64(stateObj, 'bytes'));
    Result.FirstSeq := UInt64(NatsJsonGetInt64(stateObj, 'first_seq'));
    Result.LastSeq := UInt64(NatsJsonGetInt64(stateObj, 'last_seq'));
    Result.ConsumerCount := NatsJsonGetInt(stateObj, 'consumer_count');
  finally
    obj.Free;
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
  sb: TStringBuilder;
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

  sb := TStringBuilder.Create;
  try
    sb.Append('{');
    if DurableName <> '' then
      sb.Append('"durable_name":"').Append(NatsJsonEscape(DurableName)).Append('",');
    if Name <> '' then
      sb.Append('"name":"').Append(NatsJsonEscape(Name)).Append('",');
    if Description <> '' then
      sb.Append('"description":"').Append(NatsJsonEscape(Description)).Append('",');
    if FilterSubject <> '' then
      sb.Append('"filter_subject":"').Append(NatsJsonEscape(FilterSubject)).Append('",');
    sb.Append('"deliver_policy":"').Append(deliverStr).Append('",');
    if (DeliverPolicy = dpByStartSequence) and (OptStartSeq > 0) then
      sb.Append('"opt_start_seq":').Append(OptStartSeq).Append(',');
    sb.Append('"ack_policy":"').Append(ackStr).Append('",');
    if AckWait > 0 then
      sb.Append('"ack_wait":').Append(AckWait).Append(',');
    sb.Append('"max_deliver":').Append(MaxDeliver).Append(',');
    sb.Append('"max_ack_pending":').Append(MaxAckPending).Append(',');
    sb.Append('"max_waiting":').Append(MaxWaiting).Append(',');
    sb.Append('"replay_policy":"').Append(replayStr).Append('"');
    sb.Append('}');
    Result := sb.ToString;
  finally
    sb.Free;
  end;
end;

{ TNatsConsumerInfo }

class function TNatsConsumerInfo.Parse(const AJson: string): TNatsConsumerInfo;
var
  obj: TJSONObject;
  configObj: TJSONObject;
  v: TJSONValue;
begin
  FillChar(Result, SizeOf(Result), 0);
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty JetStream API response');

  obj := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  if not Assigned(obj) then
    raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  try
    NatsJSRaiseIfError(obj);

    Result.StreamName := NatsJsonGetStr(obj, 'stream_name');
    Result.Name := NatsJsonGetStr(obj, 'name');
    Result.NumPending := UInt64(NatsJsonGetInt64(obj, 'num_pending'));
    Result.NumAckPending := NatsJsonGetInt(obj, 'num_ack_pending');
    Result.NumRedelivered := NatsJsonGetInt(obj, 'num_redelivered');
    Result.NumWaiting := NatsJsonGetInt(obj, 'num_waiting');

    configObj := nil;
    v := obj.GetValue('config');
    if Assigned(v) and (v is TJSONObject) then
      configObj := TJSONObject(v);
    Result.DurableName := NatsJsonGetStr(configObj, 'durable_name');
    Result.FilterSubject := NatsJsonGetStr(configObj, 'filter_subject');
    if Result.Name = '' then
      Result.Name := Result.DurableName;
  finally
    obj.Free;
  end;
end;

{ TNatsJetStreamPublishOptions }

class function TNatsJetStreamPublishOptions.CreateDefault: TNatsJetStreamPublishOptions;
begin
  FillChar(Result, SizeOf(Result), 0);
  Result.MsgId := '';
  Result.ExpectedStream := '';
  Result.ExpectedLastSequence := 0;
  Result.ExpectedLastMsgId := '';
  Result.TimeoutMs := 0;
end;

{ TNatsPublishAck }

class function TNatsPublishAck.Parse(const AJson: string): TNatsPublishAck;
var
  obj: TJSONObject;
begin
  FillChar(Result, SizeOf(Result), 0);
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty JetStream publish acknowledgement payload');

  obj := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  if not Assigned(obj) then
    raise EDextNatsProtocolError.CreateFmt('Malformed JetStream publish acknowledgement payload: %s', [AJson]);
  try
    NatsJSRaiseIfError(obj);

    Result.Stream := NatsJsonGetStr(obj, 'stream');
    Result.Sequence := UInt64(NatsJsonGetInt64(obj, 'seq'));
    Result.Duplicate := NatsJsonGetBool(obj, 'duplicate', False);
    Result.Domain := NatsJsonGetStr(obj, 'domain');
  finally
    obj.Free;
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
var
  obj: TJSONObject;
  json: string;
begin
  json := ApiRequest('STREAM.DELETE.' + AStreamName, '{}');

  obj := TJSONObject.ParseJSONValue(json) as TJSONObject;
  if not Assigned(obj) then
    raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [json]);
  try
    NatsJSRaiseIfError(obj);
    Result := NatsJsonGetBool(obj, 'success');
  finally
    obj.Free;
  end;
end;

function TDextNatsJetStreamContext.CreateConsumer(const AStreamName: string;
  const AConfig: TNatsConsumerConfig): TNatsConsumerInfo;
var
  consumerPart, subject, body: string;
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

  body := Format('{"stream_name":"%s","config":%s}',
    [NatsJsonEscape(AStreamName), AConfig.ToJson]);
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
var
  obj: TJSONObject;
  json: string;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create('DeleteConsumer requires stream and consumer names');

  json := ApiRequest(Format('CONSUMER.DELETE.%s.%s', [AStreamName, AConsumerName]), '{}');
  obj := TJSONObject.ParseJSONValue(json) as TJSONObject;
  if not Assigned(obj) then
    raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [json]);
  try
    NatsJSRaiseIfError(obj);
    Result := NatsJsonGetBool(obj, 'success');
  finally
    obj.Free;
  end;
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
begin
  if AOptions.MsgId <> '' then
    headers.Add('Nats-Msg-Id', AOptions.MsgId);
  if AOptions.ExpectedStream <> '' then
    headers.Add('Nats-Expected-Stream', AOptions.ExpectedStream);
  if AOptions.ExpectedLastSequence > 0 then
    headers.Add('Nats-Expected-Last-Sequence', AOptions.ExpectedLastSequence.ToString);
  if AOptions.ExpectedLastMsgId <> '' then
    headers.Add('Nats-Expected-Last-Msg-Id', AOptions.ExpectedLastMsgId);

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

end.
