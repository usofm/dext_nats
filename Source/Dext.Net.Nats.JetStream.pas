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
{  JetStream layer for the NATS client: producer-side only. Stream admin    }
{  (create/info/delete) and dedup'd publish with a Nats-Msg-Id header,       }
{  built entirely on plain request/reply against $JS.API.* subjects — no    }
{  new frame types are needed. TDextNatsJetStreamContext wraps an already   }
{  connected TDextNatsClient (composition, one-directional layering); it    }
{  neither owns nor frees the client. Consumer-side JetStream (pull/push    }
{  consumers, Ack/Nak/Term) is not implemented yet.                         }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
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
    /// <summary>Serializes the record to the JSON body expected by STREAM.CREATE.</summary>
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

  /// <summary>
  ///   Thin JetStream wrapper around an already-connected <see cref="TDextNatsClient"/>.
  ///   Producer-side only: stream admin (create/info/delete) and dedup'd publish. Does not
  ///   own the wrapped client; the caller remains responsible for its lifetime.
  /// </summary>
  TDextNatsJetStreamContext = class
  private
    FClient: TDextNatsClient;
    FApiPrefix: string;
    /// <summary>Issues a plain (no-headers) JetStream API request and returns the raw reply body.</summary>
    function ApiRequest(const ASubjectSuffix, ABody: string): string;
  public
    /// <summary>Wraps AClient. AApiPrefix defaults to "$JS.API." (no custom JetStream domain).</summary>
    constructor Create(AClient: TDextNatsClient; const AApiPrefix: string = '$JS.API.');

    /// <summary>Creates a stream from AConfig. Raises EDextNatsJetStreamError on failure
    /// (e.g. the stream already exists with a different configuration).</summary>
    function CreateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
    /// <summary>Fetches current metadata for AStreamName. Raises EDextNatsJetStreamError if it does not exist.</summary>
    function GetStreamInfo(const AStreamName: string): TNatsStreamInfo;
    /// <summary>True if AStreamName exists. False only for a "stream not found" API error;
    /// any other failure propagates.</summary>
    function StreamExists(const AStreamName: string): Boolean;
    /// <summary>Deletes AStreamName and all of its messages. Raises EDextNatsJetStreamError on failure.</summary>
    function DeleteStream(const AStreamName: string): Boolean;

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

{ TDextNatsJetStreamContext }

constructor TDextNatsJetStreamContext.Create(AClient: TDextNatsClient; const AApiPrefix: string);
begin
  inherited Create;
  FClient := AClient;
  FApiPrefix := AApiPrefix;
end;

function TDextNatsJetStreamContext.ApiRequest(const ASubjectSuffix, ABody: string): string;
begin
  Result := FClient.Request(FApiPrefix + ASubjectSuffix, TEncoding.UTF8.GetBytes(ABody)).AsString;
end;

function TDextNatsJetStreamContext.CreateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
begin
  Result := TNatsStreamInfo.Parse(ApiRequest('STREAM.CREATE.' + AConfig.Name, AConfig.ToJson));
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
