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
{                                                                           }
{***************************************************************************}
{                                                                           }
{  JetStream Key-Value store (MVP). Buckets are streams named KV_<bucket>   }
{  with subjects $KV.<bucket>.>; keys map to $KV.<bucket>.<key>.            }
{  TDextNatsKeyValue wraps an existing TDextNatsJetStreamContext by         }
{  composition and does not own its lifetime. Keys / History / Watch(All)   }
{  use ephemeral JetStream consumers (pull for Keys/History, push for       }
{  Watch). CAS Create/Update use Nats-Expected-Last-Subject-Sequence;       }
{  per-key TTL and Watch end-of-initial marker remain deferred — see       }
{  Docs/NATS_DEXT_ROADMAP.md SPEC-KV-01.                                    }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.KeyValue;

interface

uses
  System.SysUtils,
  System.Classes,
  Dext.Collections,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

const
  /// <summary>Backing stream name prefix for KV buckets.</summary>
  NATS_KV_STREAM_PREFIX = 'KV_';
  /// <summary>Subject prefix template: Format(NATS_KV_SUBJECT_PREFIX, [Bucket]) + Key.</summary>
  NATS_KV_SUBJECT_PREFIX = '$KV.%s.';
  /// <summary>Wildcard subjects for a bucket: Format(NATS_KV_SUBJECTS, [Bucket]).</summary>
  NATS_KV_SUBJECTS = '$KV.%s.>';
  NATS_KV_OP_HEADER = 'KV-Operation';
  NATS_KV_OP_DEL = 'DEL';
  NATS_KV_OP_PURGE = 'PURGE';
  NATS_KV_ROLLUP_HEADER = 'Nats-Rollup';
  NATS_KV_ROLLUP_SUBJECT = 'sub';
  /// <summary>Maximum history depth accepted by nats-server for KV buckets.</summary>
  NATS_KV_MAX_HISTORY = 64;

type
  /// <summary>Raised for Key-Value validation / not-found errors (distinct from JetStream API errors).</summary>
  EDextNatsKeyValueError = class(EDextNatsException);

  /// <summary>Raised when Get cannot find a live (non-deleted) value for a key.</summary>
  EDextNatsKeyNotFound = class(EDextNatsKeyValueError);

  /// <summary>Raised by Create when the key already has a live (non-tombstone) value.</summary>
  EDextNatsKeyExists = class(EDextNatsKeyValueError);

  /// <summary>Raised by Update when ARevision does not match the key's current revision.</summary>
  EDextNatsKeyRevisionMismatch = class(EDextNatsKeyValueError);

  /// <summary>Operation encoded on a KV entry (PUT, or tombstone DEL / PURGE).</summary>
  TNatsKvOperation = (kvoPut, kvoDelete, kvoPurge);

  /// <summary>Configuration used to create a JetStream Key-Value bucket.</summary>
  TNatsKeyValueConfig = record
    Bucket: string;
    Description: string;
    /// <summary>Revisions kept per key (1..64). Default 1.</summary>
    History: Integer;
    MaxBytes: Int64;
    MaxValueSize: Integer;
    /// <summary>Bucket TTL as stream MaxAge, in nanoseconds. 0 = unlimited.</summary>
    TTL: Int64;
    Storage: TNatsStreamStorage;
    NumReplicas: Integer;
    /// <summary>Defaults: history=1, file storage, unlimited size, 1 replica.</summary>
    class function CreateDefault(const ABucket: string): TNatsKeyValueConfig; static;
    /// <summary>Maps this bucket config onto a <see cref="TNatsStreamConfig"/> for STREAM.CREATE.</summary>
    function ToStreamConfig: TNatsStreamConfig;
  end;

  /// <summary>Bucket status derived from the backing KV_* stream info.</summary>
  TNatsKeyValueStatus = record
    Bucket: string;
    StreamName: string;
    Values: UInt64;
    Bytes: UInt64;
    History: Integer;
    class function FromStreamInfo(const ABucket: string; const AInfo: TNatsStreamInfo;
      AHistory: Integer = 1): TNatsKeyValueStatus; static;
  end;

  /// <summary>One revision of a key (value + metadata).</summary>
  TNatsKeyValueEntry = record
    Bucket: string;
    Key: string;
    Value: TBytes;
    Revision: UInt64;
    Operation: TNatsKvOperation;
    Created: string;
    /// <summary>Decodes Value as UTF-8.</summary>
    function AsString: string;
    /// <summary>True when Operation is a PUT (not a delete/purge marker).</summary>
    function IsPut: Boolean;
  end;

  /// <summary>Callback for <see cref="TDextNatsKeyValue.Watch"/> / WatchAll deliveries.</summary>
  TNatsKeyValueWatchHandler = reference to procedure(const AEntry: TNatsKeyValueEntry);

  /// <summary>
  ///   Active KV watch (ephemeral push consumer + deliver-subject SUB).
  ///   Does not own the JetStream context. Call <see cref="Stop"/> or Free before
  ///   freeing the KeyValue store. Handlers run on the client's receive thread —
  ///   do not block with Request/Fetch.
  /// </summary>
  TDextNatsKeyValueWatcher = class
  private
    FJs: TDextNatsJetStreamContext;
    FStreamName: string;
    FConsumerName: string;
    FPushSub: TDextNatsJetStreamPushSubscription;
    FActive: Boolean;
  public
    constructor Create(AJs: TDextNatsJetStreamContext; const AStreamName, AConsumerName: string;
      APushSub: TDextNatsJetStreamPushSubscription);
    destructor Destroy; override;
    /// <summary>Unsubscribes the push SUB and deletes the ephemeral consumer. Idempotent.</summary>
    procedure Stop;
    property Active: Boolean read FActive;
    property ConsumerName: string read FConsumerName;
  end;

  /// <summary>
  ///   Key-Value store bound to one bucket. Wraps a <see cref="TDextNatsJetStreamContext"/>
  ///   by composition; does not own or free the context / client.
  /// </summary>
  TDextNatsKeyValue = class
  private
    FJs: TDextNatsJetStreamContext;
    FBucket: string;
    FStreamName: string;
    FSubjectPrefix: string;
    class function StreamNameForBucket(const ABucket: string): string; static;
    class function SubjectForKey(const ABucket, AKey: string): string; static;
    class procedure ValidateBucketName(const ABucket: string); static;
    class procedure ValidateKeyName(const AKey: string); static;
    class function IsValidBucketChar(C: Char): Boolean; static;
    class function IsValidKeyChar(C: Char): Boolean; static;
    function KeyFromSubject(const ASubject: string): string;
    function EntryFromStored(const AKey: string; const AMsg: TNatsStoredMsg): TNatsKeyValueEntry;
    function EntryFromJsMsg(const AMsg: TNatsJsMsg): TNatsKeyValueEntry;
    procedure PublishMarker(const AKey, AOperation: string; APurge: Boolean);
    class function IsWrongLastSequence(const E: EDextNatsJetStreamError): Boolean; static;
    /// <summary>Publishes with Nats-Expected-Last-Subject-Sequence = ARevision (CAS).</summary>
    function PutExpected(const AKey: string; const AValue: TBytes; ARevision: UInt64): UInt64;
    /// <summary>CAS create implementation shared by Create overloads (avoids ctor name clash).</summary>
    function CreateKey(const AKey: string; const AValue: TBytes): UInt64;
    function PullSubjectEntries(const AFilterSubject: string; ADeliver: TNatsDeliverPolicy;
      AIncludeDeletes: Boolean): IList<TNatsKeyValueEntry>;
    function StartWatch(const AFilterSubject: string;
      AHandler: TNatsKeyValueWatchHandler): TDextNatsKeyValueWatcher;
  public
    /// <summary>Binds to an existing bucket (does not create it). Validates the name only.</summary>
    constructor Create(AJs: TDextNatsJetStreamContext; const ABucket: string); overload;

    /// <summary>Creates the backing KV_* stream and returns a store bound to it.
    /// Caller must Free the result; the JetStream context remains owned by the caller.</summary>
    class function CreateBucket(AJs: TDextNatsJetStreamContext;
      const AConfig: TNatsKeyValueConfig): TDextNatsKeyValue; static;
    /// <summary>Deletes the backing stream KV_&lt;bucket&gt;.</summary>
    class function DeleteBucket(AJs: TDextNatsJetStreamContext; const ABucket: string): Boolean; static;
    /// <summary>True if the backing stream exists.</summary>
    class function BucketExists(AJs: TDextNatsJetStreamContext; const ABucket: string): Boolean; static;
    /// <summary>Status for an existing bucket (raises if missing).</summary>
    class function GetStatus(AJs: TDextNatsJetStreamContext;
      const ABucket: string): TNatsKeyValueStatus; static;
    /// <summary>Opens a store for an existing bucket (raises EDextNatsJetStreamError if missing).</summary>
    class function Open(AJs: TDextNatsJetStreamContext; const ABucket: string): TDextNatsKeyValue; static;

    /// <summary>Puts AValue under AKey; returns the new revision (stream sequence).</summary>
    function Put(const AKey: string; const AValue: TBytes): UInt64; overload;
    /// <summary>UTF-8 convenience overload of Put.</summary>
    function Put(const AKey, AValue: string): UInt64; overload;
    /// <summary>
    ///   Creates AKey only if it does not exist (CAS expected last subject sequence 0).
    ///   After Delete/Purge, retries against the tombstone revision (official NATS KV semantics).
    ///   Raises <see cref="EDextNatsKeyExists"/> if a live value is already present.
    /// </summary>
    function Create(const AKey: string; const AValue: TBytes): UInt64; overload;
    /// <summary>UTF-8 convenience overload of Create (CAS).</summary>
    function Create(const AKey, AValue: string): UInt64; overload;
    /// <summary>
    ///   Updates AKey only if its current revision equals ARevision
    ///   (Nats-Expected-Last-Subject-Sequence). Raises
    ///   <see cref="EDextNatsKeyRevisionMismatch"/> on conflict.
    /// </summary>
    function Update(const AKey: string; const AValue: TBytes; ARevision: UInt64): UInt64; overload;
    /// <summary>UTF-8 convenience overload of Update.</summary>
    function Update(const AKey, AValue: string; ARevision: UInt64): UInt64; overload;
    /// <summary>Returns the latest live value. Raises EDextNatsKeyNotFound if absent or tombstoned.</summary>
    function Get(const AKey: string): TNatsKeyValueEntry;
    /// <summary>Like Get but returns False instead of raising when the key is missing/deleted.</summary>
    function TryGet(const AKey: string; out AEntry: TNatsKeyValueEntry): Boolean;
    /// <summary>Writes a DEL marker (history retained up to bucket History).</summary>
    procedure Delete(const AKey: string);
    /// <summary>Writes a PURGE rollup marker (prior revisions removed for the key).</summary>
    procedure Purge(const AKey: string);
    /// <summary>Current bucket status from STREAM.INFO.</summary>
    function Status: TNatsKeyValueStatus;

    /// <summary>
    ///   Live key names (latest revision is a PUT). Ephemeral pull consumer with
    ///   deliver_policy=last_per_subject on $KV.&lt;bucket&gt;.&gt;.
    /// </summary>
    function Keys: IList<string>;
    /// <summary>Alias of <see cref="Keys"/>.</summary>
    function ListKeys: IList<string>;
    /// <summary>
    ///   All retained revisions for AKey (oldest first), including DEL/PURGE markers.
    ///   Ephemeral pull consumer with deliver_policy=all on the key subject.
    /// </summary>
    function History(const AKey: string): IList<TNatsKeyValueEntry>;
    /// <summary>
    ///   Watches one key: delivers the current last-per-subject value (if any), then updates.
    ///   Caller must Free the watcher (or Stop) before freeing this store.
    /// </summary>
    function Watch(const AKey: string; AHandler: TNatsKeyValueWatchHandler): TDextNatsKeyValueWatcher;
    /// <summary>
    ///   Watches the whole bucket (last-per-subject snapshot + subsequent updates).
    ///   Minimal MVP: no end-of-initial marker; handlers run on the receive thread.
    /// </summary>
    function WatchAll(AHandler: TNatsKeyValueWatchHandler): TDextNatsKeyValueWatcher;

    property Bucket: string read FBucket;
    property StreamName: string read FStreamName;
    property JetStream: TDextNatsJetStreamContext read FJs;
  end;

implementation

function KvNewNuid: string;
var
  guid: TGUID;
  s: string;
begin
  CreateGUID(guid);
  s := GUIDToString(guid);
  s := StringReplace(s, '{', '', [rfReplaceAll]);
  s := StringReplace(s, '}', '', [rfReplaceAll]);
  Result := StringReplace(s, '-', '', [rfReplaceAll]);
end;

{ TNatsKeyValueConfig }

class function TNatsKeyValueConfig.CreateDefault(const ABucket: string): TNatsKeyValueConfig;
begin
  Result := Default(TNatsKeyValueConfig);
  Result.Bucket := ABucket;
  Result.History := 1;
  Result.MaxBytes := -1;
  Result.MaxValueSize := -1;
  Result.TTL := 0;
  Result.Storage := ssFile;
  Result.NumReplicas := 1;
end;

function TNatsKeyValueConfig.ToStreamConfig: TNatsStreamConfig;
var
  hist: Integer;
  dupWindow: Int64;
begin
  TDextNatsKeyValue.ValidateBucketName(Bucket);
  hist := Self.History;
  if hist <= 0 then
    hist := 1;
  if hist > NATS_KV_MAX_HISTORY then
    raise EDextNatsKeyValueError.CreateFmt(
      'KV history %d exceeds maximum %d', [hist, NATS_KV_MAX_HISTORY]);

  Result := TNatsStreamConfig.CreateDefault(
    TDextNatsKeyValue.StreamNameForBucket(Bucket),
    [Format(NATS_KV_SUBJECTS, [Bucket])]);
  Result.Description := Description;
  Result.MaxMsgsPerSubject := hist;
  Result.MaxBytes := MaxBytes;
  if MaxBytes = 0 then
    Result.MaxBytes := -1;
  Result.MaxMsgSize := MaxValueSize;
  if MaxValueSize = 0 then
    Result.MaxMsgSize := -1;
  Result.MaxAge := TTL;
  Result.Storage := Storage;
  Result.NumReplicas := NumReplicas;
  if Result.NumReplicas <= 0 then
    Result.NumReplicas := 1;
  Result.Discard := sdNew;
  Result.AllowDirect := True;
  Result.DenyDelete := True;
  Result.AllowRollup := True;
  Result.MaxMsgs := -1;
  Result.MaxConsumers := -1;
  dupWindow := 120000000000;
  if (TTL > 0) and (TTL < dupWindow) then
    Result.DuplicateWindow := TTL
  else
    Result.DuplicateWindow := dupWindow;
end;

{ TNatsKeyValueStatus }

class function TNatsKeyValueStatus.FromStreamInfo(const ABucket: string; const AInfo: TNatsStreamInfo;
  AHistory: Integer): TNatsKeyValueStatus;
begin
  Result := Default(TNatsKeyValueStatus);
  Result.Bucket := ABucket;
  Result.StreamName := AInfo.Name;
  if Result.StreamName = '' then
    Result.StreamName := TDextNatsKeyValue.StreamNameForBucket(ABucket);
  Result.Values := AInfo.Messages;
  Result.Bytes := AInfo.Bytes;
  Result.History := AHistory;
  if Result.History <= 0 then
    Result.History := 1;
end;

{ TNatsKeyValueEntry }

function TNatsKeyValueEntry.AsString: string;
begin
  if Length(Value) = 0 then
    Result := ''
  else
    Result := TEncoding.UTF8.GetString(Value);
end;

function TNatsKeyValueEntry.IsPut: Boolean;
begin
  Result := Operation = kvoPut;
end;

{ TDextNatsKeyValue }

class function TDextNatsKeyValue.IsValidBucketChar(C: Char): Boolean;
begin
  Result := ((C >= 'a') and (C <= 'z')) or ((C >= 'A') and (C <= 'Z'))
    or ((C >= '0') and (C <= '9')) or (C = '_') or (C = '-');
end;

class function TDextNatsKeyValue.IsValidKeyChar(C: Char): Boolean;
begin
  Result := IsValidBucketChar(C) or (C = '.') or (C = '/') or (C = '=');
end;

class procedure TDextNatsKeyValue.ValidateBucketName(const ABucket: string);
var
  i: Integer;
begin
  if ABucket = '' then
    raise EDextNatsKeyValueError.Create('KV bucket name must not be empty');
  for i := 1 to Length(ABucket) do
    if not IsValidBucketChar(ABucket[i]) then
      raise EDextNatsKeyValueError.CreateFmt('Invalid KV bucket name: %s', [ABucket]);
end;

class procedure TDextNatsKeyValue.ValidateKeyName(const AKey: string);
var
  i: Integer;
begin
  if AKey = '' then
    raise EDextNatsKeyValueError.Create('KV key must not be empty');
  if (AKey[1] = '.') or (AKey[Length(AKey)] = '.') or (Pos('..', AKey) > 0) then
    raise EDextNatsKeyValueError.CreateFmt('Invalid KV key: %s', [AKey]);
  for i := 1 to Length(AKey) do
    if not IsValidKeyChar(AKey[i]) then
      raise EDextNatsKeyValueError.CreateFmt('Invalid KV key: %s', [AKey]);
end;

class function TDextNatsKeyValue.StreamNameForBucket(const ABucket: string): string;
begin
  Result := NATS_KV_STREAM_PREFIX + ABucket;
end;

class function TDextNatsKeyValue.SubjectForKey(const ABucket, AKey: string): string;
begin
  Result := Format(NATS_KV_SUBJECT_PREFIX, [ABucket]) + AKey;
end;

constructor TDextNatsKeyValue.Create(AJs: TDextNatsJetStreamContext; const ABucket: string);
begin
  inherited Create;
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('TDextNatsKeyValue requires a JetStream context');
  ValidateBucketName(ABucket);
  FJs := AJs;
  FBucket := ABucket;
  FStreamName := StreamNameForBucket(ABucket);
  FSubjectPrefix := Format(NATS_KV_SUBJECT_PREFIX, [ABucket]);
end;

class function TDextNatsKeyValue.CreateBucket(AJs: TDextNatsJetStreamContext;
  const AConfig: TNatsKeyValueConfig): TDextNatsKeyValue;
var
  streamCfg: TNatsStreamConfig;
begin
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('CreateBucket requires a JetStream context');
  streamCfg := AConfig.ToStreamConfig;
  AJs.CreateStream(streamCfg);
  Result := TDextNatsKeyValue.Create(AJs, AConfig.Bucket);
end;

class function TDextNatsKeyValue.DeleteBucket(AJs: TDextNatsJetStreamContext;
  const ABucket: string): Boolean;
begin
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('DeleteBucket requires a JetStream context');
  ValidateBucketName(ABucket);
  Result := AJs.DeleteStream(StreamNameForBucket(ABucket));
end;

class function TDextNatsKeyValue.BucketExists(AJs: TDextNatsJetStreamContext;
  const ABucket: string): Boolean;
begin
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('BucketExists requires a JetStream context');
  ValidateBucketName(ABucket);
  Result := AJs.StreamExists(StreamNameForBucket(ABucket));
end;

class function TDextNatsKeyValue.GetStatus(AJs: TDextNatsJetStreamContext;
  const ABucket: string): TNatsKeyValueStatus;
var
  info: TNatsStreamInfo;
begin
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('GetStatus requires a JetStream context');
  ValidateBucketName(ABucket);
  info := AJs.GetStreamInfo(StreamNameForBucket(ABucket));
  Result := TNatsKeyValueStatus.FromStreamInfo(ABucket, info, 1);
end;

class function TDextNatsKeyValue.Open(AJs: TDextNatsJetStreamContext;
  const ABucket: string): TDextNatsKeyValue;
begin
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('Open requires a JetStream context');
  ValidateBucketName(ABucket);
  if not AJs.StreamExists(StreamNameForBucket(ABucket)) then
    raise EDextNatsKeyValueError.CreateFmt('KV bucket not found: %s', [ABucket]);
  Result := TDextNatsKeyValue.Create(AJs, ABucket);
end;

function TDextNatsKeyValue.KeyFromSubject(const ASubject: string): string;
var
  prefixLen: Integer;
begin
  prefixLen := Length(FSubjectPrefix);
  if (prefixLen > 0) and (Length(ASubject) > prefixLen) and
    (Copy(ASubject, 1, prefixLen) = FSubjectPrefix) then
    Result := Copy(ASubject, prefixLen + 1, MaxInt)
  else
    Result := '';
end;

function TDextNatsKeyValue.EntryFromStored(const AKey: string;
  const AMsg: TNatsStoredMsg): TNatsKeyValueEntry;
var
  op: string;
begin
  Result := Default(TNatsKeyValueEntry);
  Result.Bucket := FBucket;
  Result.Key := AKey;
  Result.Value := AMsg.Data;
  Result.Revision := AMsg.Sequence;
  Result.Created := AMsg.TimeStamp;
  Result.Operation := kvoPut;
  op := AMsg.Headers.GetValue(NATS_KV_OP_HEADER);
  if SameText(op, NATS_KV_OP_DEL) then
    Result.Operation := kvoDelete
  else if SameText(op, NATS_KV_OP_PURGE) then
    Result.Operation := kvoPurge;
end;

function TDextNatsKeyValue.EntryFromJsMsg(const AMsg: TNatsJsMsg): TNatsKeyValueEntry;
var
  op: string;
begin
  Result := Default(TNatsKeyValueEntry);
  Result.Bucket := FBucket;
  Result.Key := KeyFromSubject(AMsg.Subject);
  Result.Value := AMsg.Payload;
  Result.Revision := AMsg.StreamSequence;
  if AMsg.Timestamp > 0 then
    Result.Created := IntToStr(AMsg.Timestamp)
  else
    Result.Created := '';
  Result.Operation := kvoPut;
  op := AMsg.Headers.GetValue(NATS_KV_OP_HEADER);
  if SameText(op, NATS_KV_OP_DEL) then
    Result.Operation := kvoDelete
  else if SameText(op, NATS_KV_OP_PURGE) then
    Result.Operation := kvoPurge;
end;

class function TDextNatsKeyValue.IsWrongLastSequence(const E: EDextNatsJetStreamError): Boolean;
begin
  { 10071 = stream wrong last sequence; 10164 = same on replicated (R>1) streams.
    Also match description for older servers / mismatched err_code. }
  Result := (E.ErrCode = 10071) or (E.ErrCode = 10164) or
    (Pos('wrong last sequence', LowerCase(E.Message)) > 0);
end;

function TDextNatsKeyValue.PutExpected(const AKey: string; const AValue: TBytes;
  ARevision: UInt64): UInt64;
var
  opts: TNatsJetStreamPublishOptions;
  ack: TNatsPublishAck;
begin
  ValidateKeyName(AKey);
  opts := TNatsJetStreamPublishOptions.CreateDefault;
  opts.ExpectedLastSubjectSequence := ARevision;
  opts.ExpectedLastSubjectSequenceSet := True;
  ack := FJs.Publish(SubjectForKey(FBucket, AKey), AValue, opts);
  Result := ack.Sequence;
end;

function TDextNatsKeyValue.Put(const AKey: string; const AValue: TBytes): UInt64;
var
  ack: TNatsPublishAck;
begin
  ValidateKeyName(AKey);
  ack := FJs.Publish(SubjectForKey(FBucket, AKey), AValue);
  Result := ack.Sequence;
end;

function TDextNatsKeyValue.Put(const AKey, AValue: string): UInt64;
begin
  Result := Put(AKey, TEncoding.UTF8.GetBytes(AValue));
end;

function TDextNatsKeyValue.CreateKey(const AKey: string; const AValue: TBytes): UInt64;
var
  msg: TNatsStoredMsg;
  entry: TNatsKeyValueEntry;
begin
  ValidateKeyName(AKey);
  try
    Result := PutExpected(AKey, AValue, 0);
    Exit;
  except
    on E: EDextNatsJetStreamError do
    begin
      if not IsWrongLastSequence(E) then
        raise;
      { Tombstone (DEL/PURGE) still occupies a subject sequence; Create must CAS
        against that revision — same as nats.go kvs.Create. }
      try
        msg := FJs.GetLastMessage(FStreamName, SubjectForKey(FBucket, AKey));
        entry := EntryFromStored(AKey, msg);
        if not entry.IsPut then
        begin
          Result := PutExpected(AKey, AValue, entry.Revision);
          Exit;
        end;
      except
        on EGet: EDextNatsJetStreamError do
        begin
          if not ((EGet.ErrCode = 10037) or (EGet.Code = 404)) then
            raise;
        end;
      end;
      raise EDextNatsKeyExists.CreateFmt('KV key already exists: %s.%s', [FBucket, AKey]);
    end;
  end;
end;

function TDextNatsKeyValue.Create(const AKey: string; const AValue: TBytes): UInt64;
begin
  Result := CreateKey(AKey, AValue);
end;

function TDextNatsKeyValue.Create(const AKey, AValue: string): UInt64;
begin
  Result := CreateKey(AKey, TEncoding.UTF8.GetBytes(AValue));
end;

function TDextNatsKeyValue.Update(const AKey: string; const AValue: TBytes;
  ARevision: UInt64): UInt64;
begin
  try
    Result := PutExpected(AKey, AValue, ARevision);
  except
    on E: EDextNatsJetStreamError do
    begin
      if IsWrongLastSequence(E) then
        raise EDextNatsKeyRevisionMismatch.CreateFmt(
          'KV revision mismatch for %s.%s (expected %s)',
          [FBucket, AKey, UIntToStr(ARevision)])
      else
        raise;
    end;
  end;
end;

function TDextNatsKeyValue.Update(const AKey, AValue: string; ARevision: UInt64): UInt64;
begin
  Result := Update(AKey, TEncoding.UTF8.GetBytes(AValue), ARevision);
end;

function TDextNatsKeyValue.TryGet(const AKey: string; out AEntry: TNatsKeyValueEntry): Boolean;
var
  msg: TNatsStoredMsg;
begin
  AEntry := Default(TNatsKeyValueEntry);
  ValidateKeyName(AKey);
  try
    msg := FJs.GetLastMessage(FStreamName, SubjectForKey(FBucket, AKey));
  except
    on E: EDextNatsJetStreamError do
    begin
      { 10037 = message not found; 404 = stream/message missing }
      if (E.ErrCode = 10037) or (E.Code = 404) then
        Exit(False)
      else
        raise;
    end;
  end;

  AEntry := EntryFromStored(AKey, msg);
  if not AEntry.IsPut then
  begin
    AEntry := Default(TNatsKeyValueEntry);
    Exit(False);
  end;
  Result := True;
end;

function TDextNatsKeyValue.Get(const AKey: string): TNatsKeyValueEntry;
begin
  if not TryGet(AKey, Result) then
    raise EDextNatsKeyNotFound.CreateFmt('KV key not found: %s.%s', [FBucket, AKey]);
end;

procedure TDextNatsKeyValue.PublishMarker(const AKey, AOperation: string; APurge: Boolean);
var
  headers: TNatsHeaders;
  empty: TBytes;
begin
  ValidateKeyName(AKey);
  headers := nil;
  headers.Add(NATS_KV_OP_HEADER, AOperation);
  if APurge then
    headers.Add(NATS_KV_ROLLUP_HEADER, NATS_KV_ROLLUP_SUBJECT);
  SetLength(empty, 0);
  FJs.PublishWithHeaders(SubjectForKey(FBucket, AKey), empty, headers);
end;

procedure TDextNatsKeyValue.Delete(const AKey: string);
begin
  PublishMarker(AKey, NATS_KV_OP_DEL, False);
end;

procedure TDextNatsKeyValue.Purge(const AKey: string);
begin
  PublishMarker(AKey, NATS_KV_OP_PURGE, True);
end;

function TDextNatsKeyValue.Status: TNatsKeyValueStatus;
begin
  Result := GetStatus(FJs, FBucket);
end;

function TDextNatsKeyValue.PullSubjectEntries(const AFilterSubject: string;
  ADeliver: TNatsDeliverPolicy; AIncludeDeletes: Boolean): IList<TNatsKeyValueEntry>;
const
  PULL_BATCH = 64;
var
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  consumerName: string;
  msgs: IList<TNatsJsMsg>;
  batch, i, pending: Integer;
  entry: TNatsKeyValueEntry;
begin
  Result := TCollections.CreateList<TNatsKeyValueEntry>;
  consumerName := 'kvpull_' + KvNewNuid;
  cons := TNatsConsumerConfig.CreateDefault;
  cons.Name := consumerName;
  cons.FilterSubject := AFilterSubject;
  cons.DeliverPolicy := ADeliver;
  cons.AckPolicy := apNone;
  cons.MaxDeliver := 1;
  consInfo := FJs.CreateConsumer(FStreamName, cons);
  try
    pending := Integer(consInfo.NumPending);
    if pending <= 0 then
      Exit;

    while pending > 0 do
    begin
      batch := pending;
      if batch > PULL_BATCH then
        batch := PULL_BATCH;
      msgs := FJs.Fetch(FStreamName, consInfo.Name, batch, 5000);
      if msgs.Count = 0 then
        Break;

      for i := 0 to msgs.Count - 1 do
      begin
        entry := EntryFromJsMsg(msgs[i]);
        if (entry.Key = '') then
          Continue;
        if (not AIncludeDeletes) and (not entry.IsPut) then
          Continue;
        Result.Add(entry);
      end;

      pending := msgs[msgs.Count - 1].NumPending;
      if pending < 0 then
        pending := 0;
    end;
  finally
    try
      FJs.DeleteConsumer(FStreamName, consInfo.Name);
    except
    end;
  end;
end;

function TDextNatsKeyValue.Keys: IList<string>;
var
  entries: IList<TNatsKeyValueEntry>;
  i: Integer;
begin
  Result := TCollections.CreateList<string>;
  entries := PullSubjectEntries(Format(NATS_KV_SUBJECTS, [FBucket]), dpLastPerSubject, False);
  for i := 0 to entries.Count - 1 do
    if entries[i].Key <> '' then
      Result.Add(entries[i].Key);
end;

function TDextNatsKeyValue.ListKeys: IList<string>;
begin
  Result := Keys;
end;

function TDextNatsKeyValue.History(const AKey: string): IList<TNatsKeyValueEntry>;
begin
  ValidateKeyName(AKey);
  Result := PullSubjectEntries(SubjectForKey(FBucket, AKey), dpAll, True);
end;

{ TDextNatsKeyValueWatcher }

constructor TDextNatsKeyValueWatcher.Create(AJs: TDextNatsJetStreamContext;
  const AStreamName, AConsumerName: string; APushSub: TDextNatsJetStreamPushSubscription);
begin
  inherited Create;
  if AJs = nil then
    raise EDextNatsKeyValueError.Create('TDextNatsKeyValueWatcher requires a JetStream context');
  if APushSub = nil then
    raise EDextNatsKeyValueError.Create('TDextNatsKeyValueWatcher requires a push subscription');
  FJs := AJs;
  FStreamName := AStreamName;
  FConsumerName := AConsumerName;
  FPushSub := APushSub;
  FActive := True;
end;

destructor TDextNatsKeyValueWatcher.Destroy;
begin
  Stop;
  inherited;
end;

procedure TDextNatsKeyValueWatcher.Stop;
begin
  if not FActive then
    Exit;
  FActive := False;
  if FPushSub <> nil then
  begin
    try
      FPushSub.Unsubscribe;
    except
    end;
    FreeAndNil(FPushSub);
  end;
  if (FJs <> nil) and (FStreamName <> '') and (FConsumerName <> '') then
  try
    FJs.DeleteConsumer(FStreamName, FConsumerName);
  except
  end;
end;

function TDextNatsKeyValue.StartWatch(const AFilterSubject: string;
  AHandler: TNatsKeyValueWatchHandler): TDextNatsKeyValueWatcher;
var
  deliver, consumerName: string;
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  push: TDextNatsJetStreamPushSubscription;
begin
  if not Assigned(AHandler) then
    raise EDextNatsKeyValueError.Create('Watch requires a handler');
  if AFilterSubject = '' then
    raise EDextNatsKeyValueError.Create('Watch requires a filter subject');

  deliver := FJs.Client.NewInbox;
  consumerName := 'kvwatch_' + KvNewNuid;

  { Subscribe before CONSUMER.CREATE so the deliver subject has interest. }
  push := FJs.SubscribePush(deliver,
    procedure(const AMsg: TNatsJsMsg)
    begin
      AHandler(EntryFromJsMsg(AMsg));
    end);
  try
    cons := TNatsConsumerConfig.CreateDefault;
    cons.Name := consumerName;
    cons.FilterSubject := AFilterSubject;
    cons.DeliverSubject := deliver;
    cons.DeliverPolicy := dpLastPerSubject;
    cons.AckPolicy := apNone;
    { ack_policy=none rejects a positive max_ack_pending (JS API 10082). }
    cons.MaxAckPending := 0;
    consInfo := FJs.CreateConsumer(FStreamName, cons);
  except
    push.Free;
    raise;
  end;

  Result := TDextNatsKeyValueWatcher.Create(FJs, FStreamName, consInfo.Name, push);
end;

function TDextNatsKeyValue.Watch(const AKey: string;
  AHandler: TNatsKeyValueWatchHandler): TDextNatsKeyValueWatcher;
begin
  ValidateKeyName(AKey);
  Result := StartWatch(SubjectForKey(FBucket, AKey), AHandler);
end;

function TDextNatsKeyValue.WatchAll(AHandler: TNatsKeyValueWatchHandler): TDextNatsKeyValueWatcher;
begin
  Result := StartWatch(Format(NATS_KV_SUBJECTS, [FBucket]), AHandler);
end;

end.
