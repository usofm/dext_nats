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
{                                                                           }
{  JetStream Object Store (ADR-20). Buckets are OBJ_<bucket> streams with   }
{  chunk subjects $O.<bucket>.C.> and metadata $O.<bucket>.M.>.             }
{  CreateStore / DeleteStore / Put / Get / Delete / List / Keys,            }
{  Watch / WatchAll (EndOfInitial marker + MetaOnly / UpdatesOnly),         }
{  UpdateMeta, Seal, AddLink / AddBucketLink.                               }
{  Streaming Put/Get: TStream + PutFile/GetFile (chunked, no full TBytes).  }
{  Get follows object links (same or other bucket); bucket links raise.     }
{  TDextNatsObjectStoreContext wraps a TDextNatsJetStreamContext (or        }
{  creates one from TDextNatsClient); it does not own the client.           }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.ObjectStore;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Collections,
  Dext.Collections.Dict,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

const
  /// <summary>Default Object Store chunk size (128 KiB), per ADR-20.</summary>
  NATS_OBJ_DEFAULT_CHUNK_SIZE = 128 * 1024;
  /// <summary>NATS subject-level rollup header value for Object Store metadata.</summary>
  NATS_OBJ_ROLLUP_SUBJECT = 'sub';

type
  /// <summary>Raised when an Object Store object is missing or soft-deleted.</summary>
  EDextNatsObjectStoreError = class(EDextNatsException);

  /// <summary>Configuration used to create an Object Store bucket (OBJ_ stream).</summary>
  TNatsObjectStoreConfig = record
    /// <summary>Bucket name (restricted-term: A-Z a-z 0-9 _ -). Becomes stream OBJ_&lt;Bucket&gt;.</summary>
    Bucket: string;
    Description: string;
    /// <summary>Stream max_bytes; 0 or negative = unlimited (-1).</summary>
    MaxBytes: Int64;
    /// <summary>Stream max_age in nanoseconds; 0 = unlimited.</summary>
    MaxAge: Int64;
    Storage: TNatsStreamStorage;
    NumReplicas: Integer;
    /// <summary>Chunk size for Put; 0 = <see cref="NATS_OBJ_DEFAULT_CHUNK_SIZE"/>.</summary>
    ChunkSize: Integer;
    /// <summary>Sensible defaults: file storage, 1 replica, 128 KiB chunks.</summary>
    class function CreateDefault(const ABucket: string): TNatsObjectStoreConfig; static;
  end;

  /// <summary>
  ///   Mutable Object Store metadata fields for <see cref="TDextNatsObjectStore.UpdateMeta"/>.
  ///   Chunk size and link options are not updated (ADR-20 / nats.go).
  /// </summary>
  TNatsObjectMeta = record
    Name: string;
    Description: string;
    Headers: TNatsHeaders;
    Metadata: IDictionary<string, string>;
    /// <summary>Name-only meta; description/headers/metadata empty.</summary>
    class function Create(const AName: string): TNatsObjectMeta; static;
  end;

  /// <summary>
  ///   Embedded link target in ObjectInfo <c>options.link</c> (ADR-20 / nats.go).
  ///   Empty <see cref="Name"/> means a bucket link (whole store), not a gettable object.
  /// </summary>
  TNatsObjectLink = record
    Bucket: string;
    Name: string;
    class function Create(const ABucket, AName: string): TNatsObjectLink; static;
    class function CreateBucket(const ABucket: string): TNatsObjectLink; static;
  end;

  /// <summary>Object metadata + instance fields stored on $O.&lt;bucket&gt;.M.&lt;b64url(name)&gt;.</summary>
  TNatsObjectInfo = record
    Name: string;
    Description: string;
    Headers: TNatsHeaders;
    Metadata: IDictionary<string, string>;
    Bucket: string;
    Nuid: string;
    Size: UInt64;
    Chunks: Cardinal;
    Digest: string;
    Deleted: Boolean;
    ChunkSize: Integer;
    /// <summary>When set (<see cref="IsLink"/>), meta is a link; see <c>options.link</c>.</summary>
    Link: TNatsObjectLink;
    /// <summary>
    ///   True for the synthetic end-of-initial-values marker (nats.go / KV Watch equivalent).
    ///   Name is empty; this is not a close signal — live updates continue after it.
    /// </summary>
    EndOfInitial: Boolean;
    /// <summary>True when <c>options.link</c> is present (object or bucket link).</summary>
    function IsLink: Boolean;
    /// <summary>True when this is a bucket link (<see cref="IsLink"/> and empty link name).</summary>
    function IsBucketLink: Boolean;
    /// <summary>True when this info is the end-of-initial snapshot marker.</summary>
    function IsEndOfInitial: Boolean;
    /// <summary>Builds the end-of-initial marker (empty name).</summary>
    class function EndOfInitialMarker: TNatsObjectInfo; static;
    /// <summary>Parses ObjectInfo JSON (mtime is ignored / never stored).</summary>
    class function Parse(const AJson: string): TNatsObjectInfo; static;
    /// <summary>Serializes ObjectInfo for metadata publish (omits mtime).</summary>
    function ToJson: string;
  end;

  /// <summary>Callback for <see cref="TDextNatsObjectStore.Watch"/> / WatchAll deliveries.</summary>
  TNatsObjectStoreWatchHandler = reference to procedure(const AInfo: TNatsObjectInfo);

  /// <summary>Options for <see cref="TDextNatsObjectStore.Watch"/> / WatchAll.</summary>
  TNatsObjectStoreWatchOptions = record
    /// <summary>
    ///   Headers-only consumer: object name (from meta subject) without ObjectInfo JSON
    ///   payload. Maps to JetStream <c>headers_only</c>. Watch already targets meta
    ///   subjects — use this to avoid transferring meta JSON bodies.
    /// </summary>
    MetaOnly: Boolean;
    /// <summary>
    ///   Skip the last-per-subject snapshot; deliver only new updates
    ///   (<c>deliver_policy=new</c>). No EndOfInitial marker is sent (nats.go / KV semantics).
    /// </summary>
    UpdatesOnly: Boolean;
    /// <summary>Defaults: MetaOnly=False, UpdatesOnly=False (snapshot + updates + marker).</summary>
    class function CreateDefault: TNatsObjectStoreWatchOptions; static;
  end;

  /// <summary>
  ///   Active Object Store watch (ephemeral push consumer + deliver-subject SUB).
  ///   Does not own the JetStream context. Call <see cref="Stop"/> or Free before
  ///   freeing the Object Store. Handlers run on the client's receive thread —
  ///   do not block with Request/Fetch. The EndOfInitial marker may also be
  ///   delivered on the Watch caller thread when the snapshot is already empty.
  /// </summary>
  TDextNatsObjectStoreWatcher = class
  private
    FJs: TDextNatsJetStreamContext;
    FStreamName: string;
    FConsumerName: string;
    FPushSub: TDextNatsJetStreamPushSubscription;
    FActive: Boolean;
    FGate: TObject; { TNatsOsWatchGate }
    function GetInitialDone: Boolean;
  public
    constructor Create(AJs: TDextNatsJetStreamContext; const AStreamName, AConsumerName: string;
      APushSub: TDextNatsJetStreamPushSubscription; AGate: TObject);
    destructor Destroy; override;
    /// <summary>Unsubscribes the push SUB and deletes the ephemeral consumer. Idempotent.</summary>
    procedure Stop;
    property Active: Boolean read FActive;
    property ConsumerName: string read FConsumerName;
    /// <summary>True after the end-of-initial marker has been delivered (or UpdatesOnly watch).</summary>
    property InitialDone: Boolean read GetInitialDone;
  end;

  TDextNatsObjectStoreContext = class;

  /// <summary>
  ///   Bound Object Store bucket. Does not own the JetStream context or client.
  ///   Put / Get / Delete / List / Watch follow NATS Object Store conventions (ADR-20).
  /// </summary>
  TDextNatsObjectStore = class
  private
    FContext: TDextNatsObjectStoreContext;
    FBucket: string;
    FStreamName: string;
    FChunkSize: Integer;
    function MetaSubject(const AObjectName: string): string;
    function MetaWildcardSubject: string;
    function ChunkSubject(const ANuid: string): string;
    function TryGetInfo(const AObjectName: string; out AInfo: TNatsObjectInfo): Boolean;
    procedure PublishMeta(const AInfo: TNatsObjectInfo);
    procedure PurgeSubject(const ASubject: string);
    /// <summary>
    ///   Pulls chunks in modest batches and writes each payload to AStream while
    ///   hashing (avoids assembling one giant TBytes).
    /// </summary>
    procedure WriteChunksToStream(const ANuid: string; AChunkCount: Cardinal;
      AStream: TStream; out AWritten: UInt64; out ADigest: string);
    function PutFromStream(const AName: string; AStream: TStream;
      const ADescription: string; const AHeaders: TNatsHeaders;
      const AMetadata: IDictionary<string, string>): TNatsObjectInfo;
    function InfoFromJsMsg(const AMsg: TNatsJsMsg): TNatsObjectInfo;
    function NameFromMetaSubject(const ASubject: string): string;
    function StartWatch(const AFilterSubject: string;
      AHandler: TNatsObjectStoreWatchHandler;
      const AOptions: TNatsObjectStoreWatchOptions): TDextNatsObjectStoreWatcher;
  public
    constructor Create(AContext: TDextNatsObjectStoreContext; const ABucket: string;
      AChunkSize: Integer = 0);

    /// <summary>Stores AData under AName (chunked). Overwrites prior object of the same name.</summary>
    function Put(const AName: string; const AData: TBytes): TNatsObjectInfo; overload;
    /// <summary>
    ///   Streams AStream to EOF under AName in store chunk-size pieces (SHA-256 +
    ///   meta rollup). Does not require the whole payload in one TBytes.
    ///   AStream position advances to EOF.
    /// </summary>
    function Put(const AName: string; AStream: TStream): TNatsObjectInfo; overload;
    /// <summary>
    ///   Like Put(name, stream) with Name / Description / Headers / Metadata from
    ///   AMeta (nats.go <c>Put(ObjectMeta, reader)</c>).
    /// </summary>
    function Put(const AMeta: TNatsObjectMeta; AStream: TStream): TNatsObjectInfo; overload;
    /// <summary>Opens AFileName and Puts under AName (chunked from the file stream).</summary>
    function PutFile(const AName, AFileName: string): TNatsObjectInfo; overload;
    /// <summary>Puts AFileName using <c>ExtractFileName</c> as the object name.</summary>
    function PutFile(const AFileName: string): TNatsObjectInfo; overload;
    /// <summary>
    ///   Reassembles chunks for AName. Object links are followed (same or other
    ///   bucket via OpenStore) like nats.go; the returned / out info is the
    ///   resolved target. Bucket links raise (cannot get a whole bucket).
    ///   Raises if missing, deleted, or digest mismatch.
    /// </summary>
    function Get(const AName: string): TBytes; overload;
    /// <summary>Same as Get; AInfo is the resolved target metadata after link follow.</summary>
    function Get(const AName: string; out AInfo: TNatsObjectInfo): TBytes; overload;
    /// <summary>
    ///   Writes object bytes into AStream (batched chunk fetch + digest verify).
    ///   Object links are followed like Get(TBytes). Returns resolved target info.
    /// </summary>
    function Get(const AName: string; AStream: TStream): TNatsObjectInfo; overload;
    /// <summary>
    ///   Downloads AName into AFileName (creates or overwrites). Returns resolved
    ///   target info after link follow.
    /// </summary>
    function GetFile(const AName, AFileName: string): TNatsObjectInfo;
    /// <summary>
    ///   Current metadata for a live (non-deleted) object. Does not follow links —
    ///   link entries surface their own meta (<see cref="TNatsObjectInfo.Link"/>).
    /// </summary>
    function GetInfo(const AName: string): TNatsObjectInfo;
    /// <summary>
    ///   Creates (or replaces an existing link named AName with) an object link to
    ///   ATarget. Meta JSON: <c>options.link = {bucket, name}</c>. Target must not
    ///   be deleted or itself a link. A non-link object at AName raises.
    /// </summary>
    function AddLink(const AName: string; const ATarget: TNatsObjectInfo): TNatsObjectInfo;
    /// <summary>
    ///   Creates (or replaces an existing link named AName with) a bucket link to
    ///   AStore. Meta JSON: <c>options.link = {bucket}</c> (empty name). Get on a
    ///   bucket link raises; read GetInfo to learn the target bucket.
    /// </summary>
    function AddBucketLink(const AName: string; AStore: TDextNatsObjectStore): TNatsObjectInfo;
    /// <summary>Soft-deletes AName (rollup tombstone) and purges its chunk subject.</summary>
    procedure Delete(const AName: string);
    /// <summary>
    ///   Updates name, description, headers, and metadata for a live object without
    ///   rewriting chunk payload. Chunk size / links are left unchanged.
    ///   Renaming publishes under the new meta subject and purges the old one.
    ///   Raises if the object is missing/deleted, AMeta.Name is empty, or the new
    ///   name is already held by a non-deleted object.
    /// </summary>
    function UpdateMeta(const AName: string; const AMeta: TNatsObjectMeta): TNatsObjectInfo;
    /// <summary>
    ///   Seals the underlying OBJ_ stream so further Put / Delete / UpdateMeta fail
    ///   (NATS <c>sealed</c> stream config). Idempotent when already sealed.
    /// </summary>
    procedure Seal;
    /// <summary>True when the underlying OBJ_ stream reports <c>config.sealed</c>.</summary>
    function IsSealed: Boolean;

    /// <summary>
    ///   Snapshot of object metadata from <c>$O.&lt;bucket&gt;.M.&gt;</c>
    ///   (ephemeral pull, <c>deliver_policy=last_per_subject</c>).
    ///   Soft-deleted objects are omitted unless AIncludeDeleted is True.
    ///   Empty bucket returns an empty list (not an error).
    /// </summary>
    function List(AIncludeDeleted: Boolean = False): IList<TNatsObjectInfo>;
    /// <summary>Alias of <see cref="List"/> (ADR-20 List / ListObjects naming).</summary>
    function ListObjects(AIncludeDeleted: Boolean = False): IList<TNatsObjectInfo>;
    /// <summary>Live (non-deleted) object names from <see cref="List"/>.</summary>
    function Keys: IList<string>;
    /// <summary>
    ///   Watches one object: delivers the current last-per-subject meta (if any), then
    ///   an <see cref="TNatsObjectInfo.EndOfInitial"/> marker, then live updates.
    ///   Caller must Free the watcher (or Stop) before freeing this store.
    /// </summary>
    function Watch(const AName: string; AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher; overload;
    /// <summary>Watch one object with <see cref="TNatsObjectStoreWatchOptions"/>.</summary>
    function Watch(const AName: string; AHandler: TNatsObjectStoreWatchHandler;
      const AOptions: TNatsObjectStoreWatchOptions): TDextNatsObjectStoreWatcher; overload;
    /// <summary>
    ///   Watches the whole bucket meta subjects (<c>$O.&lt;bucket&gt;.M.&gt;</c>):
    ///   last-per-subject snapshot + EndOfInitial marker + subsequent updates.
    ///   Handlers run on the receive thread (marker may fire on the caller thread
    ///   when the snapshot is empty).
    /// </summary>
    function WatchAll(AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher; overload;
    /// <summary>WatchAll with <see cref="TNatsObjectStoreWatchOptions"/>.</summary>
    function WatchAll(AHandler: TNatsObjectStoreWatchHandler;
      const AOptions: TNatsObjectStoreWatchOptions): TDextNatsObjectStoreWatcher; overload;

    property Bucket: string read FBucket;
    property StreamName: string read FStreamName;
    property ChunkSize: Integer read FChunkSize;
  end;

  /// <summary>
  ///   Object Store manager: create / open / delete buckets.
  ///   Composes over <see cref="TDextNatsJetStreamContext"/>; does not own the NATS client.
  ///   When constructed from a client, owns the JetStream wrapper it creates.
  /// </summary>
  TDextNatsObjectStoreContext = class
  private
    FJs: TDextNatsJetStreamContext;
    FOwnsJs: Boolean;
    class procedure ValidateBucketName(const ABucket: string); static;
  public
    /// <summary>Wraps AClient with a new JetStream context (owned by this instance).</summary>
    constructor Create(AClient: TDextNatsClient); overload;
    /// <summary>Uses an existing JetStream context (not owned).</summary>
    constructor Create(AJs: TDextNatsJetStreamContext); overload;
    destructor Destroy; override;

    /// <summary>Creates OBJ_&lt;bucket&gt; with $O.&lt;bucket&gt;.C.&gt; and .M.&gt; subjects.</summary>
    function CreateStore(const AConfig: TNatsObjectStoreConfig): TDextNatsObjectStore;
    /// <summary>Binds to an existing Object Store bucket (stream must exist).</summary>
    function OpenStore(const ABucket: string): TDextNatsObjectStore;
    /// <summary>Deletes the underlying OBJ_&lt;bucket&gt; stream.</summary>
    procedure DeleteStore(const ABucket: string);

    /// <summary>Underlying JetStream context (lifetime depends on which constructor was used).</summary>
    property JetStream: TDextNatsJetStreamContext read FJs;
  end;

implementation

uses
  System.Hash,
  System.NetEncoding,
  Dext.Core.Span,
  Dext.Json.Utf8;

type
  PObjByteWriter = ^TObjByteWriter;
  TObjByteWriter = record
  private
    FBuf: TBytes;
    FLen: Integer;
  public
    procedure Reset;
    procedure EnsureCapacity(ANeeded: Integer);
    procedure WriteBytes(AData: Pointer; ALength: Integer);
    function ToBytes: TBytes;
  end;

procedure ObjUtf8WriteToByteWriter(AContext, AData: Pointer; ALength: Integer);
begin
  if (ALength > 0) and (AContext <> nil) then
    PObjByteWriter(AContext)^.WriteBytes(AData, ALength);
end;

procedure TObjByteWriter.Reset;
begin
  FLen := 0;
end;

procedure TObjByteWriter.EnsureCapacity(ANeeded: Integer);
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

procedure TObjByteWriter.WriteBytes(AData: Pointer; ALength: Integer);
begin
  if (ALength <= 0) or (AData = nil) then
    Exit;
  EnsureCapacity(ALength);
  Move(AData^, FBuf[FLen], ALength);
  Inc(FLen, ALength);
end;

function TObjByteWriter.ToBytes: TBytes;
begin
  SetLength(Result, FLen);
  if FLen > 0 then
    Move(FBuf[0], Result[0], FLen);
end;

procedure ObjSkipValue(var AReader: TUtf8JsonReader);
begin
  if AReader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
    AReader.Skip;
end;

procedure ObjRaiseFromErrorObject(var AReader: TUtf8JsonReader);
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
        ObjSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('err_code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        errCode := AReader.GetInt32
      else
        ObjSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('description') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        description := AReader.GetString
      else
        ObjSkipValue(AReader);
    end
    else if AReader.Read then
      ObjSkipValue(AReader);
  end;
  raise EDextNatsJetStreamError.CreateFromApi(code, errCode, description);
end;

function ObjOpenReader(const AJson, AEmptyMsg: string; out ABytes: TBytes): TUtf8JsonReader;
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

procedure ObjParseSuccessResponse(const AJson: string);
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
  ok: Boolean;
begin
  ok := False;
  try
    reader := ObjOpenReader(AJson, 'Empty JetStream API response', bytes);
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
            ObjRaiseFromErrorObject(reader)
          else
            ObjSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('success') then
      begin
        if reader.Read then
        begin
          if reader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
            ok := reader.GetBoolean
          else
            ObjSkipValue(reader);
        end;
      end
      else if reader.Read then
        ObjSkipValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);
  end;
  if not ok then
    raise EDextNatsJetStreamError.CreateFromApi(0, 0, 'STREAM.PURGE did not report success');
end;

function ObjStreamName(const ABucket: string): string;
begin
  Result := 'OBJ_' + ABucket;
end;

function ObjEncodeName(const AName: string): string;
begin
  Result := TNetEncoding.Base64URL.EncodeBytesToString(TEncoding.UTF8.GetBytes(AName));
end;

function ObjDecodeName(const AEncoded: string): string;
var
  bytes: TBytes;
begin
  Result := '';
  if AEncoded = '' then
    Exit;
  try
    bytes := TNetEncoding.Base64URL.DecodeStringToBytes(AEncoded);
    if Length(bytes) > 0 then
      Result := TEncoding.UTF8.GetString(bytes);
  except
    Result := '';
  end;
end;

function ObjNewNuid: string;
var
  guid: TGUID;
  s: string;
begin
  CreateGUID(guid);
  s := GUIDToString(guid);
  s := StringReplace(s, '{', '', [rfReplaceAll]);
  s := StringReplace(s, '}', '', [rfReplaceAll]);
  s := StringReplace(s, '-', '', [rfReplaceAll]);
  Result := Copy(s, 1, 22);
end;

function ObjDigestValue(const AHashBytes: TBytes): string;
begin
  Result := 'SHA-256=' + TNetEncoding.Base64URL.EncodeBytesToString(AHashBytes);
end;

function ObjIsValidBucketChar(C: Char): Boolean;
begin
  Result := ((C >= 'A') and (C <= 'Z')) or
            ((C >= 'a') and (C <= 'z')) or
            ((C >= '0') and (C <= '9')) or
            (C = '_') or (C = '-');
end;

{ TNatsObjectStoreConfig }

class function TNatsObjectStoreConfig.CreateDefault(const ABucket: string): TNatsObjectStoreConfig;
begin
  Result := Default(TNatsObjectStoreConfig);
  Result.Bucket := ABucket;
  Result.MaxBytes := -1;
  Result.MaxAge := 0;
  Result.Storage := ssFile;
  Result.NumReplicas := 1;
  Result.ChunkSize := NATS_OBJ_DEFAULT_CHUNK_SIZE;
end;

{ TNatsObjectMeta }

class function TNatsObjectMeta.Create(const AName: string): TNatsObjectMeta;
begin
  Result := Default(TNatsObjectMeta);
  Result.Name := AName;
  Result.Headers := nil;
  Result.Metadata := nil;
end;

{ TNatsObjectLink }

class function TNatsObjectLink.Create(const ABucket, AName: string): TNatsObjectLink;
begin
  Result.Bucket := ABucket;
  Result.Name := AName;
end;

class function TNatsObjectLink.CreateBucket(const ABucket: string): TNatsObjectLink;
begin
  Result.Bucket := ABucket;
  Result.Name := '';
end;

function TNatsObjectInfo.IsLink: Boolean;
begin
  Result := (not EndOfInitial) and (Link.Bucket <> '');
end;

function TNatsObjectInfo.IsBucketLink: Boolean;
begin
  Result := IsLink and (Link.Name = '');
end;

function TNatsObjectInfo.IsEndOfInitial: Boolean;
begin
  Result := EndOfInitial;
end;

class function TNatsObjectInfo.EndOfInitialMarker: TNatsObjectInfo;
begin
  Result := Default(TNatsObjectInfo);
  Result.EndOfInitial := True;
end;

{ TNatsObjectStoreWatchOptions }

class function TNatsObjectStoreWatchOptions.CreateDefault: TNatsObjectStoreWatchOptions;
begin
  Result := Default(TNatsObjectStoreWatchOptions);
end;

procedure ObjParseLinkObject(var AReader: TUtf8JsonReader; out ALink: TNatsObjectLink);
begin
  ALink := Default(TNatsObjectLink);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    if AReader.ValueSpanEquals('bucket') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ALink.Bucket := AReader.GetString
      else
        ObjSkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('name') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ALink.Name := AReader.GetString
      else
        ObjSkipValue(AReader);
    end
    else if AReader.Read then
      ObjSkipValue(AReader);
  end;
end;

procedure ObjParseHeadersObject(var AReader: TUtf8JsonReader; out AHeaders: TNatsHeaders);
var
  key: string;
begin
  AHeaders := nil;
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    key := AReader.GetString;
    if not AReader.Read then
      Break;
    if AReader.TokenType = TJsonTokenType.StartArray then
    begin
      while AReader.Read do
      begin
        if AReader.TokenType = TJsonTokenType.EndArray then
          Break;
        if AReader.TokenType = TJsonTokenType.StringValue then
          AHeaders.Add(key, AReader.GetString)
        else
          ObjSkipValue(AReader);
      end;
    end
    else if AReader.TokenType = TJsonTokenType.StringValue then
      AHeaders.Add(key, AReader.GetString)
    else
      ObjSkipValue(AReader);
  end;
end;

procedure ObjParseMetadataObject(var AReader: TUtf8JsonReader; out AMetadata: IDictionary<string, string>);
var
  key: string;
begin
  AMetadata := TCollections.CreateDictionary<string, string>;
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    key := AReader.GetString;
    if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      AMetadata.AddOrSetValue(key, AReader.GetString)
    else
      ObjSkipValue(AReader);
  end;
end;

procedure ObjWriteHeaders(var AWriter: TUtf8JsonWriter; const AHeaders: TNatsHeaders);
var
  i, j: Integer;
  written: TArray<Boolean>;
  key: string;
begin
  if Length(AHeaders) = 0 then
    Exit;
  AWriter.WritePropertyName('headers');
  AWriter.WriteStartObject;
  SetLength(written, Length(AHeaders));
  for i := 0 to High(AHeaders) do
  begin
    if written[i] then
      Continue;
    key := AHeaders[i].Key;
    AWriter.WritePropertyName(key);
    AWriter.WriteStartArray;
    for j := i to High(AHeaders) do
      if (not written[j]) and (AHeaders[j].Key = key) then
      begin
        AWriter.WriteString(AHeaders[j].Value);
        written[j] := True;
      end;
    AWriter.WriteEndArray;
  end;
  AWriter.WriteEndObject;
end;

procedure ObjWriteMetadata(var AWriter: TUtf8JsonWriter; const AMetadata: IDictionary<string, string>);
var
  keys: TArray<string>;
  i: Integer;
begin
  if (AMetadata = nil) or (AMetadata.Count = 0) then
    Exit;
  AWriter.WritePropertyName('metadata');
  AWriter.WriteStartObject;
  keys := AMetadata.Keys;
  for i := 0 to High(keys) do
  begin
    AWriter.WritePropertyName(keys[i]);
    AWriter.WriteString(AMetadata[keys[i]]);
  end;
  AWriter.WriteEndObject;
end;

{ TNatsObjectInfo }

class function TNatsObjectInfo.Parse(const AJson: string): TNatsObjectInfo;
var
  bytes: TBytes;
  reader: TUtf8JsonReader;
begin
  Result := Default(TNatsObjectInfo);
  Result.Headers := nil;
  Result.Metadata := nil;
  try
    reader := ObjOpenReader(AJson, 'Empty ObjectInfo JSON', bytes);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed ObjectInfo JSON: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('name') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Name := reader.GetString
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('description') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Description := reader.GetString
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('headers') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            ObjParseHeadersObject(reader, Result.Headers)
          else
            ObjSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('metadata') then
      begin
        if reader.Read then
        begin
          if reader.TokenType = TJsonTokenType.StartObject then
            ObjParseMetadataObject(reader, Result.Metadata)
          else
            ObjSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('bucket') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Bucket := reader.GetString
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('nuid') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Nuid := reader.GetString
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('size') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          Result.Size := UInt64(reader.GetInt64)
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('chunks') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
          Result.Chunks := Cardinal(reader.GetInt64)
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('digest') then
      begin
        if reader.Read and (reader.TokenType = TJsonTokenType.StringValue) then
          Result.Digest := reader.GetString
        else
          ObjSkipValue(reader);
      end
      else if reader.ValueSpanEquals('deleted') then
      begin
        if reader.Read then
        begin
          if reader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue] then
            Result.Deleted := reader.GetBoolean
          else
            ObjSkipValue(reader);
        end;
      end
      else if reader.ValueSpanEquals('options') then
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
              if reader.ValueSpanEquals('max_chunk_size') then
              begin
                if reader.Read and (reader.TokenType = TJsonTokenType.Number) then
                  Result.ChunkSize := reader.GetInt32
                else
                  ObjSkipValue(reader);
              end
              else if reader.ValueSpanEquals('link') then
              begin
                if reader.Read then
                begin
                  if reader.TokenType = TJsonTokenType.StartObject then
                    ObjParseLinkObject(reader, Result.Link)
                  else
                    ObjSkipValue(reader);
                end;
              end
              else if reader.Read then
                ObjSkipValue(reader);
            end;
          end
          else
            ObjSkipValue(reader);
        end;
      end
      else if reader.Read then
        ObjSkipValue(reader);
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EDextNatsJetStreamError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt('Malformed ObjectInfo JSON: %s', [AJson]);
  end;
end;

function TNatsObjectInfo.ToJson: string;
var
  w: TObjByteWriter;
  jw: TUtf8JsonWriter;
begin
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, ObjUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('name');
  jw.WriteString(Name);
  if Description <> '' then
  begin
    jw.WritePropertyName('description');
    jw.WriteString(Description);
  end;
  ObjWriteHeaders(jw, Headers);
  ObjWriteMetadata(jw, Metadata);
  if (ChunkSize > 0) or IsLink then
  begin
    jw.WritePropertyName('options');
    jw.WriteStartObject;
    if IsLink then
    begin
      jw.WritePropertyName('link');
      jw.WriteStartObject;
      jw.WritePropertyName('bucket');
      jw.WriteString(Link.Bucket);
      if Link.Name <> '' then
      begin
        jw.WritePropertyName('name');
        jw.WriteString(Link.Name);
      end;
      jw.WriteEndObject;
    end;
    if ChunkSize > 0 then
    begin
      jw.WritePropertyName('max_chunk_size');
      jw.WriteNumber(ChunkSize);
    end;
    jw.WriteEndObject;
  end;
  jw.WritePropertyName('bucket');
  jw.WriteString(Bucket);
  jw.WritePropertyName('nuid');
  jw.WriteString(Nuid);
  jw.WritePropertyName('size');
  jw.WriteNumber(Int64(Size));
  jw.WritePropertyName('chunks');
  jw.WriteNumber(Int64(Chunks));
  if Digest <> '' then
  begin
    jw.WritePropertyName('digest');
    jw.WriteString(Digest);
  end;
  if Deleted then
  begin
    jw.WritePropertyName('deleted');
    jw.WriteBoolean(True);
  end;
  jw.WriteEndObject;
  Result := TEncoding.UTF8.GetString(w.ToBytes);
end;

{ TDextNatsObjectStoreContext }

constructor TDextNatsObjectStoreContext.Create(AClient: TDextNatsClient);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('ObjectStore requires a NATS client');
  FJs := TDextNatsJetStreamContext.Create(AClient);
  FOwnsJs := True;
end;

constructor TDextNatsObjectStoreContext.Create(AJs: TDextNatsJetStreamContext);
begin
  inherited Create;
  if AJs = nil then
    raise EDextNatsException.Create('ObjectStore requires a JetStream context');
  FJs := AJs;
  FOwnsJs := False;
end;

destructor TDextNatsObjectStoreContext.Destroy;
begin
  if FOwnsJs then
    FreeAndNil(FJs);
  inherited;
end;

class procedure TDextNatsObjectStoreContext.ValidateBucketName(const ABucket: string);
var
  i: Integer;
begin
  if ABucket = '' then
    raise EDextNatsObjectStoreError.Create('Object Store bucket name is required');
  for i := 1 to Length(ABucket) do
    if not ObjIsValidBucketChar(ABucket[i]) then
      raise EDextNatsObjectStoreError.CreateFmt('Invalid Object Store bucket name: %s', [ABucket]);
end;

function TDextNatsObjectStoreContext.CreateStore(
  const AConfig: TNatsObjectStoreConfig): TDextNatsObjectStore;
var
  cfg: TNatsStreamConfig;
  chunkSize: Integer;
  maxBytes: Int64;
  replicas: Integer;
begin
  ValidateBucketName(AConfig.Bucket);
  chunkSize := AConfig.ChunkSize;
  if chunkSize <= 0 then
    chunkSize := NATS_OBJ_DEFAULT_CHUNK_SIZE;
  maxBytes := AConfig.MaxBytes;
  if maxBytes = 0 then
    maxBytes := -1;
  replicas := AConfig.NumReplicas;
  if replicas <= 0 then
    replicas := 1;

  cfg := TNatsStreamConfig.CreateDefault(ObjStreamName(AConfig.Bucket),
    [Format('$O.%s.C.>', [AConfig.Bucket]), Format('$O.%s.M.>', [AConfig.Bucket])]);
  cfg.Discard := sdNew;
  cfg.AllowRollup := True;
  cfg.AllowDirect := True;
  cfg.MaxBytes := maxBytes;
  cfg.MaxAge := AConfig.MaxAge;
  cfg.Storage := AConfig.Storage;
  cfg.NumReplicas := replicas;
  FJs.CreateStream(cfg);
  Result := TDextNatsObjectStore.Create(Self, AConfig.Bucket, chunkSize);
end;

function TDextNatsObjectStoreContext.OpenStore(const ABucket: string): TDextNatsObjectStore;
begin
  ValidateBucketName(ABucket);
  FJs.GetStreamInfo(ObjStreamName(ABucket));
  Result := TDextNatsObjectStore.Create(Self, ABucket, NATS_OBJ_DEFAULT_CHUNK_SIZE);
end;

procedure TDextNatsObjectStoreContext.DeleteStore(const ABucket: string);
begin
  ValidateBucketName(ABucket);
  FJs.DeleteStream(ObjStreamName(ABucket));
end;

{ TDextNatsObjectStore }

constructor TDextNatsObjectStore.Create(AContext: TDextNatsObjectStoreContext;
  const ABucket: string; AChunkSize: Integer);
begin
  inherited Create;
  if AContext = nil then
    raise EDextNatsException.Create('ObjectStore requires an ObjectStore context');
  FContext := AContext;
  FBucket := ABucket;
  FStreamName := ObjStreamName(ABucket);
  if AChunkSize > 0 then
    FChunkSize := AChunkSize
  else
    FChunkSize := NATS_OBJ_DEFAULT_CHUNK_SIZE;
end;

function TDextNatsObjectStore.MetaSubject(const AObjectName: string): string;
begin
  Result := Format('$O.%s.M.%s', [FBucket, ObjEncodeName(AObjectName)]);
end;

function TDextNatsObjectStore.MetaWildcardSubject: string;
begin
  Result := Format('$O.%s.M.>', [FBucket]);
end;

function TDextNatsObjectStore.ChunkSubject(const ANuid: string): string;
begin
  Result := Format('$O.%s.C.%s', [FBucket, ANuid]);
end;

function TDextNatsObjectStore.TryGetInfo(const AObjectName: string;
  out AInfo: TNatsObjectInfo): Boolean;
var
  msg: TNatsStoredMsg;
  json: string;
begin
  Result := False;
  AInfo := Default(TNatsObjectInfo);
  if AObjectName = '' then
    Exit;
  try
    msg := FContext.FJs.GetLastMessage(FStreamName, MetaSubject(AObjectName));
  except
    on E: EDextNatsJetStreamError do
    begin
      if (E.Code = 404) or (E.ErrCode = 10037) or (E.ErrCode = 10059) then
        Exit
      else
        raise;
    end;
  end;
  json := TEncoding.UTF8.GetString(msg.Data);
  AInfo := TNatsObjectInfo.Parse(json);
  Result := True;
end;

procedure TDextNatsObjectStore.PublishMeta(const AInfo: TNatsObjectInfo);
var
  headers: TNatsHeaders;
  payload: TBytes;
begin
  headers := nil;
  headers.Add('Nats-Rollup', NATS_OBJ_ROLLUP_SUBJECT);
  payload := TEncoding.UTF8.GetBytes(AInfo.ToJson);
  FContext.FJs.PublishWithHeaders(MetaSubject(AInfo.Name), payload, headers);
end;

procedure TDextNatsObjectStore.PurgeSubject(const ASubject: string);
var
  w: TObjByteWriter;
  jw: TUtf8JsonWriter;
  body, reply: string;
begin
  if ASubject = '' then
    Exit;
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, ObjUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  jw.WritePropertyName('filter');
  jw.WriteString(ASubject);
  jw.WriteEndObject;
  body := TEncoding.UTF8.GetString(w.ToBytes);
  reply := FContext.FJs.Client.Request(
    FContext.FJs.ApiPrefix + 'STREAM.PURGE.' + FStreamName,
    TEncoding.UTF8.GetBytes(body)).AsString;
  ObjParseSuccessResponse(reply);
end;

procedure TDextNatsObjectStore.WriteChunksToStream(const ANuid: string;
  AChunkCount: Cardinal; AStream: TStream; out AWritten: UInt64; out ADigest: string);
const
  CHUNK_FETCH_BATCH = 16;
var
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  consumerName: string;
  remaining: Cardinal;
  batch, i, n: Integer;
  msgs: IList<TNatsJsMsg>;
  hash: THashSHA2;
  got: Cardinal;
begin
  AWritten := 0;
  hash := THashSHA2.Create(THashSHA2.TSHA2Version.SHA256);
  if (ANuid = '') or (AChunkCount = 0) then
  begin
    ADigest := ObjDigestValue(hash.HashAsBytes);
    Exit;
  end;
  if AStream = nil then
    raise EDextNatsObjectStoreError.Create('Object Store Get stream is required');

  consumerName := 'osget_' + ObjNewNuid;
  cons := TNatsConsumerConfig.CreateDefault;
  cons.Name := consumerName;
  cons.FilterSubject := ChunkSubject(ANuid);
  cons.DeliverPolicy := dpAll;
  cons.AckPolicy := apNone;
  cons.MaxDeliver := 1;
  consInfo := FContext.FJs.CreateConsumer(FStreamName, cons);
  try
    remaining := AChunkCount;
    got := 0;
    while remaining > 0 do
    begin
      batch := Integer(remaining);
      if batch > CHUNK_FETCH_BATCH then
        batch := CHUNK_FETCH_BATCH;
      msgs := FContext.FJs.Fetch(FStreamName, consInfo.Name, batch, 30000);
      if msgs.Count = 0 then
        raise EDextNatsObjectStoreError.CreateFmt(
          'Object Store chunk fetch stalled for nuid %s: got %d of %d',
          [ANuid, got, AChunkCount]);
      if msgs.Count > batch then
        raise EDextNatsObjectStoreError.CreateFmt(
          'Object Store chunk batch overflow for nuid %s', [ANuid]);

      for i := 0 to msgs.Count - 1 do
      begin
        n := Length(msgs[i].Payload);
        if n > 0 then
        begin
          hash.Update(msgs[i].Payload);
          if AStream.Write(msgs[i].Payload[0], n) <> n then
            raise EDextNatsObjectStoreError.Create(
              'Object Store failed to write chunk to stream');
          Inc(AWritten, UInt64(n));
        end;
        Inc(got);
      end;
      if Cardinal(msgs.Count) > remaining then
        raise EDextNatsObjectStoreError.CreateFmt(
          'Object Store chunk count mismatch for nuid %s: expected %d',
          [ANuid, AChunkCount]);
      Dec(remaining, Cardinal(msgs.Count));
    end;
    if got <> AChunkCount then
      raise EDextNatsObjectStoreError.CreateFmt(
        'Object Store chunk count mismatch for nuid %s: expected %d got %d',
        [ANuid, AChunkCount, got]);
  finally
    try
      FContext.FJs.DeleteConsumer(FStreamName, consInfo.Name);
    except
    end;
  end;
  ADigest := ObjDigestValue(hash.HashAsBytes);
end;

function TDextNatsObjectStore.PutFromStream(const AName: string; AStream: TStream;
  const ADescription: string; const AHeaders: TNatsHeaders;
  const AMetadata: IDictionary<string, string>): TNatsObjectInfo;
var
  existing: TNatsObjectInfo;
  hadExisting: Boolean;
  nuid: string;
  chunkSubj: string;
  chunkSize, n, got, chunks: Integer;
  chunk: TBytes;
  hash: THashSHA2;
  total: UInt64;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');
  if AStream = nil then
    raise EDextNatsObjectStoreError.Create('Object Store Put stream is required');

  hadExisting := TryGetInfo(AName, existing);
  nuid := ObjNewNuid;
  chunkSubj := ChunkSubject(nuid);
  chunkSize := FChunkSize;
  if chunkSize <= 0 then
    chunkSize := NATS_OBJ_DEFAULT_CHUNK_SIZE;

  hash := THashSHA2.Create(THashSHA2.TSHA2Version.SHA256);
  chunks := 0;
  total := 0;
  SetLength(chunk, chunkSize);
  while True do
  begin
    got := 0;
    while got < chunkSize do
    begin
      n := AStream.Read(chunk[got], chunkSize - got);
      if n <= 0 then
        Break;
      Inc(got, n);
    end;
    if got = 0 then
      Break;
    if got < chunkSize then
      SetLength(chunk, got);
    hash.Update(chunk, Cardinal(got));
    FContext.FJs.Publish(chunkSubj, chunk);
    Inc(chunks);
    Inc(total, UInt64(got));
    if got < chunkSize then
      Break;
    if Length(chunk) <> chunkSize then
      SetLength(chunk, chunkSize);
  end;

  Result := Default(TNatsObjectInfo);
  Result.Name := AName;
  Result.Description := ADescription;
  Result.Headers := AHeaders;
  Result.Metadata := AMetadata;
  Result.Bucket := FBucket;
  Result.Nuid := nuid;
  Result.Size := total;
  Result.Chunks := Cardinal(chunks);
  Result.Digest := ObjDigestValue(hash.HashAsBytes);
  Result.Deleted := False;
  Result.ChunkSize := chunkSize;
  PublishMeta(Result);

  if hadExisting and (existing.Nuid <> '') then
  begin
    try
      PurgeSubject(ChunkSubject(existing.Nuid));
    except
      { Best-effort old-chunk cleanup; new object is already readable. }
    end;
  end;
end;

function TDextNatsObjectStore.Put(const AName: string; const AData: TBytes): TNatsObjectInfo;
var
  ms: TBytesStream;
begin
  ms := TBytesStream.Create(AData);
  try
    Result := Put(AName, ms);
  finally
    ms.Free;
  end;
end;

function TDextNatsObjectStore.Put(const AName: string; AStream: TStream): TNatsObjectInfo;
begin
  Result := PutFromStream(AName, AStream, '', nil, nil);
end;

function TDextNatsObjectStore.Put(const AMeta: TNatsObjectMeta; AStream: TStream): TNatsObjectInfo;
begin
  if AMeta.Name = '' then
    raise EDextNatsObjectStoreError.Create('Object Store Put requires a non-empty Name');
  Result := PutFromStream(AMeta.Name, AStream, AMeta.Description, AMeta.Headers, AMeta.Metadata);
end;

function TDextNatsObjectStore.PutFile(const AName, AFileName: string): TNatsObjectInfo;
var
  fs: TFileStream;
begin
  if AFileName = '' then
    raise EDextNatsObjectStoreError.Create('Object Store PutFile requires a file path');
  fs := TFileStream.Create(AFileName, fmOpenRead or fmShareDenyWrite);
  try
    Result := Put(AName, fs);
  finally
    fs.Free;
  end;
end;

function TDextNatsObjectStore.PutFile(const AFileName: string): TNatsObjectInfo;
begin
  Result := PutFile(ExtractFileName(AFileName), AFileName);
end;

function TDextNatsObjectStore.GetInfo(const AName: string): TNatsObjectInfo;
begin
  if not TryGetInfo(AName, Result) or Result.Deleted then
    raise EDextNatsObjectStoreError.CreateFmt('Object Store object not found: %s', [AName]);
end;

function TDextNatsObjectStore.AddLink(const AName: string;
  const ATarget: TNatsObjectInfo): TNatsObjectInfo;
var
  existing: TNatsObjectInfo;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');
  if ATarget.Name = '' then
    raise EDextNatsObjectStoreError.Create('Object Store AddLink requires a target object');
  if ATarget.Bucket = '' then
    raise EDextNatsObjectStoreError.Create('Object Store AddLink target bucket is required');
  if ATarget.Deleted then
    raise EDextNatsObjectStoreError.Create('Object Store cannot link to a deleted object');
  if ATarget.IsLink then
    raise EDextNatsObjectStoreError.Create('Object Store cannot link to another link');

  if TryGetInfo(AName, existing) then
  begin
    if not existing.IsLink then
      raise EDextNatsObjectStoreError.CreateFmt(
        'Object Store object already exists: %s', [AName]);
  end;

  Result := Default(TNatsObjectInfo);
  Result.Name := AName;
  Result.Bucket := FBucket;
  Result.Nuid := ObjNewNuid;
  Result.Size := 0;
  Result.Chunks := 0;
  Result.Deleted := False;
  Result.Link := TNatsObjectLink.Create(ATarget.Bucket, ATarget.Name);
  PublishMeta(Result);
end;

function TDextNatsObjectStore.AddBucketLink(const AName: string;
  AStore: TDextNatsObjectStore): TNatsObjectInfo;
var
  existing: TNatsObjectInfo;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');
  if AStore = nil then
    raise EDextNatsObjectStoreError.Create('Object Store AddBucketLink requires a target store');
  if AStore.Bucket = '' then
    raise EDextNatsObjectStoreError.Create('Object Store AddBucketLink target bucket is required');

  if TryGetInfo(AName, existing) then
  begin
    if not existing.IsLink then
      raise EDextNatsObjectStoreError.CreateFmt(
        'Object Store object already exists: %s', [AName]);
  end;

  Result := Default(TNatsObjectInfo);
  Result.Name := AName;
  Result.Bucket := FBucket;
  Result.Nuid := ObjNewNuid;
  Result.Size := 0;
  Result.Chunks := 0;
  Result.Deleted := False;
  Result.Link := TNatsObjectLink.CreateBucket(AStore.Bucket);
  PublishMeta(Result);
end;

function TDextNatsObjectStore.Get(const AName: string; AStream: TStream): TNatsObjectInfo;
var
  digest: string;
  written: UInt64;
  targetStore: TDextNatsObjectStore;
  linkBucket, linkName: string;
begin
  if AStream = nil then
    raise EDextNatsObjectStoreError.Create('Object Store Get stream is required');

  Result := GetInfo(AName);

  { Follow object links (nats.go Get); bucket links cannot be retrieved as bytes. }
  if Result.IsLink then
  begin
    if Result.IsBucketLink then
      raise EDextNatsObjectStoreError.CreateFmt(
        'Object Store cannot get bucket link: %s', [AName]);
    linkBucket := Result.Link.Bucket;
    linkName := Result.Link.Name;
    if linkBucket = FBucket then
      Exit(Get(linkName, AStream));
    targetStore := FContext.OpenStore(linkBucket);
    try
      Result := targetStore.Get(linkName, AStream);
    finally
      targetStore.Free;
    end;
    Exit;
  end;

  WriteChunksToStream(Result.Nuid, Result.Chunks, AStream, written, digest);
  if (Result.Digest <> '') and (digest <> Result.Digest) then
    raise EDextNatsObjectStoreError.CreateFmt('Object Store digest mismatch for %s', [AName]);
  if written <> Result.Size then
    raise EDextNatsObjectStoreError.CreateFmt(
      'Object Store size mismatch for %s: expected %d got %d',
      [AName, Result.Size, written]);
end;

function TDextNatsObjectStore.Get(const AName: string; out AInfo: TNatsObjectInfo): TBytes;
var
  ms: TBytesStream;
begin
  ms := TBytesStream.Create;
  try
    AInfo := Get(AName, ms);
    SetLength(Result, ms.Size);
    if ms.Size > 0 then
      Move(ms.Memory^, Result[0], ms.Size);
  finally
    ms.Free;
  end;
end;

function TDextNatsObjectStore.Get(const AName: string): TBytes;
var
  info: TNatsObjectInfo;
begin
  Result := Get(AName, info);
end;

function TDextNatsObjectStore.GetFile(const AName, AFileName: string): TNatsObjectInfo;
var
  fs: TFileStream;
begin
  if AFileName = '' then
    raise EDextNatsObjectStoreError.Create('Object Store GetFile requires a file path');
  fs := TFileStream.Create(AFileName, fmCreate);
  try
    Result := Get(AName, fs);
  finally
    fs.Free;
  end;
end;

procedure TDextNatsObjectStore.Delete(const AName: string);
var
  info: TNatsObjectInfo;
  nuid: string;
begin
  if not TryGetInfo(AName, info) then
    raise EDextNatsObjectStoreError.CreateFmt('Object Store object not found: %s', [AName]);
  if info.Deleted then
    Exit;

  nuid := info.Nuid;
  info.Deleted := True;
  info.Size := 0;
  info.Chunks := 0;
  info.Digest := '';
  PublishMeta(info);
  if nuid <> '' then
    PurgeSubject(ChunkSubject(nuid));
end;

function TDextNatsObjectStore.UpdateMeta(const AName: string;
  const AMeta: TNatsObjectMeta): TNatsObjectInfo;
var
  info, existing: TNatsObjectInfo;
  oldName: string;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');
  if AMeta.Name = '' then
    raise EDextNatsObjectStoreError.Create('Object Store UpdateMeta requires a non-empty Name');

  if not TryGetInfo(AName, info) or info.Deleted then
    raise EDextNatsObjectStoreError.CreateFmt(
      'Object Store UpdateMeta failed: object missing or deleted: %s', [AName]);

  if AName <> AMeta.Name then
  begin
    if TryGetInfo(AMeta.Name, existing) and (not existing.Deleted) then
      raise EDextNatsObjectStoreError.CreateFmt(
        'Object Store object already exists: %s', [AMeta.Name]);
  end;

  { Preserve nuid/size/chunks/digest/options; only replace mutable meta fields. }
  oldName := info.Name;
  info.Name := AMeta.Name;
  info.Description := AMeta.Description;
  info.Headers := AMeta.Headers;
  info.Metadata := AMeta.Metadata;
  PublishMeta(info);

  if oldName <> info.Name then
  begin
    try
      PurgeSubject(MetaSubject(oldName));
    except
      { Meta is already under the new name; best-effort old-subject cleanup. }
    end;
  end;

  Result := info;
end;

procedure TDextNatsObjectStore.Seal;
var
  si: TNatsStreamInfo;
  cfg: TNatsStreamConfig;
begin
  si := FContext.FJs.GetStreamInfo(FStreamName);
  cfg := si.Config;
  if cfg.Name = '' then
    cfg.Name := FStreamName;
  if Length(cfg.Subjects) = 0 then
    cfg.Subjects := [Format('$O.%s.C.>', [FBucket]), Format('$O.%s.M.>', [FBucket])];
  if cfg.Sealed then
    Exit;
  cfg.Sealed := True;
  FContext.FJs.UpdateStream(cfg);
end;

function TDextNatsObjectStore.IsSealed: Boolean;
begin
  Result := FContext.FJs.GetStreamInfo(FStreamName).Config.Sealed;
end;

function TDextNatsObjectStore.List(AIncludeDeleted: Boolean): IList<TNatsObjectInfo>;
const
  LIST_BATCH = 64;
var
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  consumerName: string;
  msgs: IList<TNatsJsMsg>;
  batch, i: Integer;
  info: TNatsObjectInfo;
  pending: Integer;
begin
  Result := TCollections.CreateList<TNatsObjectInfo>;

  consumerName := 'oslist_' + ObjNewNuid;
  cons := TNatsConsumerConfig.CreateDefault;
  cons.Name := consumerName;
  cons.FilterSubject := MetaWildcardSubject;
  cons.DeliverPolicy := dpLastPerSubject;
  cons.AckPolicy := apNone;
  cons.MaxDeliver := 1;
  consInfo := FContext.FJs.CreateConsumer(FStreamName, cons);
  try
    pending := Integer(consInfo.NumPending);
    if pending <= 0 then
      Exit;

    while pending > 0 do
    begin
      batch := pending;
      if batch > LIST_BATCH then
        batch := LIST_BATCH;
      msgs := FContext.FJs.Fetch(FStreamName, consInfo.Name, batch, 5000);
      if msgs.Count = 0 then
        Break;

      for i := 0 to msgs.Count - 1 do
      begin
        if Length(msgs[i].Payload) = 0 then
          Continue;
        info := TNatsObjectInfo.Parse(TEncoding.UTF8.GetString(msgs[i].Payload));
        if (not AIncludeDeleted) and info.Deleted then
          Continue;
        if info.Bucket = '' then
          info.Bucket := FBucket;
        Result.Add(info);
      end;

      pending := msgs[msgs.Count - 1].NumPending;
      if pending < 0 then
        pending := 0;
    end;
  finally
    try
      FContext.FJs.DeleteConsumer(FStreamName, consInfo.Name);
    except
    end;
  end;
end;

function TDextNatsObjectStore.ListObjects(AIncludeDeleted: Boolean): IList<TNatsObjectInfo>;
begin
  Result := List(AIncludeDeleted);
end;

function TDextNatsObjectStore.Keys: IList<string>;
var
  infos: IList<TNatsObjectInfo>;
  i: Integer;
begin
  Result := TCollections.CreateList<string>;
  infos := List(False);
  for i := 0 to infos.Count - 1 do
    if infos[i].Name <> '' then
      Result.Add(infos[i].Name);
end;

function TDextNatsObjectStore.NameFromMetaSubject(const ASubject: string): string;
var
  prefix, enc: string;
  p: Integer;
begin
  Result := '';
  prefix := Format('$O.%s.M.', [FBucket]);
  if not ASubject.StartsWith(prefix, True) then
    Exit;
  enc := Copy(ASubject, Length(prefix) + 1, MaxInt);
  { Reject wildcards / multi-token leftovers. }
  p := Pos('.', enc);
  if p > 0 then
    Exit;
  Result := ObjDecodeName(enc);
end;

function TDextNatsObjectStore.InfoFromJsMsg(const AMsg: TNatsJsMsg): TNatsObjectInfo;
begin
  Result := Default(TNatsObjectInfo);
  if Length(AMsg.Payload) = 0 then
  begin
    { MetaOnly / headers_only: recover object name from $O.<bucket>.M.<b64url>. }
    Result.Name := NameFromMetaSubject(AMsg.Subject);
    Result.Bucket := FBucket;
    Exit;
  end;
  Result := TNatsObjectInfo.Parse(TEncoding.UTF8.GetString(AMsg.Payload));
  if Result.Bucket = '' then
    Result.Bucket := FBucket;
end;

{ TNatsOsWatchGate — coordinates EndOfInitial using consumer NumPending
  (nats.go / KV Watch semantics). Owned by TDextNatsObjectStoreWatcher. }

type
  TNatsOsWatchGate = class
  private
    FLock: TCriticalSection;
    FHandler: TNatsObjectStoreWatchHandler;
    FUpdatesOnly: Boolean;
    FStopped: Boolean;
    FInitDone: Boolean;
    FInitPendingKnown: Boolean;
    FInitPending: UInt64;
    FReceived: UInt64;
  public
    constructor Create(AHandler: TNatsObjectStoreWatchHandler; AUpdatesOnly: Boolean);
    destructor Destroy; override;
    procedure Stop;
    procedure HandleJsMsg(const AInfo: TNatsObjectInfo; ANumPending: Integer);
    procedure NotifyConsumerPending(ANumPending: UInt64);
    function InitialDone: Boolean;
  end;

constructor TNatsOsWatchGate.Create(AHandler: TNatsObjectStoreWatchHandler; AUpdatesOnly: Boolean);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FHandler := AHandler;
  FUpdatesOnly := AUpdatesOnly;
  { UpdatesOnly skips the snapshot marker entirely (nats.go / KV). }
  FInitDone := AUpdatesOnly;
  FInitPendingKnown := AUpdatesOnly;
end;

destructor TNatsOsWatchGate.Destroy;
begin
  Stop;
  FreeAndNil(FLock);
  inherited;
end;

procedure TNatsOsWatchGate.Stop;
begin
  FLock.Enter;
  try
    FStopped := True;
  finally
    FLock.Leave;
  end;
end;

function TNatsOsWatchGate.InitialDone: Boolean;
begin
  FLock.Enter;
  try
    Result := FInitDone;
  finally
    FLock.Leave;
  end;
end;

procedure TNatsOsWatchGate.NotifyConsumerPending(ANumPending: UInt64);
var
  fireMarker: Boolean;
  handler: TNatsObjectStoreWatchHandler;
begin
  fireMarker := False;
  handler := nil;
  FLock.Enter;
  try
    if FStopped or FUpdatesOnly then
      Exit;
    if not FInitPendingKnown then
    begin
      FInitPending := ANumPending;
      FInitPendingKnown := True;
    end;
    if (not FInitDone) and (FReceived >= FInitPending) then
    begin
      FInitDone := True;
      fireMarker := True;
      handler := FHandler;
    end;
  finally
    FLock.Leave;
  end;
  if fireMarker and Assigned(handler) then
    handler(TNatsObjectInfo.EndOfInitialMarker);
end;

procedure TNatsOsWatchGate.HandleJsMsg(const AInfo: TNatsObjectInfo; ANumPending: Integer);
var
  fireMarker: Boolean;
  handler: TNatsObjectStoreWatchHandler;
  pending: UInt64;
begin
  fireMarker := False;
  handler := nil;
  if ANumPending < 0 then
    pending := 0
  else
    pending := UInt64(ANumPending);

  FLock.Enter;
  try
    if FStopped then
      Exit;
    handler := FHandler;
    if not FInitDone then
    begin
      if not FInitPendingKnown then
      begin
        { Bootstrap from this delivery: NumPending is remaining after this msg. }
        FInitPending := pending + 1;
        FInitPendingKnown := True;
      end;
      Inc(FReceived);
    end;
  finally
    FLock.Leave;
  end;

  if Assigned(handler) then
    handler(AInfo);

  FLock.Enter;
  try
    if FStopped or FInitDone or FUpdatesOnly then
      Exit;
    if (FReceived >= FInitPending) or (pending = 0) then
    begin
      FInitDone := True;
      fireMarker := True;
      handler := FHandler;
    end;
  finally
    FLock.Leave;
  end;

  if fireMarker and Assigned(handler) then
    handler(TNatsObjectInfo.EndOfInitialMarker);
end;

{ TDextNatsObjectStoreWatcher }

constructor TDextNatsObjectStoreWatcher.Create(AJs: TDextNatsJetStreamContext;
  const AStreamName, AConsumerName: string; APushSub: TDextNatsJetStreamPushSubscription;
  AGate: TObject);
begin
  inherited Create;
  if AJs = nil then
    raise EDextNatsObjectStoreError.Create('TDextNatsObjectStoreWatcher requires a JetStream context');
  if APushSub = nil then
    raise EDextNatsObjectStoreError.Create('TDextNatsObjectStoreWatcher requires a push subscription');
  FJs := AJs;
  FStreamName := AStreamName;
  FConsumerName := AConsumerName;
  FPushSub := APushSub;
  FGate := AGate;
  FActive := True;
end;

destructor TDextNatsObjectStoreWatcher.Destroy;
begin
  Stop;
  FreeAndNil(FGate);
  inherited;
end;

function TDextNatsObjectStoreWatcher.GetInitialDone: Boolean;
begin
  if FGate is TNatsOsWatchGate then
    Result := TNatsOsWatchGate(FGate).InitialDone
  else
    Result := False;
end;

procedure TDextNatsObjectStoreWatcher.Stop;
begin
  if not FActive then
    Exit;
  FActive := False;
  if FGate is TNatsOsWatchGate then
    TNatsOsWatchGate(FGate).Stop;
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

function TDextNatsObjectStore.StartWatch(const AFilterSubject: string;
  AHandler: TNatsObjectStoreWatchHandler;
  const AOptions: TNatsObjectStoreWatchOptions): TDextNatsObjectStoreWatcher;
var
  deliver, consumerName: string;
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  push: TDextNatsJetStreamPushSubscription;
  js: TDextNatsJetStreamContext;
  gate: TNatsOsWatchGate;
begin
  if not Assigned(AHandler) then
    raise EDextNatsObjectStoreError.Create('Watch requires a handler');
  if AFilterSubject = '' then
    raise EDextNatsObjectStoreError.Create('Watch requires a filter subject');

  js := FContext.FJs;
  deliver := js.Client.NewInbox;
  consumerName := 'oswatch_' + ObjNewNuid;
  gate := TNatsOsWatchGate.Create(AHandler, AOptions.UpdatesOnly);
  push := nil;
  try
    { Subscribe before CONSUMER.CREATE so the deliver subject has interest. }
    push := js.SubscribePush(deliver,
      procedure(const AMsg: TNatsJsMsg)
      var
        info: TNatsObjectInfo;
      begin
        info := InfoFromJsMsg(AMsg);
        if info.Name = '' then
          Exit;
        gate.HandleJsMsg(info, AMsg.NumPending);
      end);
    cons := TNatsConsumerConfig.CreateDefault;
    cons.Name := consumerName;
    cons.FilterSubject := AFilterSubject;
    cons.DeliverSubject := deliver;
    if AOptions.UpdatesOnly then
      cons.DeliverPolicy := dpNew
    else
      cons.DeliverPolicy := dpLastPerSubject;
    cons.AckPolicy := apNone;
    { ack_policy=none rejects a positive max_ack_pending (JS API 10082). }
    cons.MaxAckPending := 0;
    cons.HeadersOnly := AOptions.MetaOnly;
    consInfo := js.CreateConsumer(FStreamName, cons);
    gate.NotifyConsumerPending(consInfo.NumPending);
  except
    if push <> nil then
      push.Free;
    gate.Free;
    raise;
  end;

  Result := TDextNatsObjectStoreWatcher.Create(js, FStreamName, consInfo.Name, push, gate);
end;

function TDextNatsObjectStore.Watch(const AName: string;
  AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
begin
  Result := Watch(AName, AHandler, TNatsObjectStoreWatchOptions.CreateDefault);
end;

function TDextNatsObjectStore.Watch(const AName: string; AHandler: TNatsObjectStoreWatchHandler;
  const AOptions: TNatsObjectStoreWatchOptions): TDextNatsObjectStoreWatcher;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');
  Result := StartWatch(MetaSubject(AName), AHandler, AOptions);
end;

function TDextNatsObjectStore.WatchAll(AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
begin
  Result := WatchAll(AHandler, TNatsObjectStoreWatchOptions.CreateDefault);
end;

function TDextNatsObjectStore.WatchAll(AHandler: TNatsObjectStoreWatchHandler;
  const AOptions: TNatsObjectStoreWatchOptions): TDextNatsObjectStoreWatcher;
begin
  Result := StartWatch(MetaWildcardSubject, AHandler, AOptions);
end;

end.
