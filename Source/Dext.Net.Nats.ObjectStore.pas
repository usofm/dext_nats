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
{  Watch / WatchAll, UpdateMeta, Seal. Deferred: links.                     }
{  TDextNatsObjectStoreContext wraps a TDextNatsJetStreamContext (or        }
{  creates one from TDextNatsClient); it does not own the client.           }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.ObjectStore;

interface

uses
  System.SysUtils,
  System.Classes,
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
    /// <summary>Parses ObjectInfo JSON (mtime is ignored / never stored).</summary>
    class function Parse(const AJson: string): TNatsObjectInfo; static;
    /// <summary>Serializes ObjectInfo for metadata publish (omits mtime).</summary>
    function ToJson: string;
  end;

  /// <summary>Callback for <see cref="TDextNatsObjectStore.Watch"/> / WatchAll deliveries.</summary>
  TNatsObjectStoreWatchHandler = reference to procedure(const AInfo: TNatsObjectInfo);

  /// <summary>
  ///   Active Object Store watch (ephemeral push consumer + deliver-subject SUB).
  ///   Does not own the JetStream context. Call <see cref="Stop"/> or Free before
  ///   freeing the Object Store. Handlers run on the client's receive thread —
  ///   do not block with Request/Fetch.
  /// </summary>
  TDextNatsObjectStoreWatcher = class
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
    function FetchChunks(const ANuid: string; AChunkCount: Cardinal): TBytes;
    function InfoFromJsMsg(const AMsg: TNatsJsMsg): TNatsObjectInfo;
    function StartWatch(const AFilterSubject: string;
      AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
  public
    constructor Create(AContext: TDextNatsObjectStoreContext; const ABucket: string;
      AChunkSize: Integer = 0);

    /// <summary>Stores AData under AName (chunked). Overwrites prior object of the same name.</summary>
    function Put(const AName: string; const AData: TBytes): TNatsObjectInfo;
    /// <summary>Reassembles chunks for AName. Raises if missing, deleted, or digest mismatch.</summary>
    function Get(const AName: string): TBytes; overload;
    /// <summary>Same as Get, also returns the metadata used for the read.</summary>
    function Get(const AName: string; out AInfo: TNatsObjectInfo): TBytes; overload;
    /// <summary>Current metadata for a live (non-deleted) object.</summary>
    function GetInfo(const AName: string): TNatsObjectInfo;
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
    ///   Watches one object: delivers the current last-per-subject meta (if any), then updates.
    ///   Caller must Free the watcher (or Stop) before freeing this store.
    /// </summary>
    function Watch(const AName: string; AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
    /// <summary>
    ///   Watches the whole bucket meta subjects (<c>$O.&lt;bucket&gt;.M.&gt;</c>):
    ///   last-per-subject snapshot + subsequent updates. Minimal MVP: no end-of-initial
    ///   marker; handlers run on the receive thread.
    /// </summary>
    function WatchAll(AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;

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
  if ChunkSize > 0 then
  begin
    jw.WritePropertyName('options');
    jw.WriteStartObject;
    jw.WritePropertyName('max_chunk_size');
    jw.WriteNumber(ChunkSize);
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

function TDextNatsObjectStore.FetchChunks(const ANuid: string; AChunkCount: Cardinal): TBytes;
var
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  consumerName: string;
  batch: Integer;
  msgs: IList<TNatsJsMsg>;
  i, offset, total: Integer;
begin
  SetLength(Result, 0);
  if (ANuid = '') or (AChunkCount = 0) then
    Exit;

  consumerName := 'osget_' + ObjNewNuid;
  cons := TNatsConsumerConfig.CreateDefault;
  cons.Name := consumerName;
  cons.FilterSubject := ChunkSubject(ANuid);
  cons.DeliverPolicy := dpAll;
  cons.AckPolicy := apNone;
  cons.MaxDeliver := 1;
  consInfo := FContext.FJs.CreateConsumer(FStreamName, cons);
  try
    batch := Integer(AChunkCount);
    if batch < 1 then
      batch := 1;
    msgs := FContext.FJs.Fetch(FStreamName, consInfo.Name, batch, 30000);
    if msgs.Count <> Integer(AChunkCount) then
      raise EDextNatsObjectStoreError.CreateFmt(
        'Object Store chunk count mismatch for nuid %s: expected %d got %d',
        [ANuid, AChunkCount, msgs.Count]);

    total := 0;
    for i := 0 to msgs.Count - 1 do
      Inc(total, Length(msgs[i].Payload));
    SetLength(Result, total);
    offset := 0;
    for i := 0 to msgs.Count - 1 do
    begin
      if Length(msgs[i].Payload) > 0 then
      begin
        Move(msgs[i].Payload[0], Result[offset], Length(msgs[i].Payload));
        Inc(offset, Length(msgs[i].Payload));
      end;
    end;
  finally
    try
      FContext.FJs.DeleteConsumer(FStreamName, consInfo.Name);
    except
    end;
  end;
end;

function TDextNatsObjectStore.Put(const AName: string; const AData: TBytes): TNatsObjectInfo;
var
  existing: TNatsObjectInfo;
  hadExisting: Boolean;
  nuid: string;
  chunkSubj: string;
  chunkSize, offset, n, chunks: Integer;
  chunk: TBytes;
  hash: THashSHA2;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');

  hadExisting := TryGetInfo(AName, existing);
  nuid := ObjNewNuid;
  chunkSubj := ChunkSubject(nuid);
  chunkSize := FChunkSize;
  if chunkSize <= 0 then
    chunkSize := NATS_OBJ_DEFAULT_CHUNK_SIZE;

  hash := THashSHA2.Create(THashSHA2.TSHA2Version.SHA256);
  chunks := 0;
  offset := 0;
  while offset < Length(AData) do
  begin
    n := Length(AData) - offset;
    if n > chunkSize then
      n := chunkSize;
    SetLength(chunk, n);
    Move(AData[offset], chunk[0], n);
    hash.Update(chunk);
    FContext.FJs.Publish(chunkSubj, chunk);
    Inc(chunks);
    Inc(offset, n);
  end;

  Result := Default(TNatsObjectInfo);
  Result.Name := AName;
  Result.Bucket := FBucket;
  Result.Nuid := nuid;
  Result.Size := UInt64(Length(AData));
  Result.Chunks := Cardinal(chunks);
  Result.Digest := ObjDigestValue(hash.HashAsBytes);
  Result.Deleted := False;
  Result.ChunkSize := chunkSize;
  PublishMeta(Result);

  if hadExisting and (existing.Nuid <> '') and (not existing.Deleted) then
  begin
    try
      PurgeSubject(ChunkSubject(existing.Nuid));
    except
      { Best-effort old-chunk cleanup; new object is already readable. }
    end;
  end
  else if hadExisting and existing.Deleted and (existing.Nuid <> '') then
  begin
    try
      PurgeSubject(ChunkSubject(existing.Nuid));
    except
    end;
  end;
end;

function TDextNatsObjectStore.GetInfo(const AName: string): TNatsObjectInfo;
begin
  if not TryGetInfo(AName, Result) or Result.Deleted then
    raise EDextNatsObjectStoreError.CreateFmt('Object Store object not found: %s', [AName]);
end;

function TDextNatsObjectStore.Get(const AName: string; out AInfo: TNatsObjectInfo): TBytes;
var
  hash: THashSHA2;
  digest: string;
begin
  AInfo := GetInfo(AName);
  if AInfo.Chunks = 0 then
  begin
    SetLength(Result, 0);
    hash := THashSHA2.Create(THashSHA2.TSHA2Version.SHA256);
    digest := ObjDigestValue(hash.HashAsBytes);
    if (AInfo.Digest <> '') and (digest <> AInfo.Digest) then
      raise EDextNatsObjectStoreError.CreateFmt('Object Store digest mismatch for %s', [AName]);
    Exit;
  end;

  Result := FetchChunks(AInfo.Nuid, AInfo.Chunks);
  hash := THashSHA2.Create(THashSHA2.TSHA2Version.SHA256);
  if Length(Result) > 0 then
    hash.Update(Result);
  digest := ObjDigestValue(hash.HashAsBytes);
  if (AInfo.Digest <> '') and (digest <> AInfo.Digest) then
    raise EDextNatsObjectStoreError.CreateFmt('Object Store digest mismatch for %s', [AName]);
  if UInt64(Length(Result)) <> AInfo.Size then
    raise EDextNatsObjectStoreError.CreateFmt(
      'Object Store size mismatch for %s: expected %d got %d',
      [AName, AInfo.Size, Length(Result)]);
end;

function TDextNatsObjectStore.Get(const AName: string): TBytes;
var
  info: TNatsObjectInfo;
begin
  Result := Get(AName, info);
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

function TDextNatsObjectStore.InfoFromJsMsg(const AMsg: TNatsJsMsg): TNatsObjectInfo;
begin
  Result := Default(TNatsObjectInfo);
  if Length(AMsg.Payload) = 0 then
    Exit;
  Result := TNatsObjectInfo.Parse(TEncoding.UTF8.GetString(AMsg.Payload));
  if Result.Bucket = '' then
    Result.Bucket := FBucket;
end;

{ TDextNatsObjectStoreWatcher }

constructor TDextNatsObjectStoreWatcher.Create(AJs: TDextNatsJetStreamContext;
  const AStreamName, AConsumerName: string; APushSub: TDextNatsJetStreamPushSubscription);
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
  FActive := True;
end;

destructor TDextNatsObjectStoreWatcher.Destroy;
begin
  Stop;
  inherited;
end;

procedure TDextNatsObjectStoreWatcher.Stop;
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

function TDextNatsObjectStore.StartWatch(const AFilterSubject: string;
  AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
var
  deliver, consumerName: string;
  cons: TNatsConsumerConfig;
  consInfo: TNatsConsumerInfo;
  push: TDextNatsJetStreamPushSubscription;
  js: TDextNatsJetStreamContext;
begin
  if not Assigned(AHandler) then
    raise EDextNatsObjectStoreError.Create('Watch requires a handler');
  if AFilterSubject = '' then
    raise EDextNatsObjectStoreError.Create('Watch requires a filter subject');

  js := FContext.FJs;
  deliver := js.Client.NewInbox;
  consumerName := 'oswatch_' + ObjNewNuid;

  { Subscribe before CONSUMER.CREATE so the deliver subject has interest. }
  push := js.SubscribePush(deliver,
    procedure(const AMsg: TNatsJsMsg)
    var
      info: TNatsObjectInfo;
    begin
      info := InfoFromJsMsg(AMsg);
      if info.Name = '' then
        Exit;
      AHandler(info);
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
    consInfo := js.CreateConsumer(FStreamName, cons);
  except
    push.Free;
    raise;
  end;

  Result := TDextNatsObjectStoreWatcher.Create(js, FStreamName, consInfo.Name, push);
end;

function TDextNatsObjectStore.Watch(const AName: string;
  AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
begin
  if AName = '' then
    raise EDextNatsObjectStoreError.Create('Object name is required');
  Result := StartWatch(MetaSubject(AName), AHandler);
end;

function TDextNatsObjectStore.WatchAll(AHandler: TNatsObjectStoreWatchHandler): TDextNatsObjectStoreWatcher;
begin
  Result := StartWatch(MetaWildcardSubject, AHandler);
end;

end.
