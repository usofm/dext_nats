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
{  NATS Services API (ADR-32 / nats.go micro).                              }
{  Register request/reply endpoints and auto-respond on $SRV.PING|INFO|STATS }
{  discovery subjects. TDextNatsService wraps an existing TDextNatsClient by }
{  composition and does not own its lifetime.                               }
{                                                                           }
{  Covers: name/version/description, AddEndpoint (subject + handler),       }
{  AddGroup (subject-prefix groups + nested AddGroup / AddEndpoint),        }
{  queue group (default "q", inherit/override on group and endpoint),       }
{  PING/INFO/STATS at all/kind/instance subjects, basic stats               }
{  (num_requests / num_errors / processing_time), Respond / RespondError    }
{  headers, graceful Stop (UNSUB).                                          }
{                                                                           }
{  Gaps vs full nats.go micro: custom StatsHandler data, DoneHandler /      }
{  ErrorHandler, schema JSON, connection-closed auto-stop, subscription     }
{  Drain (Stop uses Unsubscribe), metadata immutability enforcement.        }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Services;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Collections,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats;

const
  /// <summary>Root prefix for service discovery subjects (ADR-32).</summary>
  NATS_SRV_PREFIX = '$SRV';
  /// <summary>Default queue group for endpoint subscriptions (nats.go micro).</summary>
  NATS_SRV_DEFAULT_QUEUE = 'q';
  NATS_SRV_PING_RESPONSE_TYPE = 'io.nats.micro.v1.ping_response';
  NATS_SRV_INFO_RESPONSE_TYPE = 'io.nats.micro.v1.info_response';
  NATS_SRV_STATS_RESPONSE_TYPE = 'io.nats.micro.v1.stats_response';
  /// <summary>Standard service error description header (ADR-32).</summary>
  NATS_SRV_ERROR_HEADER = 'Nats-Service-Error';
  /// <summary>Standard service error code header (numeric string).</summary>
  NATS_SRV_ERROR_CODE_HEADER = 'Nats-Service-Error-Code';

type
  /// <summary>Raised for Services validation / lifecycle errors.</summary>
  EDextNatsServiceError = class(EDextNatsException);

  /// <summary>Discovery verb used in <c>$SRV.&lt;VERB&gt;[.&lt;name&gt;[.&lt;id&gt;]]</c>.</summary>
  TNatsServiceVerb = (svPing, svStats, svInfo);

  TNatsServiceRequest = class;
  /// <summary>Endpoint request handler. Runs on the client's receive thread — do not block with Request.</summary>
  TNatsServiceHandler = reference to procedure(const ARequest: TNatsServiceRequest);

  /// <summary>
  ///   Request context for a service endpoint. Call <see cref="Respond"/> or
  ///   <see cref="RespondError"/> once. Freed by the framework after the handler returns.
  /// </summary>
  TNatsServiceRequest = class
  private
    FClient: TDextNatsClient;
    FMsg: TNatsMsg;
    FResponded: Boolean;
    FErrorDescription: string;
    function GetSubject: string;
    function GetReplyTo: string;
    function GetData: TBytes;
    function GetHeaders: TNatsHeaders;
  public
    constructor Create(AClient: TDextNatsClient; const AMsg: TNatsMsg);
    /// <summary>Publishes AData to the request's reply subject.</summary>
    procedure Respond(const AData: TBytes); overload;
    /// <summary>UTF-8 convenience overload of Respond.</summary>
    procedure Respond(const AMessage: string); overload;
    /// <summary>
    ///   Responds with <c>Nats-Service-Error</c> / <c>Nats-Service-Error-Code</c> headers
    ///   (requires server header support).
    /// </summary>
    procedure RespondError(ACode: Integer; const ADescription: string; const AData: TBytes); overload;
    procedure RespondError(ACode: Integer; const ADescription: string); overload;
    /// <summary>Decodes Data as UTF-8.</summary>
    function AsString: string;
    property Subject: string read GetSubject;
    property ReplyTo: string read GetReplyTo;
    property Data: TBytes read GetData;
    property Headers: TNatsHeaders read GetHeaders;
    property Responded: Boolean read FResponded;
    /// <summary>Set when RespondError was used; empty otherwise.</summary>
    property ErrorDescription: string read FErrorDescription;
  end;

  /// <summary>Configuration for <see cref="TDextNatsService.AddService"/>.</summary>
  TNatsServiceConfig = record
    /// <summary>Service kind (A-Z, a-z, 0-9, dash, underscore). Shared by all instances.</summary>
    Name: string;
    /// <summary>SemVer string (required).</summary>
    Version: string;
    Description: string;
    /// <summary>Optional instance metadata (serialized on PING/INFO/STATS). Nil = empty object.</summary>
    Metadata: IDictionary<string, string>;
    /// <summary>Queue group for endpoints; empty uses <see cref="NATS_SRV_DEFAULT_QUEUE"/>.</summary>
    QueueGroup: string;
    /// <summary>When True, endpoints subscribe without a queue group.</summary>
    QueueGroupDisabled: Boolean;
    /// <summary>Defaults: empty description, queue "q", metadata nil.</summary>
    class function CreateDefault(const AName, AVersion: string): TNatsServiceConfig; static;
    /// <summary>Raises <see cref="EDextNatsServiceError"/> when name/version/queue are invalid.</summary>
    procedure Validate;
  end;

  /// <summary>Configuration for <see cref="TDextNatsService.AddEndpoint"/>.</summary>
  TNatsEndpointConfig = record
    /// <summary>Endpoint name (alphanumeric / dash / underscore).</summary>
    Name: string;
    /// <summary>Subject to subscribe; empty uses Name. Prefixed when added via a group.</summary>
    Subject: string;
    /// <summary>Optional queue override; empty inherits the service or group queue.</summary>
    QueueGroup: string;
    /// <summary>When True, this endpoint uses a plain subscribe (no queue).</summary>
    QueueGroupDisabled: Boolean;
    /// <summary>Optional endpoint metadata (INFO). Nil = empty object.</summary>
    Metadata: IDictionary<string, string>;
    /// <summary>Defaults: Subject=Name, inherit service queue.</summary>
    class function CreateDefault(const AName: string; const ASubject: string = ''): TNatsEndpointConfig; static;
    procedure Validate;
  end;

  /// <summary>Configuration for <see cref="TDextNatsService.AddGroup"/> (nats.go micro Group).</summary>
  TNatsGroupConfig = record
    /// <summary>
    ///   Subject prefix for endpoints in this group (may contain dots). Empty is allowed
    ///   (endpoints use their own subject only). Nested groups join prefixes with <c>.</c>.
    /// </summary>
    Prefix: string;
    /// <summary>Optional queue override; empty inherits the parent service/group queue.</summary>
    QueueGroup: string;
    /// <summary>When True, endpoints in this group subscribe without a queue group.</summary>
    QueueGroupDisabled: Boolean;
    /// <summary>Defaults: inherit parent queue.</summary>
    class function CreateDefault(const APrefix: string): TNatsGroupConfig; static;
    /// <summary>Raises when Prefix or QueueGroup fail subject validation.</summary>
    procedure Validate;
  end;

  TDextNatsService = class;
  TDextNatsServiceGroup = class;

  /// <summary>
  ///   Subject-prefix group for nested endpoints (nats.go micro <c>Group</c>).
  ///   Owned by the parent <see cref="TDextNatsService"/> — do not Free.
  /// </summary>
  TDextNatsServiceGroup = class
  private
    FService: TDextNatsService;
    FPrefix: string;
    FQueueGroup: string;
    FQueueGroupDisabled: Boolean;
    constructor Create(AService: TDextNatsService; const APrefix, AQueueGroup: string;
      AQueueGroupDisabled: Boolean);
  public
    /// <summary>Creates a nested group: subject prefix becomes <c>Prefix.&lt;APrefix&gt;</c>.</summary>
    function AddGroup(const APrefix: string): TDextNatsServiceGroup; overload;
    /// <summary>Creates a nested group with queue override options.</summary>
    function AddGroup(const AConfig: TNatsGroupConfig): TDextNatsServiceGroup; overload;
    /// <summary>Registers an endpoint under <c>Prefix.&lt;name&gt;</c> (or Prefix.Subject).</summary>
    procedure AddEndpoint(const AName: string; const AHandler: TNatsServiceHandler); overload;
    /// <summary>Registers an endpoint; subject is prefixed with this group's Prefix.</summary>
    procedure AddEndpoint(const AConfig: TNatsEndpointConfig;
      const AHandler: TNatsServiceHandler); overload;
    property Service: TDextNatsService read FService;
    /// <summary>Resolved subject prefix (stacked for nested groups).</summary>
    property Prefix: string read FPrefix;
  end;

  /// <summary>
  ///   Running NATS microservice instance (ADR-32). Does not own the client.
  ///   Call <see cref="Stop"/> or Free before freeing the client. Handlers run on
  ///   the receive thread — avoid blocking Request/Flush inside them.
  /// </summary>
  TDextNatsService = class
  private
    type
      TEndpointState = class
      public
        Name: string;
        Subject: string;
        QueueGroup: string;
        Metadata: IDictionary<string, string>;
        Handler: TNatsServiceHandler;
        Sid: Integer;
        NumRequests: Int64;
        NumErrors: Int64;
        LastError: string;
        ProcessingTimeNanos: Int64;
      end;
  private
    FClient: TDextNatsClient;
    FLock: TCriticalSection;
    FName: string;
    FVersion: string;
    FDescription: string;
    FId: string;
    FMetadata: IDictionary<string, string>;
    FQueueGroup: string;
    FQueueGroupDisabled: Boolean;
    FEndpoints: IList<TEndpointState>;
    FGroups: IList<TDextNatsServiceGroup>;
    FDiscoverySids: IList<Integer>;
    FStartedUtc: TDateTime;
    FStopped: Boolean;
    function ResolveQueueGroup(const ACustomQueue: string; ACustomDisabled: Boolean;
      const AParentQueue: string; AParentDisabled: Boolean): string;
    procedure ResolveQueueGroupEx(const ACustomQueue: string; ACustomDisabled: Boolean;
      const AParentQueue: string; AParentDisabled: Boolean;
      out AQueue: string; out ADisabled: Boolean);
    function CreateGroup(const AConfig: TNatsGroupConfig;
      const AParentQueue: string; AParentQueueDisabled: Boolean): TDextNatsServiceGroup;
    procedure AddEndpointInternal(const AConfig: TNatsEndpointConfig;
      const AHandler: TNatsServiceHandler; const ASubjectPrefix: string;
      const AParentQueue: string; AParentQueueDisabled: Boolean);
    procedure EnsureRunning;
    procedure SubscribeDiscovery;
    procedure SubscribeDiscoverySubject(AVerb: TNatsServiceVerb; const ASubject: string);
    procedure HandleDiscovery(AVerb: TNatsServiceVerb; const AMsg: TNatsMsg);
    procedure HandleEndpoint(AEndpoint: TEndpointState; const AMsg: TNatsMsg);
    function BuildPingJson: string;
    function BuildInfoJson: string;
    function BuildStatsJson: string;
    function GetStopped: Boolean;
  public
    constructor Create(AClient: TDextNatsClient; const AConfig: TNatsServiceConfig);
    destructor Destroy; override;

    /// <summary>
    ///   Creates a service instance, assigns a unique Id, and subscribes to
    ///   <c>$SRV.PING|INFO|STATS</c> (all / kind / instance). Does not own AClient.
    /// </summary>
    class function AddService(AClient: TDextNatsClient;
      const AConfig: TNatsServiceConfig): TDextNatsService; static;

    /// <summary>Registers a request/reply endpoint (queue subscribe by default).</summary>
    procedure AddEndpoint(const AName: string; const AHandler: TNatsServiceHandler); overload;
    /// <summary>Registers a request/reply endpoint with explicit subject / queue options.</summary>
    procedure AddEndpoint(const AConfig: TNatsEndpointConfig;
      const AHandler: TNatsServiceHandler); overload;

    /// <summary>
    ///   Creates a subject-prefix group (nats.go micro). Endpoints added to the group
    ///   subscribe on <c>APrefix.&lt;endpoint&gt;</c>. Owned by this service — do not Free.
    /// </summary>
    function AddGroup(const APrefix: string): TDextNatsServiceGroup; overload;
    /// <summary>Creates a subject-prefix group with queue override options.</summary>
    function AddGroup(const AConfig: TNatsGroupConfig): TDextNatsServiceGroup; overload;

    /// <summary>Unsubscribes discovery + endpoint subscriptions. Idempotent.</summary>
    procedure Stop;
    /// <summary>Resets per-endpoint counters and the started timestamp.</summary>
    procedure Reset;

    /// <summary>ADR-32 PING JSON (<c>io.nats.micro.v1.ping_response</c>).</summary>
    function PingJson: string;
    /// <summary>ADR-32 INFO JSON including endpoints.</summary>
    function InfoJson: string;
    /// <summary>ADR-32 STATS JSON including per-endpoint counters.</summary>
    function StatsJson: string;

    property Client: TDextNatsClient read FClient;
    property Name: string read FName;
    property Version: string read FVersion;
    property Description: string read FDescription;
    property Id: string read FId;
    property Stopped: Boolean read GetStopped;
  end;

/// <summary>Builds <c>$SRV.&lt;VERB&gt;[.&lt;name&gt;[.&lt;id&gt;]]</c>. Raises when id is set without name.</summary>
function NatsServiceControlSubject(AVerb: TNatsServiceVerb; const AName: string = '';
  const AId: string = ''): string;
/// <summary>PING / STATS / INFO token for AVerb.</summary>
function NatsServiceVerbToString(AVerb: TNatsServiceVerb): string;
/// <summary>True when AName matches ADR-32 service/endpoint name rules.</summary>
function NatsServiceIsValidName(const AName: string): Boolean;
/// <summary>True when AVersion matches the official SemVer regex (ADR-32).</summary>
function NatsServiceIsValidSemVer(const AVersion: string): Boolean;
/// <summary>True when ASubject is a valid endpoint/queue subject (no spaces; optional trailing &gt;).</summary>
function NatsServiceIsValidSubject(const ASubject: string): Boolean;
/// <summary>Joins group prefix and endpoint subject with <c>.</c> (empty prefix returns ASubject).</summary>
function NatsServiceJoinSubject(const APrefix, ASubject: string): string;
/// <summary>Generates a unique service instance id (GUID hex, no dashes).</summary>
function NatsServiceNewId: string;

implementation

uses
  System.Diagnostics,
  System.DateUtils,
  System.RegularExpressions,
  Dext.Json.Utf8;

const
  CSemVerPattern =
    '^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)' +
    '(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?' +
    '(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$';

type
  PSrvByteWriter = ^TSrvByteWriter;
  TSrvByteWriter = record
  private
    FBuf: TBytes;
    FLen: Integer;
  public
    procedure Reset;
    procedure EnsureCapacity(ANeeded: Integer);
    procedure WriteBytes(AData: Pointer; ALength: Integer);
    function ToBytes: TBytes;
  end;

procedure SrvUtf8WriteToByteWriter(AContext, AData: Pointer; ALength: Integer);
begin
  if (ALength > 0) and (AContext <> nil) then
    PSrvByteWriter(AContext)^.WriteBytes(AData, ALength);
end;

procedure TSrvByteWriter.Reset;
begin
  FLen := 0;
end;

procedure TSrvByteWriter.EnsureCapacity(ANeeded: Integer);
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

procedure TSrvByteWriter.WriteBytes(AData: Pointer; ALength: Integer);
begin
  if (ALength <= 0) or (AData = nil) then
    Exit;
  EnsureCapacity(ALength);
  Move(AData^, FBuf[FLen], ALength);
  Inc(FLen, ALength);
end;

function TSrvByteWriter.ToBytes: TBytes;
begin
  SetLength(Result, FLen);
  if FLen > 0 then
    Move(FBuf[0], Result[0], FLen);
end;

function NatsServiceVerbToString(AVerb: TNatsServiceVerb): string;
begin
  case AVerb of
    svPing: Result := 'PING';
    svStats: Result := 'STATS';
    svInfo: Result := 'INFO';
  else
    Result := '';
  end;
end;

function NatsServiceControlSubject(AVerb: TNatsServiceVerb; const AName, AId: string): string;
var
  verb: string;
begin
  verb := NatsServiceVerbToString(AVerb);
  if verb = '' then
    raise EDextNatsServiceError.Create('Unsupported service verb');
  if (AName = '') and (AId <> '') then
    raise EDextNatsServiceError.Create('Service name is required to generate an ID control subject');
  if (AName = '') and (AId = '') then
    Result := NATS_SRV_PREFIX + '.' + verb
  else if AId = '' then
    Result := NATS_SRV_PREFIX + '.' + verb + '.' + AName
  else
    Result := NATS_SRV_PREFIX + '.' + verb + '.' + AName + '.' + AId;
end;

function NatsServiceIsValidName(const AName: string): Boolean;
var
  i: Integer;
  c: Char;
begin
  if AName = '' then
    Exit(False);
  for i := 1 to Length(AName) do
  begin
    c := AName[i];
    if not (((c >= 'A') and (c <= 'Z')) or ((c >= 'a') and (c <= 'z')) or
      ((c >= '0') and (c <= '9')) or (c = '-') or (c = '_')) then
      Exit(False);
  end;
  Result := True;
end;

function NatsServiceIsValidSemVer(const AVersion: string): Boolean;
begin
  Result := (AVersion <> '') and TRegEx.IsMatch(AVersion, CSemVerPattern);
end;

function NatsServiceIsValidSubject(const ASubject: string): Boolean;
var
  i: Integer;
  c: Char;
begin
  if ASubject = '' then
    Exit(False);
  for i := 1 to Length(ASubject) do
  begin
    c := ASubject[i];
    if (c = ' ') then
      Exit(False);
    if (c = '>') and (i <> Length(ASubject)) then
      Exit(False);
  end;
  Result := True;
end;

function NatsServiceJoinSubject(const APrefix, ASubject: string): string;
begin
  if APrefix = '' then
    Result := ASubject
  else if ASubject = '' then
    Result := APrefix
  else
    Result := APrefix + '.' + ASubject;
end;

function NatsServiceNewId: string;
var
  guid: TGUID;
  raw: string;
begin
  CreateGUID(guid);
  raw := GUIDToString(guid);
  raw := StringReplace(raw, '{', '', [rfReplaceAll]);
  raw := StringReplace(raw, '}', '', [rfReplaceAll]);
  raw := StringReplace(raw, '-', '', [rfReplaceAll]);
  Result := LowerCase(raw);
end;

procedure SrvWriteMetadataObject(var AWriter: TUtf8JsonWriter;
  const AMetadata: IDictionary<string, string>);
var
  keys: TArray<string>;
  i: Integer;
begin
  AWriter.WritePropertyName('metadata');
  AWriter.WriteStartObject;
  if (AMetadata <> nil) and (AMetadata.Count > 0) then
  begin
    keys := AMetadata.Keys;
    for i := 0 to High(keys) do
    begin
      AWriter.WritePropertyName(keys[i]);
      AWriter.WriteString(AMetadata[keys[i]]);
    end;
  end;
  AWriter.WriteEndObject;
end;

procedure SrvWriteIdentity(var AWriter: TUtf8JsonWriter; const AName, AId, AVersion: string;
  const AMetadata: IDictionary<string, string>);
begin
  AWriter.WritePropertyName('name');
  AWriter.WriteString(AName);
  AWriter.WritePropertyName('id');
  AWriter.WriteString(AId);
  AWriter.WritePropertyName('version');
  AWriter.WriteString(AVersion);
  SrvWriteMetadataObject(AWriter, AMetadata);
end;

{ TNatsServiceConfig }

class function TNatsServiceConfig.CreateDefault(const AName, AVersion: string): TNatsServiceConfig;
begin
  Result := Default(TNatsServiceConfig);
  Result.Name := AName;
  Result.Version := AVersion;
  Result.Description := '';
  Result.Metadata := nil;
  Result.QueueGroup := NATS_SRV_DEFAULT_QUEUE;
  Result.QueueGroupDisabled := False;
end;

procedure TNatsServiceConfig.Validate;
begin
  if not NatsServiceIsValidName(Name) then
    raise EDextNatsServiceError.Create(
      'Service name must be non-empty and consist of alphanumerical characters, dashes and underscores');
  if not NatsServiceIsValidSemVer(Version) then
    raise EDextNatsServiceError.Create('Service version must be a valid SemVer string');
  if (QueueGroup <> '') and (not NatsServiceIsValidSubject(QueueGroup)) then
    raise EDextNatsServiceError.Create('Invalid service queue group');
end;

{ TNatsEndpointConfig }

class function TNatsEndpointConfig.CreateDefault(const AName, ASubject: string): TNatsEndpointConfig;
begin
  Result := Default(TNatsEndpointConfig);
  Result.Name := AName;
  if ASubject <> '' then
    Result.Subject := ASubject
  else
    Result.Subject := AName;
  Result.QueueGroup := '';
  Result.QueueGroupDisabled := False;
  Result.Metadata := nil;
end;

procedure TNatsEndpointConfig.Validate;
var
  subj: string;
begin
  if not NatsServiceIsValidName(Name) then
    raise EDextNatsServiceError.Create('Invalid endpoint name');
  subj := Subject;
  if subj = '' then
    subj := Name;
  if not NatsServiceIsValidSubject(subj) then
    raise EDextNatsServiceError.Create('Invalid endpoint subject');
  if (QueueGroup <> '') and (not NatsServiceIsValidSubject(QueueGroup)) then
    raise EDextNatsServiceError.Create('Invalid endpoint queue group');
end;

{ TNatsGroupConfig }

class function TNatsGroupConfig.CreateDefault(const APrefix: string): TNatsGroupConfig;
begin
  Result := Default(TNatsGroupConfig);
  Result.Prefix := APrefix;
  Result.QueueGroup := '';
  Result.QueueGroupDisabled := False;
end;

procedure TNatsGroupConfig.Validate;
begin
  if (Prefix <> '') and (not NatsServiceIsValidSubject(Prefix)) then
    raise EDextNatsServiceError.Create('Invalid group subject prefix');
  if (QueueGroup <> '') and (not NatsServiceIsValidSubject(QueueGroup)) then
    raise EDextNatsServiceError.Create('Invalid group queue group');
end;

{ TNatsServiceRequest }

constructor TNatsServiceRequest.Create(AClient: TDextNatsClient; const AMsg: TNatsMsg);
begin
  inherited Create;
  FClient := AClient;
  FMsg := AMsg;
  FResponded := False;
  FErrorDescription := '';
end;

function TNatsServiceRequest.GetSubject: string;
begin
  Result := FMsg.Subject;
end;

function TNatsServiceRequest.GetReplyTo: string;
begin
  Result := FMsg.ReplyTo;
end;

function TNatsServiceRequest.GetData: TBytes;
begin
  Result := FMsg.Payload;
end;

function TNatsServiceRequest.GetHeaders: TNatsHeaders;
begin
  Result := FMsg.Headers;
end;

function TNatsServiceRequest.AsString: string;
begin
  Result := FMsg.AsString;
end;

procedure TNatsServiceRequest.Respond(const AData: TBytes);
begin
  if FResponded then
    raise EDextNatsServiceError.Create('Service request already responded');
  if not FMsg.HasReplyTo then
    raise EDextNatsServiceError.Create('Service request has no reply subject');
  FClient.Publish(FMsg.ReplyTo, AData);
  FResponded := True;
end;

procedure TNatsServiceRequest.Respond(const AMessage: string);
begin
  Respond(TEncoding.UTF8.GetBytes(AMessage));
end;

procedure TNatsServiceRequest.RespondError(ACode: Integer; const ADescription: string;
  const AData: TBytes);
var
  headers: TNatsHeaders;
begin
  if FResponded then
    raise EDextNatsServiceError.Create('Service request already responded');
  if not FMsg.HasReplyTo then
    raise EDextNatsServiceError.Create('Service request has no reply subject');
  headers := nil;
  headers.Add(NATS_SRV_ERROR_CODE_HEADER, IntToStr(ACode));
  headers.Add(NATS_SRV_ERROR_HEADER, ADescription);
  FClient.PublishWithHeaders(FMsg.ReplyTo, AData, headers);
  FResponded := True;
  FErrorDescription := ADescription;
end;

procedure TNatsServiceRequest.RespondError(ACode: Integer; const ADescription: string);
begin
  RespondError(ACode, ADescription, nil);
end;

{ TDextNatsService }

class function TDextNatsService.AddService(AClient: TDextNatsClient;
  const AConfig: TNatsServiceConfig): TDextNatsService;
begin
  Result := TDextNatsService.Create(AClient, AConfig);
end;

constructor TDextNatsService.Create(AClient: TDextNatsClient; const AConfig: TNatsServiceConfig);
var
  cfg: TNatsServiceConfig;
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsServiceError.Create('AddService requires a non-nil client');
  cfg := AConfig;
  cfg.Validate;

  FClient := AClient;
  FLock := TCriticalSection.Create;
  FName := cfg.Name;
  FVersion := cfg.Version;
  FDescription := cfg.Description;
  FId := NatsServiceNewId;
  if cfg.Metadata <> nil then
    FMetadata := cfg.Metadata
  else
    FMetadata := TCollections.CreateDictionary<string, string>;
  FQueueGroupDisabled := cfg.QueueGroupDisabled;
  if cfg.QueueGroup <> '' then
    FQueueGroup := cfg.QueueGroup
  else
    FQueueGroup := NATS_SRV_DEFAULT_QUEUE;
  FEndpoints := TCollections.CreateList<TEndpointState>;
  FGroups := TCollections.CreateList<TDextNatsServiceGroup>;
  FDiscoverySids := TCollections.CreateList<Integer>;
  FStopped := False;
  FStartedUtc := Now;
  SubscribeDiscovery;
end;

destructor TDextNatsService.Destroy;
var
  i: Integer;
begin
  try
    Stop;
  except
    { best-effort cleanup }
  end;
  if FLock <> nil then
  begin
    FLock.Enter;
    try
      if FEndpoints <> nil then
      begin
        for i := 0 to FEndpoints.Count - 1 do
          FEndpoints[i].Free;
        FEndpoints.Clear;
      end;
      if FGroups <> nil then
      begin
        for i := 0 to FGroups.Count - 1 do
          FGroups[i].Free;
        FGroups.Clear;
      end;
    finally
      FLock.Leave;
    end;
  end;
  FreeAndNil(FLock);
  inherited Destroy;
end;

function TDextNatsService.GetStopped: Boolean;
begin
  FLock.Enter;
  try
    Result := FStopped;
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsService.EnsureRunning;
begin
  if FStopped then
    raise EDextNatsServiceError.Create('Service is stopped');
end;

function TDextNatsService.ResolveQueueGroup(const ACustomQueue: string;
  ACustomDisabled: Boolean; const AParentQueue: string;
  AParentDisabled: Boolean): string;
var
  disabled: Boolean;
begin
  ResolveQueueGroupEx(ACustomQueue, ACustomDisabled, AParentQueue, AParentDisabled,
    Result, disabled);
end;

procedure TDextNatsService.ResolveQueueGroupEx(const ACustomQueue: string;
  ACustomDisabled: Boolean; const AParentQueue: string; AParentDisabled: Boolean;
  out AQueue: string; out ADisabled: Boolean);
begin
  if ACustomDisabled then
  begin
    AQueue := '';
    ADisabled := True;
    Exit;
  end;
  if ACustomQueue <> '' then
  begin
    AQueue := ACustomQueue;
    ADisabled := False;
    Exit;
  end;
  if AParentDisabled then
  begin
    AQueue := '';
    ADisabled := True;
    Exit;
  end;
  if AParentQueue <> '' then
  begin
    AQueue := AParentQueue;
    ADisabled := False;
    Exit;
  end;
  AQueue := NATS_SRV_DEFAULT_QUEUE;
  ADisabled := False;
end;

procedure TDextNatsService.SubscribeDiscoverySubject(AVerb: TNatsServiceVerb;
  const ASubject: string);
var
  verb: TNatsServiceVerb;
  sid: Integer;
begin
  { Nested frame so each closure captures its own verb value (not the loop variable). }
  verb := AVerb;
  sid := FClient.Subscribe(ASubject,
    procedure(const AMsg: TNatsMsg)
    begin
      HandleDiscovery(verb, AMsg);
    end);
  FDiscoverySids.Add(sid);
end;

procedure TDextNatsService.SubscribeDiscovery;
var
  verb: TNatsServiceVerb;
begin
  for verb := Low(TNatsServiceVerb) to High(TNatsServiceVerb) do
  begin
    SubscribeDiscoverySubject(verb, NatsServiceControlSubject(verb));
    SubscribeDiscoverySubject(verb, NatsServiceControlSubject(verb, FName));
    SubscribeDiscoverySubject(verb, NatsServiceControlSubject(verb, FName, FId));
  end;
end;

procedure TDextNatsService.HandleDiscovery(AVerb: TNatsServiceVerb; const AMsg: TNatsMsg);
var
  json: string;
begin
  FLock.Enter;
  try
    if FStopped then
      Exit;
  finally
    FLock.Leave;
  end;

  if not AMsg.HasReplyTo then
    Exit;

  case AVerb of
    svPing:
      json := BuildPingJson;
    svInfo:
      json := BuildInfoJson;
    svStats:
      json := BuildStatsJson;
  else
    Exit;
  end;

  try
    FClient.Publish(AMsg.ReplyTo, TEncoding.UTF8.GetBytes(json));
  except
    { discovery best-effort }
  end;
end;

procedure TDextNatsService.HandleEndpoint(AEndpoint: TEndpointState; const AMsg: TNatsMsg);
var
  req: TNatsServiceRequest;
  sw: TStopwatch;
  handler: TNatsServiceHandler;
  elapsed: Int64;
  hadException: Boolean;
  exceptMsg: string;
begin
  FLock.Enter;
  try
    if FStopped then
      Exit;
    handler := AEndpoint.Handler;
  finally
    FLock.Leave;
  end;

  if not Assigned(handler) then
    Exit;

  hadException := False;
  exceptMsg := '';
  req := TNatsServiceRequest.Create(FClient, AMsg);
  sw := TStopwatch.StartNew;
  try
    try
      handler(req);
    except
      on E: Exception do
      begin
        hadException := True;
        exceptMsg := E.Message;
        if (not req.Responded) and AMsg.HasReplyTo then
        begin
          try
            req.RespondError(500, E.Message);
          except
            { ignore secondary failures }
          end;
        end;
      end;
    end;
  finally
    sw.Stop;
    elapsed := sw.Elapsed.Ticks * 100; { Ticks = 100ns → nanoseconds }
    FLock.Enter;
    try
      Inc(AEndpoint.NumRequests);
      Inc(AEndpoint.ProcessingTimeNanos, elapsed);
      if req.ErrorDescription <> '' then
      begin
        Inc(AEndpoint.NumErrors);
        AEndpoint.LastError := req.ErrorDescription;
      end
      else if hadException then
      begin
        Inc(AEndpoint.NumErrors);
        AEndpoint.LastError := exceptMsg;
      end;
    finally
      FLock.Leave;
    end;
    req.Free;
  end;
end;

procedure TDextNatsService.AddEndpoint(const AName: string; const AHandler: TNatsServiceHandler);
var
  cfg: TNatsEndpointConfig;
begin
  cfg := TNatsEndpointConfig.CreateDefault(AName);
  AddEndpoint(cfg, AHandler);
end;

procedure TDextNatsService.AddEndpoint(const AConfig: TNatsEndpointConfig;
  const AHandler: TNatsServiceHandler);
begin
  AddEndpointInternal(AConfig, AHandler, '', FQueueGroup, FQueueGroupDisabled);
end;

function TDextNatsService.AddGroup(const APrefix: string): TDextNatsServiceGroup;
begin
  Result := AddGroup(TNatsGroupConfig.CreateDefault(APrefix));
end;

function TDextNatsService.AddGroup(const AConfig: TNatsGroupConfig): TDextNatsServiceGroup;
begin
  Result := CreateGroup(AConfig, FQueueGroup, FQueueGroupDisabled);
end;

function TDextNatsService.CreateGroup(const AConfig: TNatsGroupConfig;
  const AParentQueue: string; AParentQueueDisabled: Boolean): TDextNatsServiceGroup;
var
  cfg: TNatsGroupConfig;
  queue: string;
  disabled: Boolean;
begin
  cfg := AConfig;
  cfg.Validate;

  FLock.Enter;
  try
    EnsureRunning;
    ResolveQueueGroupEx(cfg.QueueGroup, cfg.QueueGroupDisabled,
      AParentQueue, AParentQueueDisabled, queue, disabled);
    Result := TDextNatsServiceGroup.Create(Self, cfg.Prefix, queue, disabled);
    FGroups.Add(Result);
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsService.AddEndpointInternal(const AConfig: TNatsEndpointConfig;
  const AHandler: TNatsServiceHandler; const ASubjectPrefix: string;
  const AParentQueue: string; AParentQueueDisabled: Boolean);
var
  cfg: TNatsEndpointConfig;
  ep: TEndpointState;
  queue: string;
  subject: string;
  sid: Integer;
  abandon: Boolean;
begin
  if not Assigned(AHandler) then
    raise EDextNatsServiceError.Create('AddEndpoint requires a handler');
  cfg := AConfig;
  cfg.Validate;

  subject := cfg.Subject;
  if subject = '' then
    subject := cfg.Name;
  subject := NatsServiceJoinSubject(ASubjectPrefix, subject);
  if not NatsServiceIsValidSubject(subject) then
    raise EDextNatsServiceError.Create('Invalid endpoint subject');

  FLock.Enter;
  try
    EnsureRunning;
    queue := ResolveQueueGroup(cfg.QueueGroup, cfg.QueueGroupDisabled,
      AParentQueue, AParentQueueDisabled);
    ep := TEndpointState.Create;
    ep.Name := cfg.Name;
    ep.Subject := subject;
    ep.QueueGroup := queue;
    if cfg.Metadata <> nil then
      ep.Metadata := cfg.Metadata
    else
      ep.Metadata := TCollections.CreateDictionary<string, string>;
    ep.Handler := AHandler;
    ep.NumRequests := 0;
    ep.NumErrors := 0;
    ep.LastError := '';
    ep.ProcessingTimeNanos := 0;
  finally
    FLock.Leave;
  end;

  try
    sid := FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        HandleEndpoint(ep, AMsg);
      end, queue);
  except
    ep.Free;
    raise;
  end;

  abandon := False;
  FLock.Enter;
  try
    if FStopped then
      abandon := True
    else
    begin
      ep.Sid := sid;
      FEndpoints.Add(ep);
    end;
  finally
    FLock.Leave;
  end;

  if abandon then
  begin
    try
      FClient.Unsubscribe(sid);
    except
    end;
    ep.Free;
    raise EDextNatsServiceError.Create('Service is stopped');
  end;
end;

{ TDextNatsServiceGroup }

constructor TDextNatsServiceGroup.Create(AService: TDextNatsService;
  const APrefix, AQueueGroup: string; AQueueGroupDisabled: Boolean);
begin
  inherited Create;
  FService := AService;
  FPrefix := APrefix;
  FQueueGroup := AQueueGroup;
  FQueueGroupDisabled := AQueueGroupDisabled;
end;

function TDextNatsServiceGroup.AddGroup(const APrefix: string): TDextNatsServiceGroup;
begin
  Result := AddGroup(TNatsGroupConfig.CreateDefault(APrefix));
end;

function TDextNatsServiceGroup.AddGroup(const AConfig: TNatsGroupConfig): TDextNatsServiceGroup;
var
  cfg: TNatsGroupConfig;
begin
  cfg := AConfig;
  cfg.Prefix := NatsServiceJoinSubject(FPrefix, cfg.Prefix);
  Result := FService.CreateGroup(cfg, FQueueGroup, FQueueGroupDisabled);
end;

procedure TDextNatsServiceGroup.AddEndpoint(const AName: string;
  const AHandler: TNatsServiceHandler);
var
  cfg: TNatsEndpointConfig;
begin
  cfg := TNatsEndpointConfig.CreateDefault(AName);
  AddEndpoint(cfg, AHandler);
end;

procedure TDextNatsServiceGroup.AddEndpoint(const AConfig: TNatsEndpointConfig;
  const AHandler: TNatsServiceHandler);
begin
  FService.AddEndpointInternal(AConfig, AHandler, FPrefix, FQueueGroup,
    FQueueGroupDisabled);
end;

procedure TDextNatsService.Stop;
var
  i: Integer;
  sids: TArray<Integer>;
  n: Integer;
begin
  FLock.Enter;
  try
    if FStopped then
      Exit;
    FStopped := True;

    n := FDiscoverySids.Count + FEndpoints.Count;
    SetLength(sids, n);
    n := 0;
    for i := 0 to FDiscoverySids.Count - 1 do
    begin
      sids[n] := FDiscoverySids[i];
      Inc(n);
    end;
    for i := 0 to FEndpoints.Count - 1 do
    begin
      sids[n] := FEndpoints[i].Sid;
      Inc(n);
      { Drop handler so a late in-flight MSG cannot invoke user code after Stop. }
      FEndpoints[i].Handler := nil;
    end;
    FDiscoverySids.Clear;
  finally
    FLock.Leave;
  end;

  for i := 0 to High(sids) do
  begin
    try
      FClient.Unsubscribe(sids[i]);
    except
      { connection may already be closed }
    end;
  end;
end;

procedure TDextNatsService.Reset;
var
  i: Integer;
begin
  FLock.Enter;
  try
    EnsureRunning;
    for i := 0 to FEndpoints.Count - 1 do
    begin
      FEndpoints[i].NumRequests := 0;
      FEndpoints[i].NumErrors := 0;
      FEndpoints[i].LastError := '';
      FEndpoints[i].ProcessingTimeNanos := 0;
    end;
    FStartedUtc := Now;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsService.BuildPingJson: string;
var
  w: TSrvByteWriter;
  jw: TUtf8JsonWriter;
begin
  w.Reset;
  jw := TUtf8JsonWriter.Create(@w, SrvUtf8WriteToByteWriter, False);
  jw.WriteStartObject;
  SrvWriteIdentity(jw, FName, FId, FVersion, FMetadata);
  jw.WritePropertyName('type');
  jw.WriteString(NATS_SRV_PING_RESPONSE_TYPE);
  jw.WriteEndObject;
  Result := TEncoding.UTF8.GetString(w.ToBytes);
end;

function TDextNatsService.BuildInfoJson: string;
var
  w: TSrvByteWriter;
  jw: TUtf8JsonWriter;
  i: Integer;
  ep: TEndpointState;
begin
  FLock.Enter;
  try
    w.Reset;
    jw := TUtf8JsonWriter.Create(@w, SrvUtf8WriteToByteWriter, False);
    jw.WriteStartObject;
    SrvWriteIdentity(jw, FName, FId, FVersion, FMetadata);
    jw.WritePropertyName('type');
    jw.WriteString(NATS_SRV_INFO_RESPONSE_TYPE);
    jw.WritePropertyName('description');
    jw.WriteString(FDescription);
    jw.WritePropertyName('endpoints');
    jw.WriteStartArray;
    for i := 0 to FEndpoints.Count - 1 do
    begin
      ep := FEndpoints[i];
      jw.WriteStartObject;
      jw.WritePropertyName('name');
      jw.WriteString(ep.Name);
      jw.WritePropertyName('subject');
      jw.WriteString(ep.Subject);
      jw.WritePropertyName('queue_group');
      jw.WriteString(ep.QueueGroup);
      SrvWriteMetadataObject(jw, ep.Metadata);
      jw.WriteEndObject;
    end;
    jw.WriteEndArray;
    jw.WriteEndObject;
    Result := TEncoding.UTF8.GetString(w.ToBytes);
  finally
    FLock.Leave;
  end;
end;

function TDextNatsService.BuildStatsJson: string;
var
  w: TSrvByteWriter;
  jw: TUtf8JsonWriter;
  i: Integer;
  ep: TEndpointState;
  avg: Int64;
  started: string;
begin
  FLock.Enter;
  try
    started := DateToISO8601(FStartedUtc, True);
    w.Reset;
    jw := TUtf8JsonWriter.Create(@w, SrvUtf8WriteToByteWriter, False);
    jw.WriteStartObject;
    SrvWriteIdentity(jw, FName, FId, FVersion, FMetadata);
    jw.WritePropertyName('type');
    jw.WriteString(NATS_SRV_STATS_RESPONSE_TYPE);
    jw.WritePropertyName('started');
    jw.WriteString(started);
    jw.WritePropertyName('endpoints');
    jw.WriteStartArray;
    for i := 0 to FEndpoints.Count - 1 do
    begin
      ep := FEndpoints[i];
      if ep.NumRequests > 0 then
        avg := ep.ProcessingTimeNanos div ep.NumRequests
      else
        avg := 0;
      jw.WriteStartObject;
      jw.WritePropertyName('name');
      jw.WriteString(ep.Name);
      jw.WritePropertyName('subject');
      jw.WriteString(ep.Subject);
      jw.WritePropertyName('queue_group');
      jw.WriteString(ep.QueueGroup);
      jw.WritePropertyName('num_requests');
      jw.WriteNumber(ep.NumRequests);
      jw.WritePropertyName('num_errors');
      jw.WriteNumber(ep.NumErrors);
      jw.WritePropertyName('last_error');
      jw.WriteString(ep.LastError);
      jw.WritePropertyName('processing_time');
      jw.WriteNumber(ep.ProcessingTimeNanos);
      jw.WritePropertyName('average_processing_time');
      jw.WriteNumber(avg);
      jw.WriteEndObject;
    end;
    jw.WriteEndArray;
    jw.WriteEndObject;
    Result := TEncoding.UTF8.GetString(w.ToBytes);
  finally
    FLock.Leave;
  end;
end;

function TDextNatsService.PingJson: string;
begin
  Result := BuildPingJson;
end;

function TDextNatsService.InfoJson: string;
begin
  Result := BuildInfoJson;
end;

function TDextNatsService.StatsJson: string;
begin
  Result := BuildStatsJson;
end;

end.
