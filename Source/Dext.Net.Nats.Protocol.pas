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
{  Wire protocol layer for the NATS client: constants, INFO/CONNECT         }
{  payloads, headers and protocol frame encoders.                          }
{  This unit performs no I/O; see Dext.Net.Nats for the socket-facing        }
{  client built on top of it.                                               }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Protocol;

interface

uses
  System.SysUtils,
  Dext.Collections.Dict,
  Dext.Collections,
  Dext.Core.Span,
  Dext.Json.Utf8;

const
  NATS_DEFAULT_PORT = 4222;
  NATS_INBOX_PREFIX = '_INBOX.';
  NATS_HEADER_VERSION = 'NATS/1.0';
  NATS_CRLF = #13#10;
  NATS_LANG = 'Delphi';
  NATS_CLIENT_VERSION = '1.0.0';
  /// <summary>Hard ceiling for a single MSG/HMSG payload, guarding against a
  /// corrupt stream or a malicious server declaring an unbounded size.</summary>
  NATS_MAX_FRAME_BYTES = 64 * 1024 * 1024;

type
  /// <summary>Base exception for all Dext.Nats errors.</summary>
  EDextNatsException = class(Exception);
  /// <summary>Raised when a byte sequence does not conform to the NATS text protocol.</summary>
  EDextNatsProtocolError = class(EDextNatsException);
  /// <summary>Raised when a blocking wait (connect, request, flush) exceeds its deadline.</summary>
  EDextNatsTimeoutError = class(EDextNatsException);
  /// <summary>Raised when the server reports a -ERR during the connection handshake.</summary>
  EDextNatsServerError = class(EDextNatsException);
  /// <summary>Raised when the requested operation needs a TLS handshake, which is not yet implemented.</summary>
  EDextNatsNotSupported = class(EDextNatsException);
  /// <summary>Raised when a request/reply gets an inline 503 "no responders" status from the server.</summary>
  EDextNatsNoResponders = class(EDextNatsException);

  /// <summary>A single NATS message header (name/value pair).</summary>
  TNatsHeader = TPair<string, string>;
  /// <summary>An ordered collection of NATS message headers.</summary>
  TNatsHeaders = TArray<TNatsHeader>;

  /// <summary>Convenience helpers for building and reading <see cref="TNatsHeaders"/>.</summary>
  TNatsHeadersHelper = record helper for TNatsHeaders
    /// <summary>Appends a new header, allowing duplicate names.</summary>
    procedure Add(const AName, AValue: string);
    /// <summary>Returns the value of the first header matching AName, or ADefault.</summary>
    function GetValue(const AName: string; const ADefault: string = ''): string;
    /// <summary>Adds AName if missing, otherwise overwrites the first occurrence.</summary>
    procedure SetValue(const AName, AValue: string);
    /// <summary>Returns the index of the first header matching AName, or -1.</summary>
    function IndexOf(const AName: string): Integer;
    /// <summary>Number of headers currently stored.</summary>
    function Count: Integer;
    /// <summary>Encodes the headers as a NATS header block (including the NATS/1.0 line
    /// and the terminating blank line), ready to be sent right after an HPUB control line.</summary>
    function Encode: TBytes;
  end;

  /// <summary>
  ///   Fields advertised by the server in INFO (initial handshake and async topology
  ///   refreshes). Only wire keys from the NATS client protocol / server Info JSON —
  ///   no invented account-limit objects. Snapshot is exposed as
  ///   <c>TDextNatsClient.ServerInfo</c>.
  /// </summary>
  TNatsServerInfo = record
    ServerId: string;
    ServerName: string;
    Version: string;
    Proto: Integer;
    GoVersion: string;
    /// <summary>Build hash from INFO <c>git_commit</c> (optional).</summary>
    GitCommit: string;
    Host: string;
    Port: Integer;
    /// <summary>Server-advertised IP from INFO <c>ip</c> (optional; often route-oriented).</summary>
    Ip: string;
    HeadersSupported: Boolean;
    /// <summary>Server max payload bytes (<c>max_payload</c>) — primary size limit on INFO.</summary>
    MaxPayload: Int64;
    ClientId: Int64;
    ClientIp: string;
    AuthRequired: Boolean;
    TlsRequired: Boolean;
    /// <summary>INFO <c>tls_verify</c> — client cert required when true.</summary>
    TlsVerify: Boolean;
    TlsAvailable: Boolean;
    Jetstream: Boolean;
    /// <summary>JetStream API level from INFO <c>api_lvl</c> (0 when absent).</summary>
    JsApiLevel: Integer;
    Nonce: string;
    /// <summary>Cluster name from INFO <c>cluster</c>.</summary>
    Cluster: string;
    /// <summary>INFO <c>cluster_dynamic</c>.</summary>
    ClusterDynamic: Boolean;
    /// <summary>Configured NATS / JetStream domain (<c>domain</c>) when set.</summary>
    Domain: string;
    /// <summary>Remote account name this connection binds to (<c>remote_account</c>).</summary>
    RemoteAccount: string;
    /// <summary>True when the bound account is the system account (<c>acc_is_sys</c>).</summary>
    IsSystemAccount: Boolean;
    /// <summary>Lame Duck Mode (<c>ldm</c>) — server draining; clients should migrate.</summary>
    LameDuckMode: Boolean;
    ConnectUrls: TArray<string>;
    /// <summary>WebSocket connect URLs from INFO <c>ws_connect_urls</c>.</summary>
    WsConnectUrls: TArray<string>;
    /// <summary>Parses a server INFO JSON payload (without the leading "INFO " token).</summary>
    class function Parse(const AJson: string): TNatsServerInfo; static;
  end;

  /// <summary>Fields sent to the server in the client CONNECT message.</summary>
  TNatsConnectOptions = record
    Verbose: Boolean;
    Pedantic: Boolean;
    TlsRequired: Boolean;
    AuthToken: string;
    User: string;
    Password: string;
    Name: string;
    Lang: string;
    Version: string;
    Protocol: Integer;
    Echo: Boolean;
    Headers: Boolean;
    NoResponders: Boolean;
    JWT: string;
    /// <summary>Public NKey (e.g. <c>U…</c>) for bare NKey auth; empty when using JWT credentials.</summary>
    Nkey: string;
    Sig: string;
    /// <summary>Returns sensible defaults: Delphi client identity, protocol 1, headers, echo and no_responders enabled.</summary>
    class function CreateDefault: TNatsConnectOptions; static;
    /// <summary>Serializes the record to the JSON payload expected after the CONNECT keyword.</summary>
    function ToJson: string;
  end;

  /// <summary>Kind of a decoded server-to-client protocol frame.</summary>
  TNatsFrameKind = (nfNone, nfInfo, nfMsg, nfHMsg, nfPing, nfPong, nfOK, nfErr);

  /// <summary>A fully decoded server-to-client protocol frame.</summary>
  TNatsFrame = record
    Kind: TNatsFrameKind;
    Subject: string;
    ReplyTo: string;
    Sid: Integer;
    Payload: TBytes;
    Headers: TNatsHeaders;
    /// <summary>Inline status code parsed from the header block first line (e.g. 503), 0 if none.</summary>
    StatusCode: Integer;
    InfoJson: string;
    ErrorText: string;
  end;

/// <summary>Generates a process-unique inbox subject, e.g. "_INBOX.3f2c9e1a...".</summary>
function NatsNewInbox: string;

/// <summary>Renders a Boolean as the lowercase JSON literal "true"/"false".</summary>
function NatsBoolStr(AValue: Boolean): string;
/// <summary>Escapes a string for safe embedding inside a JSON string literal.</summary>
function NatsJsonEscape(const S: string): string;

/// <summary>Parses a decoded NATS header block (NATS/1.0 status line + name: value lines).</summary>
procedure NatsParseHeaderBlock(const ABlock: string; out AHeaders: TNatsHeaders; out AStatusCode: Integer);

implementation

type
  /// <summary>Growable byte sink used by frame encoders and CONNECT JSON (implementation only).</summary>
  PNatsByteWriter = ^TNatsByteWriter;
  TNatsByteWriter = record
  private
    FBuf: TBytes;
    FLen: Integer;
  public
    procedure Reset;
    procedure EnsureCapacity(ANeeded: Integer);
    procedure WriteByte(AValue: Byte); inline;
    procedure WriteBytes(AData: Pointer; ALength: Integer); overload;
    procedure WriteBytes(const AData: TBytes); overload;
    procedure WriteAscii(const S: string);
    procedure WriteUtf8(const S: string);
    procedure WriteCrLf; inline;
    procedure WriteIntDec(AValue: Integer);
    function ToBytes: TBytes;
  end;

{ TNatsByteWriter }

procedure TNatsByteWriter.Reset;
begin
  FLen := 0;
end;

procedure TNatsByteWriter.EnsureCapacity(ANeeded: Integer);
var
  cap: Integer;
begin
  if ANeeded <= Length(FBuf) then
    Exit;
  // Geometric / next power-of-two capacity (avoids +N linear realloc churn).
  cap := Length(FBuf);
  if cap = 0 then
    cap := 256;
  while cap < ANeeded do
  begin
    if cap > (MaxInt div 2) then
    begin
      cap := ANeeded;
      Break;
    end;
    cap := cap * 2;
  end;
  SetLength(FBuf, cap);
end;

procedure TNatsByteWriter.WriteByte(AValue: Byte);
begin
  EnsureCapacity(FLen + 1);
  FBuf[FLen] := AValue;
  Inc(FLen);
end;

procedure TNatsByteWriter.WriteBytes(AData: Pointer; ALength: Integer);
begin
  if (ALength <= 0) or (AData = nil) then
    Exit;
  EnsureCapacity(FLen + ALength);
  Move(AData^, FBuf[FLen], ALength);
  Inc(FLen, ALength);
end;

procedure TNatsByteWriter.WriteBytes(const AData: TBytes);
begin
  if Length(AData) = 0 then
    Exit;
  WriteBytes(@AData[0], Length(AData));
end;

procedure TNatsByteWriter.WriteAscii(const S: string);
var
  i, n: Integer;
  p: PByte;
begin
  n := Length(S);
  if n = 0 then
    Exit;
  for i := 1 to n do
    if Ord(S[i]) > 127 then
    begin
      // Fall back to UTF-8 for non-ASCII (subjects are normally ASCII).
      WriteUtf8(S);
      Exit;
    end;
  EnsureCapacity(FLen + n);
  p := @FBuf[FLen];
  for i := 1 to n do
  begin
    p^ := Byte(Ord(S[i]));
    Inc(p);
  end;
  Inc(FLen, n);
end;

procedure TNatsByteWriter.WriteUtf8(const S: string);
var
  n, written: Integer;
  i: Integer;
  p: PByte;
  ascii: Boolean;
begin
  n := Length(S);
  if n = 0 then
    Exit;

  ascii := True;
  for i := 1 to n do
    if Ord(S[i]) > 127 then
    begin
      ascii := False;
      Break;
    end;

  if ascii then
  begin
    EnsureCapacity(FLen + n);
    p := @FBuf[FLen];
    for i := 1 to n do
    begin
      p^ := Byte(Ord(S[i]));
      Inc(p);
    end;
    Inc(FLen, n);
    Exit;
  end;

  written := TEncoding.UTF8.GetByteCount(S);
  EnsureCapacity(FLen + written);
  written := TEncoding.UTF8.GetBytes(S, 1, n, FBuf, FLen);
  Inc(FLen, written);
end;

procedure TNatsByteWriter.WriteCrLf;
begin
  EnsureCapacity(FLen + 2);
  FBuf[FLen] := 13;
  FBuf[FLen + 1] := 10;
  Inc(FLen, 2);
end;

procedure TNatsByteWriter.WriteIntDec(AValue: Integer);
var
  tmp: array[0..11] of Byte;
  i, n: Integer;
  v: Cardinal;
  neg: Boolean;
begin
  if AValue = 0 then
  begin
    WriteByte(Ord('0'));
    Exit;
  end;

  neg := AValue < 0;
  if AValue = Low(Integer) then
  begin
    WriteAscii('-2147483648');
    Exit;
  end;

  if neg then
    v := Cardinal(-AValue)
  else
    v := Cardinal(AValue);

  n := 0;
  while v > 0 do
  begin
    tmp[n] := Byte(Ord('0') + (v mod 10));
    v := v div 10;
    Inc(n);
  end;

  if neg then
    WriteByte(Ord('-'));
  for i := n - 1 downto 0 do
    WriteByte(tmp[i]);
end;

function TNatsByteWriter.ToBytes: TBytes;
begin
  SetLength(Result, FLen);
  if FLen > 0 then
    Move(FBuf[0], Result[0], FLen);
end;

procedure NatsUtf8WriteToByteWriter(AContext, AData: Pointer; ALength: Integer);
begin
  if (ALength > 0) and (AContext <> nil) then
    PNatsByteWriter(AContext)^.WriteBytes(AData, ALength);
end;

{ Small helpers }

function NatsBoolStr(AValue: Boolean): string;
begin
  if AValue then
    Result := 'true'
  else
    Result := 'false';
end;

function NatsJsonEscape(const S: string): string;
var
  I: Integer;
  C: Char;
begin
  Result := '';
  for I := 1 to Length(S) do
  begin
    C := S[I];
    case C of
      '"': Result := Result + '\"';
      '\': Result := Result + '\\';
      #8: Result := Result + '\b';
      #9: Result := Result + '\t';
      #10: Result := Result + '\n';
      #12: Result := Result + '\f';
      #13: Result := Result + '\r';
    else
      if Ord(C) < 32 then
        Result := Result + Format('\u%.4x', [Ord(C)])
      else
        Result := Result + C;
    end;
  end;
end;

function NatsNewInbox: string;
var
  guid: TGUID;
  raw: string;
begin
  CreateGUID(guid);
  raw := GUIDToString(guid);
  raw := StringReplace(raw, '{', '', [rfReplaceAll]);
  raw := StringReplace(raw, '}', '', [rfReplaceAll]);
  raw := StringReplace(raw, '-', '', [rfReplaceAll]);
  Result := NATS_INBOX_PREFIX + LowerCase(raw);
end;

procedure WriteConnectJson(const AOptions: TNatsConnectOptions; var AWriter: TNatsByteWriter);
var
  jw: TUtf8JsonWriter;
begin
  jw := TUtf8JsonWriter.Create(@AWriter, NatsUtf8WriteToByteWriter, False);
  jw.WriteStartObject;

  jw.WritePropertyName('verbose');
  jw.WriteBoolean(AOptions.Verbose);
  jw.WritePropertyName('pedantic');
  jw.WriteBoolean(AOptions.Pedantic);
  jw.WritePropertyName('tls_required');
  jw.WriteBoolean(AOptions.TlsRequired);

  if AOptions.AuthToken <> '' then
  begin
    jw.WritePropertyName('auth_token');
    jw.WriteString(AOptions.AuthToken);
  end;
  if AOptions.User <> '' then
  begin
    jw.WritePropertyName('user');
    jw.WriteString(AOptions.User);
  end;
  if AOptions.Password <> '' then
  begin
    jw.WritePropertyName('pass');
    jw.WriteString(AOptions.Password);
  end;
  if AOptions.JWT <> '' then
  begin
    jw.WritePropertyName('jwt');
    jw.WriteString(AOptions.JWT);
  end;
  if AOptions.Nkey <> '' then
  begin
    jw.WritePropertyName('nkey');
    jw.WriteString(AOptions.Nkey);
  end;
  if AOptions.Sig <> '' then
  begin
    jw.WritePropertyName('sig');
    jw.WriteString(AOptions.Sig);
  end;

  jw.WritePropertyName('name');
  jw.WriteString(AOptions.Name);
  jw.WritePropertyName('lang');
  jw.WriteString(AOptions.Lang);
  jw.WritePropertyName('version');
  jw.WriteString(AOptions.Version);
  jw.WritePropertyName('protocol');
  jw.WriteNumber(AOptions.Protocol);
  jw.WritePropertyName('echo');
  jw.WriteBoolean(AOptions.Echo);
  jw.WritePropertyName('headers');
  jw.WriteBoolean(AOptions.Headers);
  jw.WritePropertyName('no_responders');
  jw.WriteBoolean(AOptions.NoResponders);

  jw.WriteEndObject;
end;

{ TNatsHeadersHelper }

procedure TNatsHeadersHelper.Add(const AName, AValue: string);
begin
  Self := Self + [TNatsHeader.Create(AName, AValue)];
end;

function TNatsHeadersHelper.Count: Integer;
begin
  Result := Length(Self);
end;

function TNatsHeadersHelper.GetValue(const AName: string; const ADefault: string): string;
var
  idx: Integer;
begin
  idx := IndexOf(AName);
  if idx = -1 then
    Result := ADefault
  else
    Result := Self[idx].Value;
end;

function TNatsHeadersHelper.IndexOf(const AName: string): Integer;
var
  i: Integer;
begin
  for i := 0 to High(Self) do
    if SameText(Self[i].Key, AName) then
      Exit(i);
  Result := -1;
end;

procedure TNatsHeadersHelper.SetValue(const AName, AValue: string);
var
  idx: Integer;
begin
  idx := IndexOf(AName);
  if idx = -1 then
    Add(AName, AValue)
  else
    Self[idx] := TNatsHeader.Create(AName, AValue);
end;

function TNatsHeadersHelper.Encode: TBytes;
var
  s: string;
  h: TNatsHeader;
begin
  s := NATS_HEADER_VERSION + NATS_CRLF;
  for h in Self do
    s := s + h.Key + ': ' + h.Value + NATS_CRLF;
  s := s + NATS_CRLF;
  Result := TEncoding.UTF8.GetBytes(s);
end;

/// <summary>Parses a decoded HMSG header block (including the NATS/1.0 status line)
/// into individual headers plus an optional inline status code.</summary>
procedure NatsParseHeaderBlock(const ABlock: string; out AHeaders: TNatsHeaders; out AStatusCode: Integer);
var
  lines: TArray<string>;
  i, colonPos: Integer;
  firstLine, statusPart: string;
begin
  AHeaders := nil;
  AStatusCode := 0;
  if ABlock = '' then Exit;

  lines := ABlock.Split([NATS_CRLF]);
  if Length(lines) = 0 then Exit;

  firstLine := lines[0];
  if firstLine.StartsWith(NATS_HEADER_VERSION) then
  begin
    statusPart := Trim(Copy(firstLine, Length(NATS_HEADER_VERSION) + 1, MaxInt));
    if Length(statusPart) >= 3 then
      AStatusCode := StrToIntDef(Copy(statusPart, 1, 3), 0);
  end;

  for i := 1 to High(lines) do
  begin
    if Trim(lines[i]) = '' then Continue;
    colonPos := Pos(':', lines[i]);
    if colonPos > 0 then
      AHeaders.Add(Trim(Copy(lines[i], 1, colonPos - 1)), Trim(Copy(lines[i], colonPos + 1, MaxInt)));
  end;
end;

{ TNatsServerInfo }

class function TNatsServerInfo.Parse(const AJson: string): TNatsServerInfo;
var
  bytes: TBytes;
  span: TByteSpan;
  reader: TUtf8JsonReader;

  procedure ReadStringArray(var ADest: TArray<string>);
  var
    items: TArray<string>;
    count: Integer;
  begin
    count := 0;
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
            if count >= Length(items) then
              SetLength(items, count + 4);
            items[count] := reader.GetString;
            Inc(count);
          end
          else if reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
            reader.Skip;
        end;
      end
      else if reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
        reader.Skip;
    end;
    SetLength(items, count);
    ADest := items;
  end;

begin
  Result := Default(TNatsServerInfo);
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty INFO payload received from NATS server');

  bytes := TEncoding.UTF8.GetBytes(AJson);
  if Length(bytes) = 0 then
    raise EDextNatsProtocolError.Create('Empty INFO payload received from NATS server');

  span := TByteSpan.Create(@bytes[0], Length(bytes));
  try
    reader := TUtf8JsonReader.Create(span);
    if (not reader.Read) or (reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt(
        'Malformed INFO payload received from NATS server: %s', [AJson]);

    while reader.Read do
    begin
      if reader.TokenType = TJsonTokenType.EndObject then
        Break;
      if reader.TokenType <> TJsonTokenType.PropertyName then
        Continue;

      if reader.ValueSpanEquals('server_id') then
      begin
        if reader.Read then
          Result.ServerId := reader.GetString;
      end
      else if reader.ValueSpanEquals('server_name') then
      begin
        if reader.Read then
          Result.ServerName := reader.GetString;
      end
      else if reader.ValueSpanEquals('version') then
      begin
        if reader.Read then
          Result.Version := reader.GetString;
      end
      else if reader.ValueSpanEquals('proto') then
      begin
        if reader.Read then
          Result.Proto := reader.GetInt32;
      end
      else if reader.ValueSpanEquals('go') then
      begin
        if reader.Read then
          Result.GoVersion := reader.GetString;
      end
      else if reader.ValueSpanEquals('git_commit') then
      begin
        if reader.Read then
          Result.GitCommit := reader.GetString;
      end
      else if reader.ValueSpanEquals('host') then
      begin
        if reader.Read then
          Result.Host := reader.GetString;
      end
      else if reader.ValueSpanEquals('port') then
      begin
        if reader.Read then
          Result.Port := reader.GetInt32;
      end
      else if reader.ValueSpanEquals('ip') then
      begin
        if reader.Read then
          Result.Ip := reader.GetString;
      end
      else if reader.ValueSpanEquals('headers') then
      begin
        if reader.Read then
          Result.HeadersSupported := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('max_payload') then
      begin
        if reader.Read then
          Result.MaxPayload := reader.GetInt64;
      end
      else if reader.ValueSpanEquals('client_id') then
      begin
        if reader.Read then
          Result.ClientId := reader.GetInt64;
      end
      else if reader.ValueSpanEquals('client_ip') then
      begin
        if reader.Read then
          Result.ClientIp := reader.GetString;
      end
      else if reader.ValueSpanEquals('auth_required') then
      begin
        if reader.Read then
          Result.AuthRequired := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('tls_required') then
      begin
        if reader.Read then
          Result.TlsRequired := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('tls_verify') then
      begin
        if reader.Read then
          Result.TlsVerify := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('tls_available') then
      begin
        if reader.Read then
          Result.TlsAvailable := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('jetstream') then
      begin
        if reader.Read then
          Result.Jetstream := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('api_lvl') then
      begin
        if reader.Read then
          Result.JsApiLevel := reader.GetInt32;
      end
      else if reader.ValueSpanEquals('nonce') then
      begin
        if reader.Read then
          Result.Nonce := reader.GetString;
      end
      else if reader.ValueSpanEquals('cluster') then
      begin
        if reader.Read then
          Result.Cluster := reader.GetString;
      end
      else if reader.ValueSpanEquals('cluster_dynamic') then
      begin
        if reader.Read then
          Result.ClusterDynamic := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('domain') then
      begin
        if reader.Read then
          Result.Domain := reader.GetString;
      end
      else if reader.ValueSpanEquals('remote_account') then
      begin
        if reader.Read then
          Result.RemoteAccount := reader.GetString;
      end
      else if reader.ValueSpanEquals('acc_is_sys') then
      begin
        if reader.Read then
          Result.IsSystemAccount := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('ldm') then
      begin
        if reader.Read then
          Result.LameDuckMode := reader.GetBoolean;
      end
      else if reader.ValueSpanEquals('connect_urls') then
        ReadStringArray(Result.ConnectUrls)
      else if reader.ValueSpanEquals('ws_connect_urls') then
        ReadStringArray(Result.WsConnectUrls)
      else
      begin
        if reader.Read then
        begin
          if reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
            reader.Skip;
        end;
      end;
    end;
  except
    on E: EDextNatsProtocolError do
      raise;
    on E: EJsonException do
      raise EDextNatsProtocolError.CreateFmt(
        'Malformed INFO payload received from NATS server: %s', [AJson]);
  end;
end;

{ TNatsConnectOptions }

class function TNatsConnectOptions.CreateDefault: TNatsConnectOptions;
begin
  Result := Default(TNatsConnectOptions);
  Result.Verbose := False;
  Result.Pedantic := False;
  Result.TlsRequired := False;
  Result.Lang := NATS_LANG;
  Result.Version := NATS_CLIENT_VERSION;
  Result.Protocol := 1;
  Result.Echo := True;
  Result.Headers := True;
  Result.NoResponders := True;
end;

function TNatsConnectOptions.ToJson: string;
var
  w: TNatsByteWriter;
begin
  w.Reset;
  WriteConnectJson(Self, w);
  Result := TEncoding.UTF8.GetString(w.ToBytes);
end;

end.
