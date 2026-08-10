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
{  payloads, headers, frame encoders and the incremental frame parser.      }
{  This unit performs no I/O; see Dext.Net.Nats for the socket-facing        }
{  client built on top of it.                                               }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Protocol;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  Dext.Collections.Dict,
  Dext.Collections,
  Dext.Core.Span;

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

  /// <summary>Fields advertised by the server in its initial INFO message.</summary>
  TNatsServerInfo = record
    ServerId: string;
    ServerName: string;
    Version: string;
    Proto: Integer;
    GoVersion: string;
    Host: string;
    Port: Integer;
    HeadersSupported: Boolean;
    MaxPayload: Int64;
    ClientId: Int64;
    ClientIp: string;
    AuthRequired: Boolean;
    TlsRequired: Boolean;
    TlsAvailable: Boolean;
    Jetstream: Boolean;
    Nonce: string;
    ConnectUrls: TArray<string>;
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

  /// <summary>
  ///   Incremental, allocation-conscious decoder for the NATS text protocol.
  ///   Bytes arriving from the socket are appended with <see cref="Append"/>;
  ///   complete frames are pulled out one at a time with <see cref="TryReadFrame"/>.
  ///   Designed to be driven from a single reader thread; not thread-safe.
  /// </summary>
  TDextNatsFrameParser = class
  private
    FBuffer: TBytes;
    FBufferLen: Integer;
    FMaxFrameBytes: Int64;
    FHasPendingControl: Boolean;
    FPendingFrame: TNatsFrame;
    FPendingLineLength: Integer;
    FPendingHeaderBytes: Integer;
    FPendingTotalBytes: Integer;
    function IndexOfCrLf(AStart: Integer): Integer;
    procedure ShiftBuffer(ACount: Integer);
    function ParseControlLine(const ALine: string; out AFrame: TNatsFrame;
      out AHeaderBytes, ATotalBytes: Integer): Boolean;
  public
    constructor Create;
    /// <summary>Appends a zero-copy view of freshly received bytes to the internal buffer.</summary>
    procedure Append(const AData: TByteSpan); overload;
    /// <summary>Appends the first ACount bytes of AData to the internal buffer.</summary>
    procedure Append(const AData: TBytes; ACount: Integer); overload;
    /// <summary>
    ///   Attempts to decode the next complete frame from the buffered bytes.
    ///   Returns False when more bytes are needed; call again after the next Append.
    /// </summary>
    function TryReadFrame(out AFrame: TNatsFrame): Boolean;
    /// <summary>Discards all buffered bytes and any partially parsed frame (used on reconnect).</summary>
    procedure Clear;
    /// <summary>Safety ceiling for a single MSG/HMSG payload; defaults to <see cref="NATS_MAX_FRAME_BYTES"/>.</summary>
    property MaxFrameBytes: Int64 read FMaxFrameBytes write FMaxFrameBytes;
  end;

/// <summary>Generates a process-unique inbox subject, e.g. "_INBOX.3f2c9e1a...".</summary>
function NatsNewInbox: string;

/// <summary>Renders a Boolean as the lowercase JSON literal "true"/"false".</summary>
function NatsBoolStr(AValue: Boolean): string;
/// <summary>Escapes a string for safe embedding inside a JSON string literal.</summary>
function NatsJsonEscape(const S: string): string;
/// <summary>Reads a string field from AObj, or ADefault if absent/null.</summary>
function NatsJsonGetStr(AObj: TJSONObject; const AKey: string; const ADefault: string = ''): string;
/// <summary>Reads an Integer field from AObj, or ADefault if absent/null/unparsable.</summary>
function NatsJsonGetInt(AObj: TJSONObject; const AKey: string; ADefault: Integer = 0): Integer;
/// <summary>Reads an Int64 field from AObj, or ADefault if absent/null/unparsable.</summary>
function NatsJsonGetInt64(AObj: TJSONObject; const AKey: string; ADefault: Int64 = 0): Int64;
/// <summary>Reads a Boolean field from AObj, or ADefault if absent/null.</summary>
function NatsJsonGetBool(AObj: TJSONObject; const AKey: string; ADefault: Boolean = False): Boolean;

/// <summary>Encodes a CONNECT control line from the given options.</summary>
function NatsEncodeConnect(const AOptions: TNatsConnectOptions): TBytes;
/// <summary>Encodes a PUB frame (control line + payload + trailing CRLF).</summary>
function NatsEncodePub(const ASubject, AReplyTo: string; const APayload: TBytes): TBytes;
/// <summary>Encodes an HPUB frame (control line + header block + payload + trailing CRLF).</summary>
function NatsEncodeHPub(const ASubject, AReplyTo: string; const AHeaders: TNatsHeaders;
  const APayload: TBytes): TBytes;
/// <summary>Encodes a SUB control line.</summary>
function NatsEncodeSub(const ASubject, AQueue: string; ASid: Integer): TBytes;
/// <summary>Encodes an UNSUB control line. AMaxMsgs &lt;= 0 unsubscribes immediately.</summary>
function NatsEncodeUnsub(ASid: Integer; AMaxMsgs: Integer): TBytes;
/// <summary>Encodes a PING control line.</summary>
function NatsEncodePing: TBytes;
/// <summary>Encodes a PONG control line.</summary>
function NatsEncodePong: TBytes;

implementation

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

function NatsConcatBytes(const AParts: array of TBytes): TBytes;
var
  total, pos, i: Integer;
begin
  total := 0;
  for i := 0 to High(AParts) do
    Inc(total, Length(AParts[i]));

  SetLength(Result, total);
  pos := 0;
  for i := 0 to High(AParts) do
  begin
    if Length(AParts[i]) > 0 then
      Move(AParts[i][0], Result[pos], Length(AParts[i]));
    Inc(pos, Length(AParts[i]));
  end;
end;

function NatsJsonGetStr(AObj: TJSONObject; const AKey: string; const ADefault: string = ''): string;
var
  v: TJSONValue;
begin
  Result := ADefault;
  if not Assigned(AObj) then Exit;
  v := AObj.GetValue(AKey);
  if Assigned(v) and not (v is TJSONNull) then
    Result := v.Value;
end;

function NatsJsonGetInt(AObj: TJSONObject; const AKey: string; ADefault: Integer = 0): Integer;
var
  v: TJSONValue;
begin
  Result := ADefault;
  if not Assigned(AObj) then Exit;
  v := AObj.GetValue(AKey);
  if Assigned(v) and not (v is TJSONNull) then
    Result := StrToIntDef(v.Value, ADefault);
end;

function NatsJsonGetInt64(AObj: TJSONObject; const AKey: string; ADefault: Int64 = 0): Int64;
var
  v: TJSONValue;
begin
  Result := ADefault;
  if not Assigned(AObj) then Exit;
  v := AObj.GetValue(AKey);
  if Assigned(v) and not (v is TJSONNull) then
    Result := StrToInt64Def(v.Value, ADefault);
end;

function NatsJsonGetBool(AObj: TJSONObject; const AKey: string; ADefault: Boolean = False): Boolean;
var
  v: TJSONValue;
begin
  Result := ADefault;
  if not Assigned(AObj) then Exit;
  v := AObj.GetValue(AKey);
  if Assigned(v) and not (v is TJSONNull) then
    Result := (v is TJSONTrue) or SameText(v.Value, 'true');
end;

/// <summary>Splits a NATS control line on single spaces, ignoring empty tokens
/// caused by accidental repeated separators.</summary>
function NatsSplitLine(const ALine: string): TArray<string>;
begin
  Result := ALine.Split([' '], TStringSplitOptions.ExcludeEmpty);
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
  obj: TJSONObject;
  urlsArr: TJSONValue;
  arr: TJSONArray;
  i: Integer;
begin
  FillChar(Result, SizeOf(Result), 0);
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty INFO payload received from NATS server');

  obj := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  if not Assigned(obj) then
    raise EDextNatsProtocolError.CreateFmt('Malformed INFO payload received from NATS server: %s', [AJson]);
  try
    Result.ServerId := NatsJsonGetStr(obj, 'server_id');
    Result.ServerName := NatsJsonGetStr(obj, 'server_name');
    Result.Version := NatsJsonGetStr(obj, 'version');
    Result.Proto := NatsJsonGetInt(obj, 'proto');
    Result.GoVersion := NatsJsonGetStr(obj, 'go');
    Result.Host := NatsJsonGetStr(obj, 'host');
    Result.Port := NatsJsonGetInt(obj, 'port');
    Result.HeadersSupported := NatsJsonGetBool(obj, 'headers');
    Result.MaxPayload := NatsJsonGetInt64(obj, 'max_payload');
    Result.ClientId := NatsJsonGetInt64(obj, 'client_id');
    Result.ClientIp := NatsJsonGetStr(obj, 'client_ip');
    Result.AuthRequired := NatsJsonGetBool(obj, 'auth_required');
    Result.TlsRequired := NatsJsonGetBool(obj, 'tls_required');
    Result.TlsAvailable := NatsJsonGetBool(obj, 'tls_available');
    Result.Jetstream := NatsJsonGetBool(obj, 'jetstream');
    Result.Nonce := NatsJsonGetStr(obj, 'nonce');

    urlsArr := obj.GetValue('connect_urls');
    if Assigned(urlsArr) and (urlsArr is TJSONArray) then
    begin
      arr := TJSONArray(urlsArr);
      SetLength(Result.ConnectUrls, arr.Count);
      for i := 0 to arr.Count - 1 do
        Result.ConnectUrls[i] := arr.Items[i].Value;
    end;
  finally
    obj.Free;
  end;
end;

{ TNatsConnectOptions }

class function TNatsConnectOptions.CreateDefault: TNatsConnectOptions;
begin
  FillChar(Result, SizeOf(Result), 0);
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
  sb: TStringBuilder;
begin
  sb := TStringBuilder.Create;
  try
    sb.Append('{');
    sb.Append('"verbose":').Append(NatsBoolStr(Verbose)).Append(',');
    sb.Append('"pedantic":').Append(NatsBoolStr(Pedantic)).Append(',');
    sb.Append('"tls_required":').Append(NatsBoolStr(TlsRequired)).Append(',');
    if AuthToken <> '' then
      sb.Append('"auth_token":"').Append(NatsJsonEscape(AuthToken)).Append('",');
    if User <> '' then
      sb.Append('"user":"').Append(NatsJsonEscape(User)).Append('",');
    if Password <> '' then
      sb.Append('"pass":"').Append(NatsJsonEscape(Password)).Append('",');
    if JWT <> '' then
      sb.Append('"jwt":"').Append(NatsJsonEscape(JWT)).Append('",');
    if Nkey <> '' then
      sb.Append('"nkey":"').Append(NatsJsonEscape(Nkey)).Append('",');
    if Sig <> '' then
      sb.Append('"sig":"').Append(NatsJsonEscape(Sig)).Append('",');
    sb.Append('"name":"').Append(NatsJsonEscape(Name)).Append('",');
    sb.Append('"lang":"').Append(NatsJsonEscape(Lang)).Append('",');
    sb.Append('"version":"').Append(NatsJsonEscape(Version)).Append('",');
    sb.Append('"protocol":').Append(Protocol).Append(',');
    sb.Append('"echo":').Append(NatsBoolStr(Echo)).Append(',');
    sb.Append('"headers":').Append(NatsBoolStr(Headers)).Append(',');
    sb.Append('"no_responders":').Append(NatsBoolStr(NoResponders));
    sb.Append('}');
    Result := sb.ToString;
  finally
    sb.Free;
  end;
end;

{ Frame encoders }

function NatsEncodeConnect(const AOptions: TNatsConnectOptions): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes('CONNECT ' + AOptions.ToJson + NATS_CRLF);
end;

function NatsEncodePub(const ASubject, AReplyTo: string; const APayload: TBytes): TBytes;
var
  line: string;
begin
  if AReplyTo = '' then
    line := Format('PUB %s %d' + NATS_CRLF, [ASubject, Length(APayload)])
  else
    line := Format('PUB %s %s %d' + NATS_CRLF, [ASubject, AReplyTo, Length(APayload)]);

  Result := NatsConcatBytes([TEncoding.UTF8.GetBytes(line), APayload, TEncoding.UTF8.GetBytes(NATS_CRLF)]);
end;

function NatsEncodeHPub(const ASubject, AReplyTo: string; const AHeaders: TNatsHeaders;
  const APayload: TBytes): TBytes;
var
  headerBlock: TBytes;
  line: string;
begin
  headerBlock := AHeaders.Encode;

  if AReplyTo = '' then
    line := Format('HPUB %s %d %d' + NATS_CRLF,
      [ASubject, Length(headerBlock), Length(headerBlock) + Length(APayload)])
  else
    line := Format('HPUB %s %s %d %d' + NATS_CRLF,
      [ASubject, AReplyTo, Length(headerBlock), Length(headerBlock) + Length(APayload)]);

  Result := NatsConcatBytes([TEncoding.UTF8.GetBytes(line), headerBlock, APayload,
    TEncoding.UTF8.GetBytes(NATS_CRLF)]);
end;

function NatsEncodeSub(const ASubject, AQueue: string; ASid: Integer): TBytes;
var
  line: string;
begin
  if AQueue = '' then
    line := Format('SUB %s %d' + NATS_CRLF, [ASubject, ASid])
  else
    line := Format('SUB %s %s %d' + NATS_CRLF, [ASubject, AQueue, ASid]);
  Result := TEncoding.UTF8.GetBytes(line);
end;

function NatsEncodeUnsub(ASid: Integer; AMaxMsgs: Integer): TBytes;
var
  line: string;
begin
  if AMaxMsgs <= 0 then
    line := Format('UNSUB %d' + NATS_CRLF, [ASid])
  else
    line := Format('UNSUB %d %d' + NATS_CRLF, [ASid, AMaxMsgs]);
  Result := TEncoding.UTF8.GetBytes(line);
end;

function NatsEncodePing: TBytes;
begin
  Result := TEncoding.UTF8.GetBytes('PING' + NATS_CRLF);
end;

function NatsEncodePong: TBytes;
begin
  Result := TEncoding.UTF8.GetBytes('PONG' + NATS_CRLF);
end;

{ TDextNatsFrameParser }

constructor TDextNatsFrameParser.Create;
begin
  inherited Create;
  SetLength(FBuffer, 4096);
  FBufferLen := 0;
  FMaxFrameBytes := NATS_MAX_FRAME_BYTES;
end;

procedure TDextNatsFrameParser.Append(const AData: TByteSpan);
begin
  if AData.Length = 0 then Exit;

  if FBufferLen + AData.Length > Length(FBuffer) then
    SetLength(FBuffer, FBufferLen + AData.Length + 4096);

  Move(AData.Data^, FBuffer[FBufferLen], AData.Length);
  Inc(FBufferLen, AData.Length);
end;

procedure TDextNatsFrameParser.Append(const AData: TBytes; ACount: Integer);
var
  span: TByteSpan;
begin
  if ACount <= 0 then Exit;
  span := TByteSpan.Create(@AData[0], ACount);
  Append(span);
end;

procedure TDextNatsFrameParser.Clear;
begin
  FBufferLen := 0;
  FHasPendingControl := False;
end;

function TDextNatsFrameParser.IndexOfCrLf(AStart: Integer): Integer;
var
  i: Integer;
begin
  i := AStart;
  while i < FBufferLen - 1 do
  begin
    if (FBuffer[i] = 13) and (FBuffer[i + 1] = 10) then
      Exit(i);
    Inc(i);
  end;
  Result := -1;
end;

procedure TDextNatsFrameParser.ShiftBuffer(ACount: Integer);
begin
  if ACount <= 0 then Exit;

  if ACount >= FBufferLen then
  begin
    FBufferLen := 0;
    Exit;
  end;

  Move(FBuffer[ACount], FBuffer[0], FBufferLen - ACount);
  Dec(FBufferLen, ACount);
end;

function TDextNatsFrameParser.ParseControlLine(const ALine: string; out AFrame: TNatsFrame;
  out AHeaderBytes, ATotalBytes: Integer): Boolean;
var
  parts: TArray<string>;
  cmd: string;
  sp: Integer;
begin
  AHeaderBytes := 0;
  ATotalBytes := 0;
  FillChar(AFrame, SizeOf(AFrame), 0);

  if ALine = '' then Exit(False);

  sp := ALine.IndexOf(' ');
  if sp < 0 then
    cmd := ALine
  else
    cmd := Copy(ALine, 1, sp);

  if SameText(cmd, 'PING') then
  begin
    AFrame.Kind := nfPing;
    Result := True;
  end
  else if SameText(cmd, 'PONG') then
  begin
    AFrame.Kind := nfPong;
    Result := True;
  end
  else if cmd = '+OK' then
  begin
    AFrame.Kind := nfOK;
    Result := True;
  end
  else if SameText(cmd, '-ERR') then
  begin
    AFrame.Kind := nfErr;
    AFrame.ErrorText := Trim(Copy(ALine, 5, MaxInt));
    AFrame.ErrorText := AFrame.ErrorText.Trim(['''']);
    Result := True;
  end
  else if SameText(cmd, 'INFO') then
  begin
    AFrame.Kind := nfInfo;
    AFrame.InfoJson := Trim(Copy(ALine, 5, MaxInt));
    Result := True;
  end
  else if SameText(cmd, 'MSG') then
  begin
    AFrame.Kind := nfMsg;
    parts := NatsSplitLine(ALine);
    // MSG <subject> <sid> [reply-to] <#bytes>
    if (Length(parts) < 4) or (Length(parts) > 5) then Exit(False);

    AFrame.Subject := parts[1];
    if not TryStrToInt(parts[2], AFrame.Sid) then Exit(False);

    if Length(parts) = 4 then
      ATotalBytes := StrToIntDef(parts[3], -1)
    else
    begin
      AFrame.ReplyTo := parts[3];
      ATotalBytes := StrToIntDef(parts[4], -1);
    end;

    Result := ATotalBytes >= 0;
  end
  else if SameText(cmd, 'HMSG') then
  begin
    AFrame.Kind := nfHMsg;
    parts := NatsSplitLine(ALine);
    // HMSG <subject> <sid> [reply-to] <#header-bytes> <#total-bytes>
    if (Length(parts) < 5) or (Length(parts) > 6) then Exit(False);

    AFrame.Subject := parts[1];
    if not TryStrToInt(parts[2], AFrame.Sid) then Exit(False);

    if Length(parts) = 5 then
    begin
      AHeaderBytes := StrToIntDef(parts[3], -1);
      ATotalBytes := StrToIntDef(parts[4], -1);
    end
    else
    begin
      AFrame.ReplyTo := parts[3];
      AHeaderBytes := StrToIntDef(parts[4], -1);
      ATotalBytes := StrToIntDef(parts[5], -1);
    end;

    Result := (AHeaderBytes >= 0) and (ATotalBytes >= AHeaderBytes);
  end
  else
    Result := False;

  if Result and ((AHeaderBytes > FMaxFrameBytes) or (ATotalBytes > FMaxFrameBytes)) then
    raise EDextNatsProtocolError.CreateFmt(
      'NATS server announced a frame of %d bytes, which exceeds the configured safety limit of %d bytes',
      [ATotalBytes, FMaxFrameBytes]);
end;

function TDextNatsFrameParser.TryReadFrame(out AFrame: TNatsFrame): Boolean;
var
  crlfIdx: Integer;
  line: string;
  headerBytes, totalBytes: Integer;
  neededBytes: Integer;
  headerBlock: TBytes;
  payload: TBytes;
  payloadLen: Integer;
begin
  Result := False;
  FillChar(AFrame, SizeOf(AFrame), 0);

  if not FHasPendingControl then
  begin
    crlfIdx := IndexOfCrLf(0);
    if crlfIdx < 0 then
      Exit(False);

    line := TEncoding.UTF8.GetString(FBuffer, 0, crlfIdx);
    if not ParseControlLine(line, FPendingFrame, headerBytes, totalBytes) then
    begin
      ShiftBuffer(crlfIdx + 2);
      raise EDextNatsProtocolError.CreateFmt('Malformed NATS protocol line: "%s"', [line]);
    end;

    FPendingLineLength := crlfIdx + 2;
    FPendingHeaderBytes := headerBytes;
    FPendingTotalBytes := totalBytes;
    FHasPendingControl := True;
  end;

  if not (FPendingFrame.Kind in [nfMsg, nfHMsg]) then
  begin
    AFrame := FPendingFrame;
    ShiftBuffer(FPendingLineLength);
    FHasPendingControl := False;
    Exit(True);
  end;

  neededBytes := FPendingLineLength + FPendingTotalBytes + 2; // + trailing CRLF after the payload
  if FBufferLen < neededBytes then
    Exit(False);

  if FPendingFrame.Kind = nfHMsg then
  begin
    SetLength(headerBlock, FPendingHeaderBytes);
    if FPendingHeaderBytes > 0 then
      Move(FBuffer[FPendingLineLength], headerBlock[0], FPendingHeaderBytes);
    NatsParseHeaderBlock(TEncoding.UTF8.GetString(headerBlock), FPendingFrame.Headers, FPendingFrame.StatusCode);

    payloadLen := FPendingTotalBytes - FPendingHeaderBytes;
    SetLength(payload, payloadLen);
    if payloadLen > 0 then
      Move(FBuffer[FPendingLineLength + FPendingHeaderBytes], payload[0], payloadLen);
    FPendingFrame.Payload := payload;
  end
  else
  begin
    SetLength(payload, FPendingTotalBytes);
    if FPendingTotalBytes > 0 then
      Move(FBuffer[FPendingLineLength], payload[0], FPendingTotalBytes);
    FPendingFrame.Payload := payload;
  end;

  AFrame := FPendingFrame;
  ShiftBuffer(neededBytes);
  FHasPendingControl := False;
  Result := True;
end;

end.
