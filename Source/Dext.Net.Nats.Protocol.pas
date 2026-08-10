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
    function ParseControlLine(AOffset, ALength: Integer; out AFrame: TNatsFrame;
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

/// <summary>Encodes a CONNECT control line from the given options.</summary>
function NatsEncodeConnect(const AOptions: TNatsConnectOptions): TBytes;
/// <summary>Encodes a PUB frame (control line + payload + trailing CRLF).</summary>
function NatsEncodePub(const ASubject, AReplyTo: string; const APayload: TBytes): TBytes;
/// <summary>Encodes an HPUB frame (control line + header block + payload + trailing CRLF).</summary>
function NatsEncodeHPub(const ASubject, AReplyTo: string; const AHeaders: TNatsHeaders;
  const APayload: TBytes): TBytes;
/// <summary>Parses a decoded NATS header block (NATS/1.0 status line + name: value lines).</summary>
procedure NatsParseHeaderBlock(const ABlock: string; out AHeaders: TNatsHeaders; out AStatusCode: Integer);
/// <summary>Encodes a SUB control line.</summary>
function NatsEncodeSub(const ASubject, AQueue: string; ASid: Integer): TBytes;
/// <summary>Encodes an UNSUB control line. AMaxMsgs &lt;= 0 unsubscribes immediately.</summary>
function NatsEncodeUnsub(ASid: Integer; AMaxMsgs: Integer): TBytes;
/// <summary>Encodes a PING control line.</summary>
function NatsEncodePing: TBytes;
/// <summary>Encodes a PONG control line.</summary>
function NatsEncodePong: TBytes;

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

var
  GNatsEncodedPing: TBytes;
  GNatsEncodedPong: TBytes;

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

function NatsBytesToUtf8(const ABuf: TBytes; AOffset, ALength: Integer): string;
begin
  if ALength <= 0 then
    Result := ''
  else
    Result := TEncoding.UTF8.GetString(ABuf, AOffset, ALength);
end;

function NatsOpcodeEquals(const ABuf: TBytes; AOffset, ALength: Integer;
  const AOpcode: RawByteString): Boolean;
var
  i: Integer;
begin
  if ALength <> Length(AOpcode) then
    Exit(False);
  for i := 1 to ALength do
    if ABuf[AOffset + i - 1] <> Byte(AOpcode[i]) then
      Exit(False);
  Result := True;
end;

function NatsNextToken(const ABuf: TBytes; var APos: Integer; ALineEnd: Integer;
  out ATokStart, ATokLen: Integer): Boolean;
begin
  while (APos < ALineEnd) and (ABuf[APos] = Ord(' ')) do
    Inc(APos);
  if APos >= ALineEnd then
    Exit(False);
  ATokStart := APos;
  while (APos < ALineEnd) and (ABuf[APos] <> Ord(' ')) do
    Inc(APos);
  ATokLen := APos - ATokStart;
  Result := True;
end;

function NatsTryParseIntDec(const ABuf: TBytes; AOffset, ALength: Integer;
  out AValue: Integer): Boolean;
var
  i: Integer;
  neg: Boolean;
  v: Integer;
  d: Integer;
begin
  AValue := 0;
  if ALength <= 0 then
    Exit(False);

  i := 0;
  neg := False;
  if ABuf[AOffset] = Ord('-') then
  begin
    neg := True;
    Inc(i);
    if i >= ALength then
      Exit(False);
  end;

  v := 0;
  while i < ALength do
  begin
    d := ABuf[AOffset + i] - Ord('0');
    if (d < 0) or (d > 9) then
      Exit(False);
    if v > (MaxInt - d) div 10 then
      Exit(False);
    v := v * 10 + d;
    Inc(i);
  end;

  if neg then
    AValue := -v
  else
    AValue := v;
  Result := True;
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
  urls: TArray<string>;
  urlCount: Integer;
begin
  Result := Default(TNatsServerInfo);
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty INFO payload received from NATS server');

  bytes := TEncoding.UTF8.GetBytes(AJson);
  if Length(bytes) = 0 then
    raise EDextNatsProtocolError.Create('Empty INFO payload received from NATS server');

  span := TByteSpan.Create(@bytes[0], Length(bytes));
  urlCount := 0;
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
      else if reader.ValueSpanEquals('nonce') then
      begin
        if reader.Read then
          Result.Nonce := reader.GetString;
      end
      else if reader.ValueSpanEquals('connect_urls') then
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
                if urlCount >= Length(urls) then
                  SetLength(urls, urlCount + 4);
                urls[urlCount] := reader.GetString;
                Inc(urlCount);
              end
              else if reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
                reader.Skip;
            end;
          end
          else if reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
            reader.Skip;
        end;
      end
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

  SetLength(urls, urlCount);
  Result.ConnectUrls := urls;
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

{ Frame encoders }

function NatsEncodeConnect(const AOptions: TNatsConnectOptions): TBytes;
var
  w: TNatsByteWriter;
begin
  w.Reset;
  w.WriteAscii('CONNECT ');
  WriteConnectJson(AOptions, w);
  w.WriteCrLf;
  Result := w.ToBytes;
end;

function NatsEncodePub(const ASubject, AReplyTo: string; const APayload: TBytes): TBytes;
var
  w: TNatsByteWriter;
begin
  w.Reset;
  w.WriteAscii('PUB ');
  w.WriteUtf8(ASubject);
  w.WriteByte(Ord(' '));
  if AReplyTo <> '' then
  begin
    w.WriteUtf8(AReplyTo);
    w.WriteByte(Ord(' '));
  end;
  w.WriteIntDec(Length(APayload));
  w.WriteCrLf;
  w.WriteBytes(APayload);
  w.WriteCrLf;
  Result := w.ToBytes;
end;

function NatsEncodeHPub(const ASubject, AReplyTo: string; const AHeaders: TNatsHeaders;
  const APayload: TBytes): TBytes;
var
  w: TNatsByteWriter;
  headerBlock: TBytes;
begin
  headerBlock := AHeaders.Encode;

  w.Reset;
  w.WriteAscii('HPUB ');
  w.WriteUtf8(ASubject);
  w.WriteByte(Ord(' '));
  if AReplyTo <> '' then
  begin
    w.WriteUtf8(AReplyTo);
    w.WriteByte(Ord(' '));
  end;
  w.WriteIntDec(Length(headerBlock));
  w.WriteByte(Ord(' '));
  w.WriteIntDec(Length(headerBlock) + Length(APayload));
  w.WriteCrLf;
  w.WriteBytes(headerBlock);
  w.WriteBytes(APayload);
  w.WriteCrLf;
  Result := w.ToBytes;
end;

function NatsEncodeSub(const ASubject, AQueue: string; ASid: Integer): TBytes;
var
  w: TNatsByteWriter;
begin
  w.Reset;
  w.WriteAscii('SUB ');
  w.WriteUtf8(ASubject);
  w.WriteByte(Ord(' '));
  if AQueue <> '' then
  begin
    w.WriteUtf8(AQueue);
    w.WriteByte(Ord(' '));
  end;
  w.WriteIntDec(ASid);
  w.WriteCrLf;
  Result := w.ToBytes;
end;

function NatsEncodeUnsub(ASid: Integer; AMaxMsgs: Integer): TBytes;
var
  w: TNatsByteWriter;
begin
  w.Reset;
  w.WriteAscii('UNSUB ');
  w.WriteIntDec(ASid);
  if AMaxMsgs > 0 then
  begin
    w.WriteByte(Ord(' '));
    w.WriteIntDec(AMaxMsgs);
  end;
  w.WriteCrLf;
  Result := w.ToBytes;
end;

function NatsEncodePing: TBytes;
begin
  Result := GNatsEncodedPing;
end;

function NatsEncodePong: TBytes;
begin
  Result := GNatsEncodedPong;
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
var
  needed, cap: Integer;
begin
  if AData.Length = 0 then Exit;

  needed := FBufferLen + AData.Length;
  if needed > Length(FBuffer) then
  begin
    // Geometric / next power-of-two capacity (not linear +4096 only).
    cap := Length(FBuffer);
    if cap = 0 then
      cap := 4096;
    while cap < needed do
    begin
      if cap > (MaxInt div 2) then
      begin
        cap := needed;
        Break;
      end;
      cap := cap * 2;
    end;
    SetLength(FBuffer, cap);
  end;

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

function TDextNatsFrameParser.ParseControlLine(AOffset, ALength: Integer; out AFrame: TNatsFrame;
  out AHeaderBytes, ATotalBytes: Integer): Boolean;
var
  pos, lineEnd: Integer;
  opStart, opLen: Integer;
  t1s, t1l, t2s, t2l, t3s, t3l, t4s, t4l, t5s, t5l: Integer;
  tokCount: Integer;
  errText: string;
begin
  AHeaderBytes := 0;
  ATotalBytes := 0;
  AFrame := Default(TNatsFrame);

  if ALength <= 0 then
    Exit(False);

  lineEnd := AOffset + ALength;
  pos := AOffset;
  if not NatsNextToken(FBuffer, pos, lineEnd, opStart, opLen) then
    Exit(False);

  if NatsOpcodeEquals(FBuffer, opStart, opLen, 'PING') then
  begin
    AFrame.Kind := nfPing;
    Result := True;
  end
  else if NatsOpcodeEquals(FBuffer, opStart, opLen, 'PONG') then
  begin
    AFrame.Kind := nfPong;
    Result := True;
  end
  else if NatsOpcodeEquals(FBuffer, opStart, opLen, '+OK') then
  begin
    AFrame.Kind := nfOK;
    Result := True;
  end
  else if NatsOpcodeEquals(FBuffer, opStart, opLen, '-ERR') then
  begin
    AFrame.Kind := nfErr;
    errText := Trim(NatsBytesToUtf8(FBuffer, pos, lineEnd - pos));
    AFrame.ErrorText := errText.Trim(['''']);
    Result := True;
  end
  else if NatsOpcodeEquals(FBuffer, opStart, opLen, 'INFO') then
  begin
    AFrame.Kind := nfInfo;
    AFrame.InfoJson := Trim(NatsBytesToUtf8(FBuffer, pos, lineEnd - pos));
    Result := True;
  end
  else if NatsOpcodeEquals(FBuffer, opStart, opLen, 'MSG') then
  begin
    AFrame.Kind := nfMsg;
    // MSG <subject> <sid> [reply-to] <#bytes>
    tokCount := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t1s, t1l) then Inc(tokCount) else t1l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t2s, t2l) then Inc(tokCount) else t2l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t3s, t3l) then Inc(tokCount) else t3l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t4s, t4l) then Inc(tokCount) else t4l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t5s, t5l) then
      Exit(False); // too many tokens

    if (tokCount < 3) or (tokCount > 4) then
      Exit(False);

    AFrame.Subject := NatsBytesToUtf8(FBuffer, t1s, t1l);
    if not NatsTryParseIntDec(FBuffer, t2s, t2l, AFrame.Sid) then
      Exit(False);

    if tokCount = 3 then
    begin
      if not NatsTryParseIntDec(FBuffer, t3s, t3l, ATotalBytes) then
        Exit(False);
    end
    else
    begin
      AFrame.ReplyTo := NatsBytesToUtf8(FBuffer, t3s, t3l);
      if not NatsTryParseIntDec(FBuffer, t4s, t4l, ATotalBytes) then
        Exit(False);
    end;

    Result := ATotalBytes >= 0;
  end
  else if NatsOpcodeEquals(FBuffer, opStart, opLen, 'HMSG') then
  begin
    AFrame.Kind := nfHMsg;
    // HMSG <subject> <sid> [reply-to] <#header-bytes> <#total-bytes>
    tokCount := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t1s, t1l) then Inc(tokCount) else t1l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t2s, t2l) then Inc(tokCount) else t2l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t3s, t3l) then Inc(tokCount) else t3l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t4s, t4l) then Inc(tokCount) else t4l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, t5s, t5l) then Inc(tokCount) else t5l := 0;
    if NatsNextToken(FBuffer, pos, lineEnd, opStart, opLen) then
      Exit(False); // too many tokens

    if (tokCount < 4) or (tokCount > 5) then
      Exit(False);

    AFrame.Subject := NatsBytesToUtf8(FBuffer, t1s, t1l);
    if not NatsTryParseIntDec(FBuffer, t2s, t2l, AFrame.Sid) then
      Exit(False);

    if tokCount = 4 then
    begin
      if not NatsTryParseIntDec(FBuffer, t3s, t3l, AHeaderBytes) then
        Exit(False);
      if not NatsTryParseIntDec(FBuffer, t4s, t4l, ATotalBytes) then
        Exit(False);
    end
    else
    begin
      AFrame.ReplyTo := NatsBytesToUtf8(FBuffer, t3s, t3l);
      if not NatsTryParseIntDec(FBuffer, t4s, t4l, AHeaderBytes) then
        Exit(False);
      if not NatsTryParseIntDec(FBuffer, t5s, t5l, ATotalBytes) then
        Exit(False);
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
  AFrame := Default(TNatsFrame);

  if not FHasPendingControl then
  begin
    crlfIdx := IndexOfCrLf(0);
    if crlfIdx < 0 then
      Exit(False);

    // Opcode match on raw bytes — avoid GetString of the whole line on the hot path.
    if not ParseControlLine(0, crlfIdx, FPendingFrame, headerBytes, totalBytes) then
    begin
      line := NatsBytesToUtf8(FBuffer, 0, crlfIdx);
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
    // Single Move into owned Payload TBytes (no double copy).
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

initialization
  GNatsEncodedPing := BytesOf(RawByteString('PING'#13#10));
  GNatsEncodedPong := BytesOf(RawByteString('PONG'#13#10));

end.
