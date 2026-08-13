unit Dext.Net.Nats.Protocol.Writer;

interface

uses
  System.SysUtils,
  Dext.Net.Nats.Protocol;

function NatsV2EncodeConnect(const AOptions: TNatsConnectOptions): TBytes;
function NatsV2EncodePub(const ASubject, AReplyTo: string;
  const APayload: TBytes): TBytes;
function NatsV2EncodeHPub(const ASubject, AReplyTo: string;
  const AHeaders: TNatsHeaders; const APayload: TBytes): TBytes;

implementation

uses
  Dext.Json.Utf8,
  Dext.Collections.Pool,
  Dext.Net.Nats.Protocol.Headers;

type
  PNatsV2Writer = ^TNatsV2Writer;
  TNatsV2Writer = record
  private
    FBuf: TBytes;
    FLen: Integer;
    procedure EnsureCapacity(ANeeded: Integer);
  public
    procedure Reset;
    procedure WriteByte(AValue: Byte);
    procedure WriteBytes(AData: Pointer; ALength: Integer); overload;
    procedure WriteBytes(const AData: TBytes); overload;
    procedure WriteAscii(const S: RawByteString);
    procedure WriteUtf8(const S: string);
    procedure WriteIntDec(AValue: Int64);
    procedure WriteCrLf;
    function ToBytes: TBytes;
  end;

  /// <summary>
  ///   TNatsV2Writer is a record, so it cannot be TDextPool&lt;T&gt; directly
  ///   (constraint T: class, constructor). This parameterless class wrapper
  ///   lets PUB/HPUB/CONNECT reuse the grown TBytes scratch. Public
  ///   NatsV2Encode* still return an owned TBytes copy (Publish API unchanged).
  ///   AcquireTimeoutMs=0: if the pool is exhausted, encode uses a stack writer
  ///   instead of blocking the publish path.
  /// </summary>
  TDextNatsEncodeScratch = class
  public
    Writer: TNatsV2Writer;
    constructor Create;
  end;

var
  GEncodePool: IDextPool<TDextNatsEncodeScratch>;

procedure TNatsV2Writer.Reset;
begin
  FLen := 0;
end;

procedure TNatsV2Writer.EnsureCapacity(ANeeded: Integer);
var
  NewCap: Integer;
begin
  if FLen + ANeeded <= Length(FBuf) then Exit;
  NewCap := Length(FBuf);
  if NewCap < 256 then NewCap := 256;
  while FLen + ANeeded > NewCap do
    NewCap := NewCap * 2;
  SetLength(FBuf, NewCap);
end;

procedure TNatsV2Writer.WriteByte(AValue: Byte);
begin
  EnsureCapacity(1);
  FBuf[FLen] := AValue;
  Inc(FLen);
end;

procedure TNatsV2Writer.WriteBytes(AData: Pointer; ALength: Integer);
begin
  if (AData = nil) or (ALength <= 0) then Exit;
  EnsureCapacity(ALength);
  Move(AData^, FBuf[FLen], ALength);
  Inc(FLen, ALength);
end;

procedure TNatsV2Writer.WriteBytes(const AData: TBytes);
begin
  if Length(AData) > 0 then WriteBytes(@AData[0], Length(AData));
end;

procedure TNatsV2Writer.WriteAscii(const S: RawByteString);
begin
  if Length(S) > 0 then WriteBytes(@S[1], Length(S));
end;

procedure TNatsV2Writer.WriteUtf8(const S: string);
var
  Bytes: TBytes;
begin
  if S = '' then Exit;
  Bytes := TEncoding.UTF8.GetBytes(S);
  WriteBytes(Bytes);
end;

procedure TNatsV2Writer.WriteIntDec(AValue: Int64);
var
  Buf: array[0..31] of AnsiChar;
  P: Integer;
  Negative: Boolean;
  V: UInt64;
begin
  P := High(Buf);
  Negative := AValue < 0;
  if Negative then V := UInt64(-(AValue + 1)) + 1 else V := UInt64(AValue);
  repeat
    Buf[P] := AnsiChar(Ord('0') + (V mod 10));
    V := V div 10;
    Dec(P);
  until V = 0;
  if Negative then
  begin
    Buf[P] := '-';
    Dec(P);
  end;
  Inc(P);
  WriteBytes(@Buf[P], High(Buf) - P + 1);
end;

procedure TNatsV2Writer.WriteCrLf;
begin
  WriteByte(13);
  WriteByte(10);
end;

function TNatsV2Writer.ToBytes: TBytes;
begin
  SetLength(Result, FLen);
  if FLen > 0 then Move(FBuf[0], Result[0], FLen);
end;

constructor TDextNatsEncodeScratch.Create;
begin
  inherited Create;
  Writer.Reset;
end;

function AcquireEncodeWriter(out Scratch: TDextNatsEncodeScratch;
  var Local: TNatsV2Writer): PNatsV2Writer;
begin
  Scratch := nil;
  if (GEncodePool <> nil) and GEncodePool.Acquire(Scratch) then
  begin
    Scratch.Writer.Reset;
    Result := @Scratch.Writer;
  end
  else
  begin
    Scratch := nil;
    Local.Reset;
    Result := @Local;
  end;
end;

procedure ReleaseEncodeWriter(Scratch: TDextNatsEncodeScratch);
begin
  if (Scratch <> nil) and (GEncodePool <> nil) then
    GEncodePool.Release(Scratch);
end;

procedure Utf8Sink(AContext, AData: Pointer; ALength: Integer);
begin
  if (AContext <> nil) and (AData <> nil) and (ALength > 0) then
    PNatsV2Writer(AContext)^.WriteBytes(AData, ALength);
end;

procedure WriteConnectJson(const AOptions: TNatsConnectOptions;
  var AWriter: TNatsV2Writer);
var
  Json: TUtf8JsonWriter;
begin
  Json := TUtf8JsonWriter.Create(@AWriter, Utf8Sink, False);
  Json.WriteStartObject;
  Json.WritePropertyName('verbose'); Json.WriteBoolean(AOptions.Verbose);
  Json.WritePropertyName('pedantic'); Json.WriteBoolean(AOptions.Pedantic);
  Json.WritePropertyName('tls_required'); Json.WriteBoolean(AOptions.TlsRequired);
  if AOptions.AuthToken <> '' then begin Json.WritePropertyName('auth_token'); Json.WriteString(AOptions.AuthToken); end;
  if AOptions.User <> '' then begin Json.WritePropertyName('user'); Json.WriteString(AOptions.User); end;
  if AOptions.Password <> '' then begin Json.WritePropertyName('pass'); Json.WriteString(AOptions.Password); end;
  if AOptions.JWT <> '' then begin Json.WritePropertyName('jwt'); Json.WriteString(AOptions.JWT); end;
  if AOptions.Nkey <> '' then begin Json.WritePropertyName('nkey'); Json.WriteString(AOptions.Nkey); end;
  if AOptions.Sig <> '' then begin Json.WritePropertyName('sig'); Json.WriteString(AOptions.Sig); end;
  Json.WritePropertyName('name'); Json.WriteString(AOptions.Name);
  Json.WritePropertyName('lang'); Json.WriteString(AOptions.Lang);
  Json.WritePropertyName('version'); Json.WriteString(AOptions.Version);
  Json.WritePropertyName('protocol'); Json.WriteNumber(AOptions.Protocol);
  Json.WritePropertyName('echo'); Json.WriteBoolean(AOptions.Echo);
  Json.WritePropertyName('headers'); Json.WriteBoolean(AOptions.Headers);
  Json.WritePropertyName('no_responders'); Json.WriteBoolean(AOptions.NoResponders);
  Json.WriteEndObject;
end;

function NatsV2EncodeConnect(const AOptions: TNatsConnectOptions): TBytes;
var
  Scratch: TDextNatsEncodeScratch;
  Local: TNatsV2Writer;
  Writer: PNatsV2Writer;
begin
  Writer := AcquireEncodeWriter(Scratch, Local);
  try
    Writer^.WriteAscii('CONNECT ');
    WriteConnectJson(AOptions, Writer^);
    Writer^.WriteCrLf;
    Result := Writer^.ToBytes;
  finally
    ReleaseEncodeWriter(Scratch);
  end;
end;

function NatsV2EncodePub(const ASubject, AReplyTo: string;
  const APayload: TBytes): TBytes;
var
  Scratch: TDextNatsEncodeScratch;
  Local: TNatsV2Writer;
  Writer: PNatsV2Writer;
begin
  Writer := AcquireEncodeWriter(Scratch, Local);
  try
    Writer^.WriteAscii('PUB ');
    Writer^.WriteUtf8(ASubject);
    Writer^.WriteByte(Ord(' '));
    if AReplyTo <> '' then
    begin
      Writer^.WriteUtf8(AReplyTo);
      Writer^.WriteByte(Ord(' '));
    end;
    Writer^.WriteIntDec(Length(APayload));
    Writer^.WriteCrLf;
    Writer^.WriteBytes(APayload);
    Writer^.WriteCrLf;
    Result := Writer^.ToBytes;
  finally
    ReleaseEncodeWriter(Scratch);
  end;
end;

function NatsV2EncodeHPub(const ASubject, AReplyTo: string;
  const AHeaders: TNatsHeaders; const APayload: TBytes): TBytes;
var
  Scratch: TDextNatsEncodeScratch;
  Local: TNatsV2Writer;
  Writer: PNatsV2Writer;
  HeaderBlock: TBytes;
begin
  HeaderBlock := NatsEncodeHeaderBlock(AHeaders);
  Writer := AcquireEncodeWriter(Scratch, Local);
  try
    Writer^.WriteAscii('HPUB ');
    Writer^.WriteUtf8(ASubject);
    Writer^.WriteByte(Ord(' '));
    if AReplyTo <> '' then
    begin
      Writer^.WriteUtf8(AReplyTo);
      Writer^.WriteByte(Ord(' '));
    end;
    Writer^.WriteIntDec(Length(HeaderBlock));
    Writer^.WriteByte(Ord(' '));
    Writer^.WriteIntDec(Length(HeaderBlock) + Length(APayload));
    Writer^.WriteCrLf;
    Writer^.WriteBytes(HeaderBlock);
    Writer^.WriteBytes(APayload);
    Writer^.WriteCrLf;
    Result := Writer^.ToBytes;
  finally
    ReleaseEncodeWriter(Scratch);
  end;
end;

procedure InitEncodePool;
var
  Config: TDextPoolConfig;
begin
  Config := TDextPoolConfig.Default;
  Config.MinSize := 2;
  Config.MaxSize := 16;
  Config.AcquireTimeoutMs := 0;
  GEncodePool := TDextPool<TDextNatsEncodeScratch>.Create(Config,
    procedure(Item: TDextNatsEncodeScratch)
    begin
      Item.Writer.Reset;
    end);
end;

initialization
  InitEncodePool;

finalization
  GEncodePool := nil;

end.
