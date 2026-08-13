unit Dext.Net.Nats.Protocol.Headers;

interface

uses
  System.SysUtils,
  Dext.Core.Span,
  Dext.Net.Nats.Protocol;

/// <summary>
///   Encodes <see cref="TNatsHeaders"/> as a NATS/1.0 header block (CRLF lines,
///   terminating blank line) directly into UTF-8 bytes. Public header names and
///   values remain <c>string</c>.
/// </summary>
function NatsEncodeHeaderBlock(const AHeaders: TNatsHeaders): TBytes;

procedure NatsDecodeHeaderBlock(const ABlock: TByteSpan;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer); overload;
procedure NatsDecodeHeaderBlock(const ABlock: TBytes;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer); overload;
procedure NatsDecodeHeaderBlock(const ABlock: string;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer); overload;

implementation

const
  NATS_HEADER_VERSION_UTF8: RawByteString = 'NATS/1.0';

procedure EnsureCapacity(var ABuf: TBytes; var ALen: Integer; ANeeded: Integer);
var
  Cap: Integer;
begin
  if ALen + ANeeded <= Length(ABuf) then
    Exit;
  Cap := Length(ABuf);
  if Cap < 256 then
    Cap := 256;
  while ALen + ANeeded > Cap do
  begin
    if Cap > (MaxInt div 2) then
    begin
      Cap := ALen + ANeeded;
      Break;
    end;
    Cap := Cap * 2;
  end;
  SetLength(ABuf, Cap);
end;

procedure WriteRaw(var ABuf: TBytes; var ALen: Integer; AData: Pointer; ACount: Integer);
begin
  if (AData = nil) or (ACount <= 0) then
    Exit;
  EnsureCapacity(ABuf, ALen, ACount);
  Move(AData^, ABuf[ALen], ACount);
  Inc(ALen, ACount);
end;

procedure WriteAsciiConst(var ABuf: TBytes; var ALen: Integer; const AText: RawByteString);
begin
  if Length(AText) > 0 then
    WriteRaw(ABuf, ALen, @AText[1], Length(AText));
end;

procedure WriteCrLf(var ABuf: TBytes; var ALen: Integer);
begin
  EnsureCapacity(ABuf, ALen, 2);
  ABuf[ALen] := 13;
  ABuf[ALen + 1] := 10;
  Inc(ALen, 2);
end;

procedure WriteUtf8(var ABuf: TBytes; var ALen: Integer; const S: string);
var
  N, I, Written: Integer;
  Ascii: Boolean;
  P: PByte;
begin
  N := Length(S);
  if N = 0 then
    Exit;

  Ascii := True;
  for I := 1 to N do
    if Ord(S[I]) > 127 then
    begin
      Ascii := False;
      Break;
    end;

  if Ascii then
  begin
    EnsureCapacity(ABuf, ALen, N);
    P := @ABuf[ALen];
    for I := 1 to N do
    begin
      P^ := Byte(Ord(S[I]));
      Inc(P);
    end;
    Inc(ALen, N);
    Exit;
  end;

  Written := TEncoding.UTF8.GetByteCount(S);
  EnsureCapacity(ABuf, ALen, Written);
  Written := TEncoding.UTF8.GetBytes(S, 1, N, ABuf, ALen);
  Inc(ALen, Written);
end;

function NatsEncodeHeaderBlock(const AHeaders: TNatsHeaders): TBytes;
var
  Buf: TBytes;
  Len, I: Integer;
begin
  Len := 0;
  WriteAsciiConst(Buf, Len, NATS_HEADER_VERSION_UTF8);
  WriteCrLf(Buf, Len);
  for I := 0 to High(AHeaders) do
  begin
    WriteUtf8(Buf, Len, AHeaders[I].Key);
    WriteAsciiConst(Buf, Len, ': ');
    WriteUtf8(Buf, Len, AHeaders[I].Value);
    WriteCrLf(Buf, Len);
  end;
  WriteCrLf(Buf, Len);
  SetLength(Result, Len);
  if Len > 0 then
    Move(Buf[0], Result[0], Len);
end;

function IndexOfCrLf(const ASpan: TByteSpan): Integer;
var
  I: Integer;
begin
  I := 0;
  while I < ASpan.Length - 1 do
  begin
    if (ASpan.Data[I] = 13) and (ASpan.Data[I + 1] = 10) then
      Exit(I);
    Inc(I);
  end;
  Result := -1;
end;

function TrimByteSpan(const ASpan: TByteSpan): TByteSpan;
var
  Start, Stop: Integer;
begin
  Start := 0;
  Stop := ASpan.Length;
  while (Start < Stop) and (ASpan.Data[Start] in [9, 32]) do
    Inc(Start);
  while (Stop > Start) and (ASpan.Data[Stop - 1] in [9, 32]) do
    Dec(Stop);
  if (Start = 0) and (Stop = ASpan.Length) then
    Exit(ASpan);
  Result := ASpan.Slice(Start, Stop - Start);
end;

function SpanToUtf8(const ASpan: TByteSpan): string;
var
  Bytes: TBytes;
begin
  if ASpan.Length <= 0 then
    Exit('');
  SetLength(Bytes, ASpan.Length);
  Move(ASpan.Data^, Bytes[0], ASpan.Length);
  Result := TEncoding.UTF8.GetString(Bytes);
end;

function SpanStartsWithVersion(const ALine: TByteSpan): Boolean;
var
  Prefix: TByteSpan;
begin
  if ALine.Length < Length(NATS_HEADER_VERSION_UTF8) then
    Exit(False);
  Prefix := ALine.Slice(0, Length(NATS_HEADER_VERSION_UTF8));
  Result := Prefix.EqualsString(NATS_HEADER_VERSION);
end;

procedure ParseStatusLine(const ALine: TByteSpan; out AStatusCode: Integer);
var
  I, Digits: Integer;
  Acc: Integer;
begin
  AStatusCode := 0;
  if not SpanStartsWithVersion(ALine) then
    Exit;
  I := Length(NATS_HEADER_VERSION_UTF8);
  while (I < ALine.Length) and (ALine.Data[I] in [9, 32]) do
    Inc(I);
  Acc := 0;
  Digits := 0;
  while (I < ALine.Length) and (Digits < 3) and (ALine.Data[I] in [Ord('0')..Ord('9')]) do
  begin
    Acc := Acc * 10 + (ALine.Data[I] - Ord('0'));
    Inc(I);
    Inc(Digits);
  end;
  if Digits = 3 then
    AStatusCode := Acc;
end;

procedure ParseNameValue(const ALine: TByteSpan; var AHeaders: TNatsHeaders);
var
  Colon: Integer;
  Name, Value: TByteSpan;
begin
  Colon := ALine.IndexOf(Ord(':'));
  if Colon <= 0 then
    Exit;
  Name := TrimByteSpan(ALine.Slice(0, Colon));
  if Colon + 1 >= ALine.Length then
    Value := Default(TByteSpan)
  else
    Value := TrimByteSpan(ALine.Slice(Colon + 1));
  if Name.Length = 0 then
    Exit;
  AHeaders.Add(SpanToUtf8(Name), SpanToUtf8(Value));
end;

procedure NatsDecodeHeaderBlock(const ABlock: TByteSpan;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer);
var
  Rest, Line, Trimmed: TByteSpan;
  Idx: Integer;
  First: Boolean;
begin
  AHeaders := nil;
  AStatusCode := 0;
  if ABlock.Length <= 0 then
    Exit;

  Rest := ABlock;
  First := True;
  while Rest.Length > 0 do
  begin
    Idx := IndexOfCrLf(Rest);
    if Idx < 0 then
      Line := Rest
    else
      Line := Rest.Slice(0, Idx);

    if First then
      ParseStatusLine(Line, AStatusCode)
    else
    begin
      Trimmed := TrimByteSpan(Line);
      if Trimmed.Length = 0 then
        Break;
      ParseNameValue(Trimmed, AHeaders);
    end;

    First := False;
    if Idx < 0 then
      Break;
    Rest := Rest.Slice(Idx + 2);
  end;
end;

procedure NatsDecodeHeaderBlock(const ABlock: TBytes;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer);
begin
  NatsDecodeHeaderBlock(TByteSpan.FromBytes(ABlock), AHeaders, AStatusCode);
end;

procedure NatsDecodeHeaderBlock(const ABlock: string;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer);
var
  Bytes: TBytes;
begin
  if ABlock = '' then
  begin
    AHeaders := nil;
    AStatusCode := 0;
    Exit;
  end;
  Bytes := TEncoding.UTF8.GetBytes(ABlock);
  NatsDecodeHeaderBlock(TByteSpan.FromBytes(Bytes), AHeaders, AStatusCode);
end;

end.
