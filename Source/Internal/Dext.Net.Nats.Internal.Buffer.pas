{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Internal allocation-conscious read buffer                       }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Internal.Buffer;

interface

uses
  System.SysUtils,
  Dext.Core.Span,
  Dext.Collections.Simd;

type
  /// <summary>
  ///   Growable byte buffer with independent read/write cursors. Consuming
  ///   bytes advances a cursor instead of shifting the unread tail after
  ///   every frame. The buffer compacts only when additional writable space
  ///   is required, eliminating the parser's O(n) Move-per-frame pattern.
  /// </summary>
  TDextNatsReadBuffer = class
  private
    FBuffer: TBytes;
    FReadPos: Integer;
    FWritePos: Integer;
    FCompactionCount: Int64;
    procedure EnsureWritable(ACount: Integer);
    procedure Compact;
  public
    constructor Create(AInitialCapacity: Integer = 4096);

    procedure Append(const AData: TByteSpan); overload;
    procedure Append(const AData: TBytes; ACount: Integer); overload;
    procedure Consume(ACount: Integer);
    procedure Clear;

    /// <summary>
    ///   Writable tail for a socket/TLS fill. When the buffer is empty the
    ///   tail is sized to at least <paramref name="AMinLength"/>. When unread
    ///   bytes remain, the existing free tail is returned so a partial frame
    ///   does not force a 64 KiB growth. Commit with <see cref="CommitWrite"/>.
    /// </summary>
    function WritableSpan(AMinLength: Integer): TByteSpan;
    /// <summary>Advance the write cursor after filling <see cref="WritableSpan"/>.</summary>
    procedure CommitWrite(ACount: Integer);

    function Available: Integer;
    function DataSpan: TByteSpan;
    function Capacity: Integer;
    function ByteAt(AOffset: Integer): Byte; inline;
    function IndexOfCrLf(AStartOffset: Integer = 0): Integer;
    procedure CopyTo(AOffset: Integer; var ADest: TBytes; ACount: Integer);
    function Utf8String(AOffset, ACount: Integer): string;

    /// <summary>Number of physical unread-tail compactions since construction.</summary>
    property CompactionCount: Int64 read FCompactionCount;
  end;

implementation

constructor TDextNatsReadBuffer.Create(AInitialCapacity: Integer);
begin
  inherited Create;
  if AInitialCapacity < 256 then
    AInitialCapacity := 256;
  SetLength(FBuffer, AInitialCapacity);
end;

function TDextNatsReadBuffer.Available: Integer;
begin
  Result := FWritePos - FReadPos;
end;

function TDextNatsReadBuffer.Capacity: Integer;
begin
  Result := Length(FBuffer);
end;

function TDextNatsReadBuffer.ByteAt(AOffset: Integer): Byte;
begin
  if (AOffset < 0) or (AOffset >= Available) then
    raise EArgumentOutOfRangeException.Create('Buffer offset is outside unread data');
  Result := FBuffer[FReadPos + AOffset];
end;

procedure TDextNatsReadBuffer.Clear;
begin
  FReadPos := 0;
  FWritePos := 0;
end;

procedure TDextNatsReadBuffer.Compact;
var
  Count: Integer;
begin
  Count := Available;
  if (Count > 0) and (FReadPos > 0) then
  begin
    Move(FBuffer[FReadPos], FBuffer[0], Count);
    Inc(FCompactionCount);
  end;
  FReadPos := 0;
  FWritePos := Count;
end;

procedure TDextNatsReadBuffer.EnsureWritable(ACount: Integer);
var
  Needed: Integer;
  NewCapacity: Integer;
begin
  if ACount <= 0 then
    Exit;

  if Length(FBuffer) - FWritePos >= ACount then
    Exit;

  { Reclaim consumed prefix before allocating a larger array. }
  if (FReadPos > 0) and (Length(FBuffer) - Available >= ACount) then
  begin
    Compact;
    Exit;
  end;

  Needed := Available + ACount;
  NewCapacity := Length(FBuffer);
  if NewCapacity = 0 then
    NewCapacity := 256;
  while NewCapacity < Needed do
  begin
    if NewCapacity > (MaxInt div 2) then
    begin
      NewCapacity := Needed;
      Break;
    end;
    NewCapacity := NewCapacity * 2;
  end;

  Compact;
  SetLength(FBuffer, NewCapacity);
end;

function TDextNatsReadBuffer.WritableSpan(AMinLength: Integer): TByteSpan;
var
  FreeTail: Integer;
begin
  if AMinLength < 1 then
    AMinLength := 1;

  if Available = 0 then
  begin
    Clear;
    EnsureWritable(AMinLength);
  end
  else if Length(FBuffer) - FWritePos = 0 then
    EnsureWritable(AMinLength);

  FreeTail := Length(FBuffer) - FWritePos;
  if FreeTail <= 0 then
    Exit(Default(TByteSpan));
  Result := TByteSpan.Create(@FBuffer[FWritePos], FreeTail);
end;

procedure TDextNatsReadBuffer.CommitWrite(ACount: Integer);
begin
  if ACount < 0 then
    raise EArgumentOutOfRangeException.Create('ACount must be >= 0');
  if ACount = 0 then
    Exit;
  if FWritePos + ACount > Length(FBuffer) then
    raise EArgumentOutOfRangeException.Create('Commit exceeds writable capacity');
  Inc(FWritePos, ACount);
end;

procedure TDextNatsReadBuffer.Append(const AData: TByteSpan);
begin
  if AData.Length <= 0 then
    Exit;
  EnsureWritable(AData.Length);
  Move(AData.Data^, FBuffer[FWritePos], AData.Length);
  Inc(FWritePos, AData.Length);
end;

procedure TDextNatsReadBuffer.Append(const AData: TBytes; ACount: Integer);
var
  Span: TByteSpan;
begin
  if ACount <= 0 then
    Exit;
  if ACount > Length(AData) then
    raise EArgumentOutOfRangeException.Create('ACount exceeds source buffer length');
  Span := TByteSpan.Create(@AData[0], ACount);
  Append(Span);
end;

procedure TDextNatsReadBuffer.Consume(ACount: Integer);
begin
  if ACount < 0 then
    raise EArgumentOutOfRangeException.Create('ACount must be >= 0');
  if ACount > Available then
    raise EArgumentOutOfRangeException.Create('Cannot consume more bytes than available');

  Inc(FReadPos, ACount);
  if FReadPos = FWritePos then
    Clear;
end;

procedure TDextNatsReadBuffer.CopyTo(AOffset: Integer; var ADest: TBytes; ACount: Integer);
begin
  if (AOffset < 0) or (ACount < 0) or (AOffset + ACount > Available) then
    raise EArgumentOutOfRangeException.Create('Copy range is outside unread data');
  SetLength(ADest, ACount);
  if ACount > 0 then
    Move(FBuffer[FReadPos + AOffset], ADest[0], ACount);
end;

function TDextNatsReadBuffer.DataSpan: TByteSpan;
var
  Count: Integer;
begin
  Count := Available;
  if Count = 0 then
    Exit(Default(TByteSpan));
  Result := TByteSpan.Create(@FBuffer[FReadPos], Count);
end;

function TDextNatsReadBuffer.IndexOfCrLf(AStartOffset: Integer): Integer;
var
  Base: PByte;
  Remaining: Integer;
  Found: Integer;
  Rel: Integer;
begin
  if AStartOffset < 0 then
    AStartOffset := 0;
  Remaining := Available - AStartOffset;
  if Remaining < 2 then
    Exit(-1);

  Base := @FBuffer[FReadPos + AStartOffset];
  Rel := 0;
  while Rel <= Remaining - 2 do
  begin
    Found := TDextSimd.IndexOfByte(Base + Rel, Remaining - Rel, 13);
    if Found < 0 then
      Exit(-1);
    Inc(Rel, Found);
    if Rel + 1 >= Remaining then
      Exit(-1);
    if (Base + Rel + 1)^ = 10 then
      Exit(AStartOffset + Rel);
    Inc(Rel);
  end;
  Result := -1;
end;

function TDextNatsReadBuffer.Utf8String(AOffset, ACount: Integer): string;
var
  Bytes: TBytes;
begin
  CopyTo(AOffset, Bytes, ACount);
  if Length(Bytes) = 0 then
    Exit('');
  Result := TEncoding.UTF8.GetString(Bytes);
end;

end.
