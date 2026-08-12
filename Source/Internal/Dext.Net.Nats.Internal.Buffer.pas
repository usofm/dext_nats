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
  Dext.Core.Span;

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
    procedure EnsureWritable(ACount: Integer);
    procedure Compact;
  public
    constructor Create(AInitialCapacity: Integer = 4096);

    procedure Append(const AData: TByteSpan); overload;
    procedure Append(const AData: TBytes; ACount: Integer); overload;
    procedure Consume(ACount: Integer);
    procedure Clear;

    function Available: Integer;
    function DataSpan: TByteSpan;
    function Capacity: Integer;
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
    Move(FBuffer[FReadPos], FBuffer[0], Count);
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

function TDextNatsReadBuffer.DataSpan: TByteSpan;
var
  Count: Integer;
begin
  Count := Available;
  if Count = 0 then
    Exit(Default(TByteSpan));
  Result := TByteSpan.Create(@FBuffer[FReadPos], Count);
end;

end.
