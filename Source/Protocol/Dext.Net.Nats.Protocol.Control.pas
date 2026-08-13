unit Dext.Net.Nats.Protocol.Control;

interface

uses
  System.SysUtils;

function NatsControlPing: TBytes;
function NatsControlPong: TBytes;
function NatsControlSub(const ASubject, AQueue: string; ASid: Integer): TBytes;
function NatsControlUnsub(ASid: Integer; AMaxMsgs: Integer = 0): TBytes;

implementation

uses
  Dext.Collections.Pool;

type
  PNatsControlWriter = ^TNatsControlWriter;
  TNatsControlWriter = record
  private
    FBuffer: TBytes;
    FLength: Integer;
    procedure EnsureCapacity(AAdditional: Integer);
  public
    procedure Reset;
    procedure WriteByte(AValue: Byte);
    procedure WriteAscii(const AValue: RawByteString);
    procedure WriteUtf8(const AValue: string);
    procedure WriteInt(AValue: Integer);
    procedure WriteCrLf;
    function ToBytes: TBytes;
  end;

  /// <summary>
  ///   TNatsControlWriter is a record (TDextPool requires class + constructor).
  ///   Wrapper reuses SUB/UNSUB encode scratch. PING/PONG stay interned.
  /// </summary>
  TDextNatsControlScratch = class
  public
    Writer: TNatsControlWriter;
    constructor Create;
  end;

var
  GNatsPingFrame: TBytes;
  GNatsPongFrame: TBytes;
  GControlPool: IDextPool<TDextNatsControlScratch>;

procedure TNatsControlWriter.Reset;
begin
  FLength := 0;
end;

procedure TNatsControlWriter.EnsureCapacity(AAdditional: Integer);
var
  Needed, NewCapacity: Integer;
begin
  Needed := FLength + AAdditional;
  if Needed <= Length(FBuffer) then
    Exit;

  NewCapacity := Length(FBuffer);
  if NewCapacity < 64 then
    NewCapacity := 64;
  while NewCapacity < Needed do
    NewCapacity := NewCapacity * 2;
  SetLength(FBuffer, NewCapacity);
end;

procedure TNatsControlWriter.WriteByte(AValue: Byte);
begin
  EnsureCapacity(1);
  FBuffer[FLength] := AValue;
  Inc(FLength);
end;

procedure TNatsControlWriter.WriteAscii(const AValue: RawByteString);
begin
  if AValue = '' then
    Exit;
  EnsureCapacity(Length(AValue));
  Move(AValue[1], FBuffer[FLength], Length(AValue));
  Inc(FLength, Length(AValue));
end;

procedure TNatsControlWriter.WriteUtf8(const AValue: string);
var
  Bytes: TBytes;
begin
  if AValue = '' then
    Exit;
  Bytes := TEncoding.UTF8.GetBytes(AValue);
  EnsureCapacity(Length(Bytes));
  Move(Bytes[0], FBuffer[FLength], Length(Bytes));
  Inc(FLength, Length(Bytes));
end;

procedure TNatsControlWriter.WriteInt(AValue: Integer);
var
  Buffer: array[0..11] of AnsiChar;
  P: Integer;
  Value: Cardinal;
  Negative: Boolean;
begin
  if AValue = 0 then
  begin
    WriteByte(Ord('0'));
    Exit;
  end;

  if AValue = Low(Integer) then
  begin
    WriteAscii('-2147483648');
    Exit;
  end;

  Negative := AValue < 0;
  if Negative then
    Value := Cardinal(-AValue)
  else
    Value := Cardinal(AValue);

  P := High(Buffer);
  while Value > 0 do
  begin
    Buffer[P] := AnsiChar(Ord('0') + (Value mod 10));
    Value := Value div 10;
    Dec(P);
  end;
  if Negative then
  begin
    Buffer[P] := '-';
    Dec(P);
  end;
  Inc(P);

  EnsureCapacity(High(Buffer) - P + 1);
  Move(Buffer[P], FBuffer[FLength], High(Buffer) - P + 1);
  Inc(FLength, High(Buffer) - P + 1);
end;

procedure TNatsControlWriter.WriteCrLf;
begin
  WriteByte(13);
  WriteByte(10);
end;

function TNatsControlWriter.ToBytes: TBytes;
begin
  SetLength(Result, FLength);
  if FLength > 0 then
    Move(FBuffer[0], Result[0], FLength);
end;

constructor TDextNatsControlScratch.Create;
begin
  inherited Create;
  Writer.Reset;
end;

function AcquireControlWriter(out Scratch: TDextNatsControlScratch;
  var Local: TNatsControlWriter): PNatsControlWriter;
begin
  Scratch := nil;
  if (GControlPool <> nil) and GControlPool.Acquire(Scratch) then
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

procedure ReleaseControlWriter(Scratch: TDextNatsControlScratch);
begin
  if (Scratch <> nil) and (GControlPool <> nil) then
    GControlPool.Release(Scratch);
end;

function NatsControlPing: TBytes;
begin
  Result := GNatsPingFrame;
end;

function NatsControlPong: TBytes;
begin
  Result := GNatsPongFrame;
end;

function NatsControlSub(const ASubject, AQueue: string;
  ASid: Integer): TBytes;
var
  Scratch: TDextNatsControlScratch;
  Local: TNatsControlWriter;
  Writer: PNatsControlWriter;
begin
  Writer := AcquireControlWriter(Scratch, Local);
  try
    Writer^.WriteAscii('SUB ');
    Writer^.WriteUtf8(ASubject);
    Writer^.WriteByte(Ord(' '));
    if AQueue <> '' then
    begin
      Writer^.WriteUtf8(AQueue);
      Writer^.WriteByte(Ord(' '));
    end;
    Writer^.WriteInt(ASid);
    Writer^.WriteCrLf;
    Result := Writer^.ToBytes;
  finally
    ReleaseControlWriter(Scratch);
  end;
end;

function NatsControlUnsub(ASid, AMaxMsgs: Integer): TBytes;
var
  Scratch: TDextNatsControlScratch;
  Local: TNatsControlWriter;
  Writer: PNatsControlWriter;
begin
  Writer := AcquireControlWriter(Scratch, Local);
  try
    Writer^.WriteAscii('UNSUB ');
    Writer^.WriteInt(ASid);
    if AMaxMsgs > 0 then
    begin
      Writer^.WriteByte(Ord(' '));
      Writer^.WriteInt(AMaxMsgs);
    end;
    Writer^.WriteCrLf;
    Result := Writer^.ToBytes;
  finally
    ReleaseControlWriter(Scratch);
  end;
end;

procedure InitControlPool;
var
  Config: TDextPoolConfig;
begin
  Config := TDextPoolConfig.Default;
  Config.MinSize := 1;
  Config.MaxSize := 8;
  Config.AcquireTimeoutMs := 0;
  GControlPool := TDextPool<TDextNatsControlScratch>.Create(Config,
    procedure(Item: TDextNatsControlScratch)
    begin
      Item.Writer.Reset;
    end);
end;

initialization
  GNatsPingFrame := BytesOf(RawByteString('PING'#13#10));
  GNatsPongFrame := BytesOf(RawByteString('PONG'#13#10));
  InitControlPool;

finalization
  GControlPool := nil;

end.
