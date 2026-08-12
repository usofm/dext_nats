{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Cursor-based internal NATS frame parser                         }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Internal.Parser;

interface

uses
  System.SysUtils,
  Dext.Core.Span,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Internal.Buffer;

type
  /// <summary>
  ///   Cursor-based successor to TDextNatsFrameParser. It preserves the
  ///   existing owned TNatsFrame contract while consuming bytes by advancing
  ///   TDextNatsReadBuffer rather than shifting the unread tail after every
  ///   frame. This type stays internal until parity tests and Delphi builds
  ///   prove it can replace the current parser safely.
  /// </summary>
  TDextNatsFrameParserV2 = class
  private
    FBuffer: TDextNatsReadBuffer;
    FMaxFrameBytes: Int64;
    FHasPendingControl: Boolean;
    FPendingFrame: TNatsFrame;
    FPendingLineLength: Integer;
    FPendingHeaderBytes: Integer;
    FPendingTotalBytes: Integer;

    function OpcodeEquals(AOffset, ALength: Integer; const AOpcode: RawByteString): Boolean;
    function NextToken(var APos: Integer; ALineEnd: Integer;
      out ATokStart, ATokLen: Integer): Boolean;
    function TryParseIntDec(AOffset, ALength: Integer; out AValue: Integer): Boolean;
    function ParseControlLine(AOffset, ALength: Integer; out AFrame: TNatsFrame;
      out AHeaderBytes, ATotalBytes: Integer): Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Append(const AData: TByteSpan); overload;
    procedure Append(const AData: TBytes; ACount: Integer); overload;
    procedure Clear;
    function TryReadFrame(out AFrame: TNatsFrame): Boolean;

    property MaxFrameBytes: Int64 read FMaxFrameBytes write FMaxFrameBytes;
  end;

implementation

constructor TDextNatsFrameParserV2.Create;
begin
  inherited Create;
  FBuffer := TDextNatsReadBuffer.Create(4096);
  FMaxFrameBytes := NATS_MAX_FRAME_BYTES;
end;

destructor TDextNatsFrameParserV2.Destroy;
begin
  FBuffer.Free;
  inherited;
end;

procedure TDextNatsFrameParserV2.Append(const AData: TByteSpan);
begin
  FBuffer.Append(AData);
end;

procedure TDextNatsFrameParserV2.Append(const AData: TBytes; ACount: Integer);
begin
  FBuffer.Append(AData, ACount);
end;

procedure TDextNatsFrameParserV2.Clear;
begin
  FBuffer.Clear;
  FHasPendingControl := False;
  FPendingFrame := Default(TNatsFrame);
  FPendingLineLength := 0;
  FPendingHeaderBytes := 0;
  FPendingTotalBytes := 0;
end;

function TDextNatsFrameParserV2.OpcodeEquals(AOffset, ALength: Integer;
  const AOpcode: RawByteString): Boolean;
var
  I: Integer;
begin
  if ALength <> Length(AOpcode) then
    Exit(False);
  for I := 1 to ALength do
    if FBuffer.ByteAt(AOffset + I - 1) <> Byte(AOpcode[I]) then
      Exit(False);
  Result := True;
end;

function TDextNatsFrameParserV2.NextToken(var APos: Integer; ALineEnd: Integer;
  out ATokStart, ATokLen: Integer): Boolean;
begin
  while (APos < ALineEnd) and (FBuffer.ByteAt(APos) = Ord(' ')) do
    Inc(APos);
  if APos >= ALineEnd then
    Exit(False);

  ATokStart := APos;
  while (APos < ALineEnd) and (FBuffer.ByteAt(APos) <> Ord(' ')) do
    Inc(APos);
  ATokLen := APos - ATokStart;
  Result := True;
end;

function TDextNatsFrameParserV2.TryParseIntDec(AOffset, ALength: Integer;
  out AValue: Integer): Boolean;
var
  I, D, V: Integer;
  Neg: Boolean;
begin
  AValue := 0;
  if ALength <= 0 then
    Exit(False);

  I := 0;
  Neg := False;
  if FBuffer.ByteAt(AOffset) = Ord('-') then
  begin
    Neg := True;
    Inc(I);
    if I >= ALength then
      Exit(False);
  end;

  V := 0;
  while I < ALength do
  begin
    D := FBuffer.ByteAt(AOffset + I) - Ord('0');
    if (D < 0) or (D > 9) then
      Exit(False);
    if V > (MaxInt - D) div 10 then
      Exit(False);
    V := V * 10 + D;
    Inc(I);
  end;

  if Neg then
    AValue := -V
  else
    AValue := V;
  Result := True;
end;

function TDextNatsFrameParserV2.ParseControlLine(AOffset, ALength: Integer;
  out AFrame: TNatsFrame; out AHeaderBytes, ATotalBytes: Integer): Boolean;
var
  Pos, LineEnd: Integer;
  OpStart, OpLen: Integer;
  T1S, T1L, T2S, T2L, T3S, T3L, T4S, T4L, T5S, T5L: Integer;
  TokCount: Integer;
  ErrText: string;
begin
  AHeaderBytes := 0;
  ATotalBytes := 0;
  AFrame := Default(TNatsFrame);

  if ALength <= 0 then
    Exit(False);

  LineEnd := AOffset + ALength;
  Pos := AOffset;
  if not NextToken(Pos, LineEnd, OpStart, OpLen) then
    Exit(False);

  if OpcodeEquals(OpStart, OpLen, 'PING') then
  begin
    AFrame.Kind := nfPing;
    Result := True;
  end
  else if OpcodeEquals(OpStart, OpLen, 'PONG') then
  begin
    AFrame.Kind := nfPong;
    Result := True;
  end
  else if OpcodeEquals(OpStart, OpLen, '+OK') then
  begin
    AFrame.Kind := nfOK;
    Result := True;
  end
  else if OpcodeEquals(OpStart, OpLen, '-ERR') then
  begin
    AFrame.Kind := nfErr;
    ErrText := Trim(FBuffer.Utf8String(Pos, LineEnd - Pos));
    AFrame.ErrorText := ErrText.Trim(['''']);
    Result := True;
  end
  else if OpcodeEquals(OpStart, OpLen, 'INFO') then
  begin
    AFrame.Kind := nfInfo;
    AFrame.InfoJson := Trim(FBuffer.Utf8String(Pos, LineEnd - Pos));
    Result := True;
  end
  else if OpcodeEquals(OpStart, OpLen, 'MSG') then
  begin
    AFrame.Kind := nfMsg;
    TokCount := 0;
    if NextToken(Pos, LineEnd, T1S, T1L) then Inc(TokCount) else T1L := 0;
    if NextToken(Pos, LineEnd, T2S, T2L) then Inc(TokCount) else T2L := 0;
    if NextToken(Pos, LineEnd, T3S, T3L) then Inc(TokCount) else T3L := 0;
    if NextToken(Pos, LineEnd, T4S, T4L) then Inc(TokCount) else T4L := 0;
    if NextToken(Pos, LineEnd, T5S, T5L) then
      Exit(False);

    if (TokCount < 3) or (TokCount > 4) then
      Exit(False);

    AFrame.Subject := FBuffer.Utf8String(T1S, T1L);
    if not TryParseIntDec(T2S, T2L, AFrame.Sid) then
      Exit(False);

    if TokCount = 3 then
    begin
      if not TryParseIntDec(T3S, T3L, ATotalBytes) then
        Exit(False);
    end
    else
    begin
      AFrame.ReplyTo := FBuffer.Utf8String(T3S, T3L);
      if not TryParseIntDec(T4S, T4L, ATotalBytes) then
        Exit(False);
    end;

    Result := ATotalBytes >= 0;
  end
  else if OpcodeEquals(OpStart, OpLen, 'HMSG') then
  begin
    AFrame.Kind := nfHMsg;
    TokCount := 0;
    if NextToken(Pos, LineEnd, T1S, T1L) then Inc(TokCount) else T1L := 0;
    if NextToken(Pos, LineEnd, T2S, T2L) then Inc(TokCount) else T2L := 0;
    if NextToken(Pos, LineEnd, T3S, T3L) then Inc(TokCount) else T3L := 0;
    if NextToken(Pos, LineEnd, T4S, T4L) then Inc(TokCount) else T4L := 0;
    if NextToken(Pos, LineEnd, T5S, T5L) then Inc(TokCount) else T5L := 0;
    if NextToken(Pos, LineEnd, OpStart, OpLen) then
      Exit(False);

    if (TokCount < 4) or (TokCount > 5) then
      Exit(False);

    AFrame.Subject := FBuffer.Utf8String(T1S, T1L);
    if not TryParseIntDec(T2S, T2L, AFrame.Sid) then
      Exit(False);

    if TokCount = 4 then
    begin
      if not TryParseIntDec(T3S, T3L, AHeaderBytes) then
        Exit(False);
      if not TryParseIntDec(T4S, T4L, ATotalBytes) then
        Exit(False);
    end
    else
    begin
      AFrame.ReplyTo := FBuffer.Utf8String(T3S, T3L);
      if not TryParseIntDec(T4S, T4L, AHeaderBytes) then
        Exit(False);
      if not TryParseIntDec(T5S, T5L, ATotalBytes) then
        Exit(False);
    end;

    Result := (AHeaderBytes >= 0) and (ATotalBytes >= AHeaderBytes);
  end
  else
    Result := False;

  if Result and ((AHeaderBytes > FMaxFrameBytes) or
    (ATotalBytes > FMaxFrameBytes)) then
    raise EDextNatsProtocolError.CreateFmt(
      'NATS server announced a frame of %d bytes, which exceeds the configured safety limit of %d bytes',
      [ATotalBytes, FMaxFrameBytes]);
end;

function TDextNatsFrameParserV2.TryReadFrame(out AFrame: TNatsFrame): Boolean;
var
  CrLfIdx: Integer;
  Line: string;
  HeaderBytes, TotalBytes: Integer;
  NeededBytes: Integer;
  HeaderBlock: TBytes;
  Payload: TBytes;
  PayloadLen: Integer;
begin
  AFrame := Default(TNatsFrame);

  if not FHasPendingControl then
  begin
    CrLfIdx := FBuffer.IndexOfCrLf(0);
    if CrLfIdx < 0 then
      Exit(False);

    if not ParseControlLine(0, CrLfIdx, FPendingFrame,
      HeaderBytes, TotalBytes) then
    begin
      Line := FBuffer.Utf8String(0, CrLfIdx);
      FBuffer.Consume(CrLfIdx + 2);
      raise EDextNatsProtocolError.CreateFmt(
        'Malformed NATS protocol line: "%s"', [Line]);
    end;

    FPendingLineLength := CrLfIdx + 2;
    FPendingHeaderBytes := HeaderBytes;
    FPendingTotalBytes := TotalBytes;
    FHasPendingControl := True;
  end;

  if not (FPendingFrame.Kind in [nfMsg, nfHMsg]) then
  begin
    AFrame := FPendingFrame;
    FBuffer.Consume(FPendingLineLength);
    FHasPendingControl := False;
    Exit(True);
  end;

  NeededBytes := FPendingLineLength + FPendingTotalBytes + 2;
  if FBuffer.Available < NeededBytes then
    Exit(False);

  if FPendingFrame.Kind = nfHMsg then
  begin
    FBuffer.CopyTo(FPendingLineLength, HeaderBlock, FPendingHeaderBytes);
    NatsParseHeaderBlock(TEncoding.UTF8.GetString(HeaderBlock),
      FPendingFrame.Headers, FPendingFrame.StatusCode);

    PayloadLen := FPendingTotalBytes - FPendingHeaderBytes;
    FBuffer.CopyTo(FPendingLineLength + FPendingHeaderBytes,
      Payload, PayloadLen);
    FPendingFrame.Payload := Payload;
  end
  else
  begin
    FBuffer.CopyTo(FPendingLineLength, Payload, FPendingTotalBytes);
    FPendingFrame.Payload := Payload;
  end;

  AFrame := FPendingFrame;
  FBuffer.Consume(NeededBytes);
  FHasPendingControl := False;
  Result := True;
end;

end.
