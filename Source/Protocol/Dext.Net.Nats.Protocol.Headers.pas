unit Dext.Net.Nats.Protocol.Headers;

interface

uses
  System.SysUtils,
  Dext.Net.Nats.Protocol;

function NatsEncodeHeaderBlock(const AHeaders: TNatsHeaders): TBytes;
procedure NatsDecodeHeaderBlock(const ABlock: string;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer);

implementation

uses
  System.Classes;

function NatsEncodeHeaderBlock(const AHeaders: TNatsHeaders): TBytes;
var
  S: TStringBuilder;
  I: Integer;
begin
  S := TStringBuilder.Create;
  try
    S.Append(NATS_HEADER_VERSION).Append(NATS_CRLF);
    for I := 0 to High(AHeaders) do
      S.Append(AHeaders[I].Key).Append(': ').Append(AHeaders[I].Value)
        .Append(NATS_CRLF);
    S.Append(NATS_CRLF);
    Result := TEncoding.UTF8.GetBytes(S.ToString);
  finally
    S.Free;
  end;
end;

procedure NatsDecodeHeaderBlock(const ABlock: string;
  out AHeaders: TNatsHeaders; out AStatusCode: Integer);
var
  Lines: TArray<string>;
  I, P: Integer;
  First, Name, Value: string;
begin
  AHeaders := nil;
  AStatusCode := 0;
  Lines := ABlock.Split([NATS_CRLF]);
  if Length(Lines) = 0 then Exit;

  First := Trim(Lines[0]);
  if First.StartsWith(NATS_HEADER_VERSION + ' ') then
    AStatusCode := StrToIntDef(Trim(Copy(First,
      Length(NATS_HEADER_VERSION) + 2, 3)), 0);

  for I := 1 to High(Lines) do
  begin
    if Lines[I] = '' then Break;
    P := Pos(':', Lines[I]);
    if P <= 1 then Continue;
    Name := Trim(Copy(Lines[I], 1, P - 1));
    Value := Trim(Copy(Lines[I], P + 1, MaxInt));
    AHeaders.Add(Name, Value);
  end;
end;

end.
