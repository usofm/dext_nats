{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream paging helpers                                        }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Paging;

interface

uses
  System.SysUtils;

type
  TNatsJsNamePage = record
    Total: Integer;
    Offset: Integer;
    Limit: Integer;
    Items: TArray<string>;
  end;

function NatsJsParseNamePage(const AJson, AArrayName: string): TNatsJsNamePage; overload;
function NatsJsParseNamePage(const AJson: TBytes; const AArrayName: string): TNatsJsNamePage; overload;

implementation

uses
  Dext.Core.Span,
  Dext.Json.Utf8,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.JetStream;

procedure SkipValue(var AReader: TUtf8JsonReader);
begin
  if AReader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
    AReader.Skip;
end;

procedure RaiseApiError(var AReader: TUtf8JsonReader);
var
  Code, ErrCode: Integer;
  Description: string;
begin
  Code := 0;
  ErrCode := 0;
  Description := '';
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    if AReader.ValueSpanEquals('code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        Code := AReader.GetInt32
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('err_code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        ErrCode := AReader.GetInt32
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('description') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        Description := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.Read then
      SkipValue(AReader);
  end;
  raise EDextNatsJetStreamError.CreateFromApi(Code, ErrCode, Description);
end;

function NatsJsParseNamePage(const AJson, AArrayName: string): TNatsJsNamePage;
begin
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty JetStream paged response');
  Result := NatsJsParseNamePage(TEncoding.UTF8.GetBytes(AJson), AArrayName);
end;

function NatsJsParseNamePage(const AJson: TBytes; const AArrayName: string): TNatsJsNamePage;
var
  Span: TByteSpan;
  Reader: TUtf8JsonReader;
  Items: TArray<string>;
  Count: Integer;
begin
  Result := Default(TNatsJsNamePage);
  if Length(AJson) = 0 then
    raise EDextNatsProtocolError.Create('Empty JetStream paged response');

  Span := TByteSpan.FromBytes(AJson);
  Reader := TUtf8JsonReader.Create(Span);
  if (not Reader.Read) or (Reader.TokenType <> TJsonTokenType.StartObject) then
    raise EDextNatsProtocolError.Create('Malformed JetStream paged response');

  SetLength(Items, 0);
  while Reader.Read do
  begin
    if Reader.TokenType = TJsonTokenType.EndObject then
      Break;
    if Reader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if Reader.ValueSpanEquals('error') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.StartObject) then
        RaiseApiError(Reader)
      else
        SkipValue(Reader);
    end
    else if Reader.ValueSpanEquals('total') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Total := Reader.GetInt32
      else
        SkipValue(Reader);
    end
    else if Reader.ValueSpanEquals('offset') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Offset := Reader.GetInt32
      else
        SkipValue(Reader);
    end
    else if Reader.ValueSpanEquals('limit') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Limit := Reader.GetInt32
      else
        SkipValue(Reader);
    end
    else if Reader.ValueSpanEquals(AArrayName) then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.StartArray) then
      begin
        Count := 0;
        while Reader.Read do
        begin
          if Reader.TokenType = TJsonTokenType.EndArray then
            Break;
          if Reader.TokenType = TJsonTokenType.StringValue then
          begin
            SetLength(Items, Count + 1);
            Items[Count] := Reader.GetString;
            Inc(Count);
          end
          else
            SkipValue(Reader);
        end;
        Result.Items := Items;
      end
      else
        SkipValue(Reader);
    end
    else if Reader.Read then
      SkipValue(Reader);
  end;
end;

end.
