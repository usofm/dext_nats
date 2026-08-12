{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream paged object response parser                          }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.ObjectPaging;

interface

uses
  Dext.Net.Nats.JetStream;

type
  TNatsJsStreamInfoPage = record
    Total: Integer;
    Offset: Integer;
    Limit: Integer;
    Items: TArray<TNatsStreamInfo>;
  end;

  TNatsJsConsumerInfoPage = record
    Total: Integer;
    Offset: Integer;
    Limit: Integer;
    Items: TArray<TNatsConsumerInfo>;
  end;

function NatsJsParseStreamInfoPage(const AJson: string): TNatsJsStreamInfoPage;
function NatsJsParseConsumerInfoPage(const AJson: string): TNatsJsConsumerInfoPage;

implementation

uses
  System.SysUtils,
  Dext.Core.Span,
  Dext.Json.Utf8,
  Dext.Net.Nats,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.JetStream.Json,
  Dext.Net.Nats.JetStream.Parsers;

procedure CopyCurrentValue(var AReader: TUtf8JsonReader;
  var AWriter: TUtf8JsonWriter); forward;

procedure CopyObject(var AReader: TUtf8JsonReader; var AWriter: TUtf8JsonWriter);
begin
  AWriter.WriteStartObject;
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    AWriter.WritePropertyName(AReader.GetString);
    if not AReader.Read then
      raise EDextNatsProtocolError.Create('Unexpected end of JetStream JSON object');
    CopyCurrentValue(AReader, AWriter);
  end;
  AWriter.WriteEndObject;
end;

procedure CopyArray(var AReader: TUtf8JsonReader; var AWriter: TUtf8JsonWriter);
begin
  AWriter.WriteStartArray;
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndArray then
      Break;
    CopyCurrentValue(AReader, AWriter);
  end;
  AWriter.WriteEndArray;
end;

procedure CopyCurrentValue(var AReader: TUtf8JsonReader;
  var AWriter: TUtf8JsonWriter);
begin
  case AReader.TokenType of
    TJsonTokenType.StartObject:
      CopyObject(AReader, AWriter);
    TJsonTokenType.StartArray:
      CopyArray(AReader, AWriter);
    TJsonTokenType.StringValue:
      AWriter.WriteString(AReader.GetString);
    TJsonTokenType.Number:
      AWriter.WriteNumber(AReader.GetInt64);
    TJsonTokenType.TrueValue:
      AWriter.WriteBoolean(True);
    TJsonTokenType.FalseValue:
      AWriter.WriteBoolean(False);
    TJsonTokenType.NullValue:
      AWriter.WriteNull;
  else
    raise EDextNatsProtocolError.Create('Unsupported JetStream JSON token while paging');
  end;
end;

function CurrentObjectToJson(var AReader: TUtf8JsonReader): string;
var
  Sink: TDextNatsJsByteWriter;
  Writer: TUtf8JsonWriter;
begin
  Sink.Reset;
  Writer := TUtf8JsonWriter.Create(@Sink, DextNatsJsUtf8SinkWrite, False);
  CopyObject(AReader, Writer);
  Writer.Flush;
  Result := Sink.ToUtf8String;
end;

function WrapErrorObject(const AErrorObjectJson: string): string;
begin
  Result := '{"error":' + AErrorObjectJson + '}';
end;

function OpenReader(const AJson: string; out ABytes: TBytes): TUtf8JsonReader;
var
  Span: TByteSpan;
begin
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create('Empty JetStream paged response');
  ABytes := TEncoding.UTF8.GetBytes(AJson);
  Span := TByteSpan.Create(@ABytes[0], Length(ABytes));
  Result := TUtf8JsonReader.Create(Span);
  if (not Result.Read) or (Result.TokenType <> TJsonTokenType.StartObject) then
    raise EDextNatsProtocolError.Create('Malformed JetStream paged response');
end;

function NatsJsParseStreamInfoPage(const AJson: string): TNatsJsStreamInfoPage;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
  ItemJson: string;
  Info: TNatsStreamInfo;
begin
  Result := Default(TNatsJsStreamInfoPage);
  Reader := OpenReader(AJson, Bytes);
  while Reader.Read do
  begin
    if Reader.TokenType = TJsonTokenType.EndObject then
      Break;
    if Reader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if Reader.ValueSpanEquals('total') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Total := Reader.GetInt32;
    end
    else if Reader.ValueSpanEquals('offset') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Offset := Reader.GetInt32;
    end
    else if Reader.ValueSpanEquals('limit') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Limit := Reader.GetInt32;
    end
    else if Reader.ValueSpanEquals('error') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.StartObject) then
      begin
        ItemJson := CurrentObjectToJson(Reader);
        NatsJsParseSuccess(WrapErrorObject(ItemJson));
      end;
    end
    else if Reader.ValueSpanEquals('streams') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.StartArray) then
        while Reader.Read do
        begin
          if Reader.TokenType = TJsonTokenType.EndArray then
            Break;
          if Reader.TokenType = TJsonTokenType.StartObject then
          begin
            ItemJson := CurrentObjectToJson(Reader);
            Info := NatsJsParseStreamInfo(ItemJson);
            SetLength(Result.Items, Length(Result.Items) + 1);
            Result.Items[High(Result.Items)] := Info;
          end
          else if Reader.TokenType in [TJsonTokenType.StartArray, TJsonTokenType.StartObject] then
            Reader.Skip;
        end;
    end
    else if Reader.Read and (Reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray]) then
      Reader.Skip;
  end;
end;

function NatsJsParseConsumerInfoPage(const AJson: string): TNatsJsConsumerInfoPage;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
  ItemJson: string;
  Info: TNatsConsumerInfo;
begin
  Result := Default(TNatsJsConsumerInfoPage);
  Reader := OpenReader(AJson, Bytes);
  while Reader.Read do
  begin
    if Reader.TokenType = TJsonTokenType.EndObject then
      Break;
    if Reader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if Reader.ValueSpanEquals('total') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Total := Reader.GetInt32;
    end
    else if Reader.ValueSpanEquals('offset') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Offset := Reader.GetInt32;
    end
    else if Reader.ValueSpanEquals('limit') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.Number) then
        Result.Limit := Reader.GetInt32;
    end
    else if Reader.ValueSpanEquals('error') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.StartObject) then
      begin
        ItemJson := CurrentObjectToJson(Reader);
        NatsJsParseSuccess(WrapErrorObject(ItemJson));
      end;
    end
    else if Reader.ValueSpanEquals('consumers') then
    begin
      if Reader.Read and (Reader.TokenType = TJsonTokenType.StartArray) then
        while Reader.Read do
        begin
          if Reader.TokenType = TJsonTokenType.EndArray then
            Break;
          if Reader.TokenType = TJsonTokenType.StartObject then
          begin
            ItemJson := CurrentObjectToJson(Reader);
            Info := NatsJsParseConsumerInfo(ItemJson);
            SetLength(Result.Items, Length(Result.Items) + 1);
            Result.Items[High(Result.Items)] := Info;
          end
          else if Reader.TokenType in [TJsonTokenType.StartArray, TJsonTokenType.StartObject] then
            Reader.Skip;
        end;
    end
    else if Reader.Read and (Reader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray]) then
      Reader.Skip;
  end;
end;

end.
