{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream consumer administration                               }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Consumers;

interface

uses
  Dext.Collections,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Transport;

type
  TDextNatsJetStreamConsumers = class
  private
    FTransport: INatsJetStreamApiTransport;
    function BuildCreateBody(const AStreamName: string;
      const AConfig: TNatsConsumerConfig): string;
  public
    constructor Create(const ATransport: INatsJetStreamApiTransport);

    function CreateConsumer(const AStreamName: string;
      const AConfig: TNatsConsumerConfig): TNatsConsumerInfo;
    function GetConsumerInfo(const AStreamName,
      AConsumerName: string): TNatsConsumerInfo;
    function DeleteConsumer(const AStreamName,
      AConsumerName: string): Boolean;
    function ListConsumerNames(const AStreamName: string): IList<string>;
    function ListConsumers(const AStreamName: string): IList<TNatsConsumerInfo>;
  end;

implementation

uses
  System.SysUtils,
  Dext.Json.Utf8,
  Dext.Net.Nats,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.JetStream.Json,
  Dext.Net.Nats.JetStream.Codecs,
  Dext.Net.Nats.JetStream.Parsers,
  Dext.Net.Nats.JetStream.Paging,
  Dext.Net.Nats.JetStream.ObjectPaging;

constructor TDextNatsJetStreamConsumers.Create(
  const ATransport: INatsJetStreamApiTransport);
begin
  inherited Create;
  if ATransport = nil then
    raise EDextNatsException.Create('JetStream consumers service requires a transport');
  FTransport := ATransport;
end;

function TDextNatsJetStreamConsumers.BuildCreateBody(const AStreamName: string;
  const AConfig: TNatsConsumerConfig): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
  ConfigBytes: TBytes;
begin
  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;
  Json.WritePropertyName('stream_name');
  Json.WriteString(AStreamName);
  Json.WritePropertyName('config');
  ConfigBytes := TEncoding.UTF8.GetBytes(NatsJsEncodeConsumerConfig(AConfig));
  if Length(ConfigBytes) > 0 then
    Writer.WriteBytes(@ConfigBytes[0], Length(ConfigBytes));
  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

function TDextNatsJetStreamConsumers.CreateConsumer(const AStreamName: string;
  const AConfig: TNatsConsumerConfig): TNatsConsumerInfo;
var
  ConsumerPart, Subject: string;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('CreateConsumer requires a stream name');

  ConsumerPart := AConfig.DurableName;
  if ConsumerPart = '' then
    ConsumerPart := AConfig.Name;

  if ConsumerPart <> '' then
    Subject := Format('CONSUMER.CREATE.%s.%s', [AStreamName, ConsumerPart])
  else
    Subject := Format('CONSUMER.CREATE.%s', [AStreamName]);

  Result := NatsJsParseConsumerInfo(FTransport.Request(Subject,
    BuildCreateBody(AStreamName, AConfig)));
end;

function TDextNatsJetStreamConsumers.GetConsumerInfo(const AStreamName,
  AConsumerName: string): TNatsConsumerInfo;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create(
      'GetConsumerInfo requires stream and consumer names');
  Result := NatsJsParseConsumerInfo(FTransport.Request(
    Format('CONSUMER.INFO.%s.%s', [AStreamName, AConsumerName]), '{}'));
end;

function TDextNatsJetStreamConsumers.DeleteConsumer(const AStreamName,
  AConsumerName: string): Boolean;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create(
      'DeleteConsumer requires stream and consumer names');
  Result := NatsJsParseSuccess(FTransport.Request(
    Format('CONSUMER.DELETE.%s.%s', [AStreamName, AConsumerName]), '{}'));
end;

function TDextNatsJetStreamConsumers.ListConsumerNames(
  const AStreamName: string): IList<string>;
var
  Offset, I: Integer;
  Page: TNatsJsNamePage;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('ListConsumerNames requires a stream name');

  Result := TCollections.CreateList<string>;
  Offset := 0;
  repeat
    Page := NatsJsParseNamePage(FTransport.Request(
      Format('CONSUMER.NAMES.%s', [AStreamName]),
      NatsJsBuildPagedListRequest(Offset, '')), 'consumers');
    for I := 0 to High(Page.Items) do
      Result.Add(Page.Items[I]);
    if Length(Page.Items) = 0 then
      Break;
    Inc(Offset, Length(Page.Items));
  until Offset >= Page.Total;
end;

function TDextNatsJetStreamConsumers.ListConsumers(
  const AStreamName: string): IList<TNatsConsumerInfo>;
var
  Offset, I: Integer;
  Page: TNatsJsConsumerInfoPage;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('ListConsumers requires a stream name');

  Result := TCollections.CreateList<TNatsConsumerInfo>;
  Offset := 0;
  repeat
    Page := NatsJsParseConsumerInfoPage(FTransport.Request(
      Format('CONSUMER.LIST.%s', [AStreamName]),
      NatsJsBuildPagedListRequest(Offset, '')));
    for I := 0 to High(Page.Items) do
      Result.Add(Page.Items[I]);
    if Length(Page.Items) = 0 then
      Break;
    Inc(Offset, Length(Page.Items));
  until Offset >= Page.Total;
end;

end.
