{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream consumer administration tests                         }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Consumers.Tests;

interface

uses
  System.SysUtils,
  Dext.Collections,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Transport,
  Dext.Net.Nats.JetStream.Consumers;

type
  TFakeConsumerTransport = class(TInterfacedObject, INatsJetStreamApiTransport)
  public
    LastSubject: string;
    LastBody: string;
    Response: string;
    CallCount: Integer;
    function Request(const ASubjectSuffix, ABody: string;
      ATimeoutMs: Integer = 0): TBytes;
  end;

  [TestFixture('NATS JetStream Consumers')]
  TDextNatsJetStreamConsumersTests = class
  private
    function NewTransport(out ARaw: TFakeConsumerTransport): INatsJetStreamApiTransport;
  public
    [Test, Category('Unit'), Category('JetStream')]
    procedure CreateConsumer_ShouldUseDurableSubjectAndConfigBody;

    [Test, Category('Unit'), Category('JetStream')]
    procedure GetAndDeleteConsumer_ShouldUseExpectedSubjects;

    [Test, Category('Unit'), Category('JetStream')]
    procedure ListConsumerNames_ShouldParsePagedNames;
  end;

implementation

function TFakeConsumerTransport.Request(const ASubjectSuffix, ABody: string;
  ATimeoutMs: Integer): TBytes;
begin
  Inc(CallCount);
  LastSubject := ASubjectSuffix;
  LastBody := ABody;
  Result := TEncoding.UTF8.GetBytes(Response);
end;

function TDextNatsJetStreamConsumersTests.NewTransport(
  out ARaw: TFakeConsumerTransport): INatsJetStreamApiTransport;
begin
  ARaw := TFakeConsumerTransport.Create;
  Result := ARaw;
end;

procedure TDextNatsJetStreamConsumersTests.CreateConsumer_ShouldUseDurableSubjectAndConfigBody;
var
  Raw: TFakeConsumerTransport;
  Transport: INatsJetStreamApiTransport;
  Consumers: TDextNatsJetStreamConsumers;
  Config: TNatsConsumerConfig;
  Info: TNatsConsumerInfo;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"stream_name":"ORDERS","name":"WORKER","config":{"durable_name":"WORKER","filter_subject":"orders.*","deliver_policy":"all","ack_policy":"explicit","max_deliver":-1,"max_ack_pending":1000,"max_waiting":512,"replay_policy":"instant"}}';
  Consumers := TDextNatsJetStreamConsumers.Create(Transport);
  try
    Config := TNatsConsumerConfig.CreateDefault('WORKER', 'orders.*');
    Info := Consumers.CreateConsumer('ORDERS', Config);
    Should(Raw.LastSubject).Be('CONSUMER.CREATE.ORDERS.WORKER');
    Should(Raw.LastBody.Contains('"stream_name":"ORDERS"')).Be(True);
    Should(Raw.LastBody.Contains('"config":')).Be(True);
    Should(Raw.LastBody.Contains('"durable_name":"WORKER"')).Be(True);
    Should(Info.StreamName).Be('ORDERS');
    Should(Info.Name).Be('WORKER');
  finally
    Consumers.Free;
  end;
end;

procedure TDextNatsJetStreamConsumersTests.GetAndDeleteConsumer_ShouldUseExpectedSubjects;
var
  Raw: TFakeConsumerTransport;
  Transport: INatsJetStreamApiTransport;
  Consumers: TDextNatsJetStreamConsumers;
begin
  Transport := NewTransport(Raw);
  Consumers := TDextNatsJetStreamConsumers.Create(Transport);
  try
    Raw.Response := '{"stream_name":"ORDERS","name":"WORKER","config":{}}';
    Consumers.GetConsumerInfo('ORDERS', 'WORKER');
    Should(Raw.LastSubject).Be('CONSUMER.INFO.ORDERS.WORKER');
    Should(Raw.LastBody).Be('{}');

    Raw.Response := '{"success":true}';
    Should(Consumers.DeleteConsumer('ORDERS', 'WORKER')).Be(True);
    Should(Raw.LastSubject).Be('CONSUMER.DELETE.ORDERS.WORKER');
  finally
    Consumers.Free;
  end;
end;

procedure TDextNatsJetStreamConsumersTests.ListConsumerNames_ShouldParsePagedNames;
var
  Raw: TFakeConsumerTransport;
  Transport: INatsJetStreamApiTransport;
  Consumers: TDextNatsJetStreamConsumers;
  Names: IList<string>;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"type":"io.nats.jetstream.api.v1.consumer_names_response","total":2,"offset":0,"limit":1024,"consumers":["A","B"]}';
  Consumers := TDextNatsJetStreamConsumers.Create(Transport);
  try
    Names := Consumers.ListConsumerNames('ORDERS');
    Should(Names.Count).Be(2);
    Should(Names[0]).Be('A');
    Should(Names[1]).Be('B');
    Should(Raw.LastSubject).Be('CONSUMER.NAMES.ORDERS');
    Should(Raw.LastBody).Be('{"offset":0}');
    Should(Raw.CallCount).Be(1);
  finally
    Consumers.Free;
  end;
end;

end.
