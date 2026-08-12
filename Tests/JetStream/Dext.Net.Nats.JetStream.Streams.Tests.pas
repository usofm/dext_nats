{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream stream administration tests                           }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Streams.Tests;

interface

uses
  System.SysUtils,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Transport,
  Dext.Net.Nats.JetStream.Streams;

type
  TFakeJetStreamTransport = class(TInterfacedObject, INatsJetStreamApiTransport)
  public
    LastSubject: string;
    LastBody: string;
    LastTimeoutMs: Integer;
    Response: string;
    function Request(const ASubjectSuffix, ABody: string;
      ATimeoutMs: Integer = 0): string;
  end;

  [TestFixture('NATS JetStream Streams')]
  TDextNatsJetStreamStreamsTests = class
  private
    function NewTransport(out ARaw: TFakeJetStreamTransport): INatsJetStreamApiTransport;
  public
    [Test, Category('Unit'), Category('JetStream')]
    procedure CreateStream_ShouldUseCreateSubjectAndCodec;

    [Test, Category('Unit'), Category('JetStream')]
    procedure UpdateStream_ShouldUseUpdateSubject;

    [Test, Category('Unit'), Category('JetStream')]
    procedure DeleteAndPurge_ShouldParseSuccess;

    [Test, Category('Unit'), Category('JetStream')]
    procedure GetMessage_ShouldForwardSequenceAndTimeout;

    [Test, Category('Unit'), Category('JetStream')]
    procedure GetLastMessage_ShouldForwardSubjectAndTimeout;

    [Test, Category('Unit'), Category('JetStream')]
    procedure StreamExists_ShouldReturnFalseForNotFound;
  end;

implementation

function TFakeJetStreamTransport.Request(const ASubjectSuffix, ABody: string;
  ATimeoutMs: Integer): string;
begin
  LastSubject := ASubjectSuffix;
  LastBody := ABody;
  LastTimeoutMs := ATimeoutMs;
  Result := Response;
end;

function TDextNatsJetStreamStreamsTests.NewTransport(
  out ARaw: TFakeJetStreamTransport): INatsJetStreamApiTransport;
begin
  ARaw := TFakeJetStreamTransport.Create;
  Result := ARaw;
end;

procedure TDextNatsJetStreamStreamsTests.CreateStream_ShouldUseCreateSubjectAndCodec;
var
  Raw: TFakeJetStreamTransport;
  Transport: INatsJetStreamApiTransport;
  Streams: TDextNatsJetStreamStreams;
  Config: TNatsStreamConfig;
  Info: TNatsStreamInfo;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits","storage":"file","max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"discard":"old","num_replicas":1,"duplicate_window":120000000000},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}';
  Streams := TDextNatsJetStreamStreams.Create(Transport);
  try
    Config := TNatsStreamConfig.CreateDefault('ORDERS', ['orders.*']);
    Info := Streams.CreateStream(Config);
    Should(Raw.LastSubject).Be('STREAM.CREATE.ORDERS');
    Should(Raw.LastBody).Be(Config.ToJson);
    Should(Info.Name).Be('ORDERS');
  finally
    Streams.Free;
  end;
end;

procedure TDextNatsJetStreamStreamsTests.UpdateStream_ShouldUseUpdateSubject;
var
  Raw: TFakeJetStreamTransport;
  Transport: INatsJetStreamApiTransport;
  Streams: TDextNatsJetStreamStreams;
  Config: TNatsStreamConfig;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"config":{"name":"ORDERS"},"state":{}}';
  Streams := TDextNatsJetStreamStreams.Create(Transport);
  try
    Config := TNatsStreamConfig.CreateDefault('ORDERS', ['orders.*']);
    Config.MaxMsgs := 1000;
    Streams.UpdateStream(Config);
    Should(Raw.LastSubject).Be('STREAM.UPDATE.ORDERS');
    Should(Raw.LastBody).Be(Config.ToJson);
  finally
    Streams.Free;
  end;
end;

procedure TDextNatsJetStreamStreamsTests.DeleteAndPurge_ShouldParseSuccess;
var
  Raw: TFakeJetStreamTransport;
  Transport: INatsJetStreamApiTransport;
  Streams: TDextNatsJetStreamStreams;
  Purge: TNatsStreamPurgeRequest;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"success":true}';
  Streams := TDextNatsJetStreamStreams.Create(Transport);
  try
    Should(Streams.DeleteStream('ORDERS')).Be(True);
    Should(Raw.LastSubject).Be('STREAM.DELETE.ORDERS');
    Should(Raw.LastBody).Be('{}');

    Purge := TNatsStreamPurgeRequest.CreateDefault;
    Purge.Subject := 'orders.old';
    Purge.Keep := 2;
    Should(Streams.PurgeStream('ORDERS', Purge)).Be(True);
    Should(Raw.LastSubject).Be('STREAM.PURGE.ORDERS');
    Should(Raw.LastBody).Be(Purge.ToJson);
  finally
    Streams.Free;
  end;
end;

procedure TDextNatsJetStreamStreamsTests.GetMessage_ShouldForwardSequenceAndTimeout;
var
  Raw: TFakeJetStreamTransport;
  Transport: INatsJetStreamApiTransport;
  Streams: TDextNatsJetStreamStreams;
  Msg: TNatsStoredMsg;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"message":{"subject":"orders.created","seq":42,"data":"aGVsbG8=","time":"2026-08-13T00:00:00Z"}}';
  Streams := TDextNatsJetStreamStreams.Create(Transport);
  try
    Msg := Streams.GetMessage('ORDERS', 42, 2500);
    Should(Raw.LastSubject).Be('STREAM.MSG.GET.ORDERS');
    Should(Raw.LastBody).Be('{"seq":42}');
    Should(Raw.LastTimeoutMs).Be(2500);
    Should(Msg.Sequence).Be(UInt64(42));
    Should(TEncoding.UTF8.GetString(Msg.Data)).Be('hello');
  finally
    Streams.Free;
  end;
end;

procedure TDextNatsJetStreamStreamsTests.GetLastMessage_ShouldForwardSubjectAndTimeout;
var
  Raw: TFakeJetStreamTransport;
  Transport: INatsJetStreamApiTransport;
  Streams: TDextNatsJetStreamStreams;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"message":{"subject":"orders.created","seq":7,"data":"eA=="}}';
  Streams := TDextNatsJetStreamStreams.Create(Transport);
  try
    Streams.GetLastMessage('ORDERS', 'orders.created', 1200);
    Should(Raw.LastSubject).Be('STREAM.MSG.GET.ORDERS');
    Should(Raw.LastBody).Be('{"last_by_subj":"orders.created"}');
    Should(Raw.LastTimeoutMs).Be(1200);
  finally
    Streams.Free;
  end;
end;

procedure TDextNatsJetStreamStreamsTests.StreamExists_ShouldReturnFalseForNotFound;
var
  Raw: TFakeJetStreamTransport;
  Transport: INatsJetStreamApiTransport;
  Streams: TDextNatsJetStreamStreams;
begin
  Transport := NewTransport(Raw);
  Raw.Response := '{"error":{"code":404,"err_code":10059,"description":"stream not found"}}';
  Streams := TDextNatsJetStreamStreams.Create(Transport);
  try
    Should(Streams.StreamExists('MISSING')).Be(False);
    Should(Raw.LastSubject).Be('STREAM.INFO.MISSING');
  finally
    Streams.Free;
  end;
end;

end.
