{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream JSON core tests                                       }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Json.Tests;

interface

uses
  System.SysUtils,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Json,
  Dext.Net.Nats.JetStream.Codecs;

type
  [TestFixture('NATS JetStream JSON Core')]
  TDextNatsJetStreamJsonTests = class
  public
    [Test, Category('Unit'), Category('JetStream')]
    procedure PagedList_ShouldMatchWireContract;

    [Test, Category('Unit'), Category('JetStream')]
    procedure GetLastMessage_ShouldMatchWireContract;

    [Test, Category('Unit'), Category('JetStream')]
    procedure GetMessage_ShouldMatchWireContract;

    [Test, Category('Unit'), Category('JetStream')]
    procedure Writer_ShouldGrowAndPreserveUtf8;

    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure StreamConfigCodec_ShouldMatchFacadeToJson;

    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure ConsumerConfigCodec_ShouldMatchFacadeToJson;

    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure PurgeCodec_ShouldMatchFacadeToJson;
  end;

implementation

procedure TDextNatsJetStreamJsonTests.PagedList_ShouldMatchWireContract;
begin
  Should(NatsJsBuildPagedListRequest(0)).Be('{"offset":0}');
  Should(NatsJsBuildPagedListRequest(25, 'orders.*')).Be(
    '{"offset":25,"subject":"orders.*"}');
end;

procedure TDextNatsJetStreamJsonTests.GetLastMessage_ShouldMatchWireContract;
begin
  Should(NatsJsBuildGetLastMessageRequest('KV_bucket.key')).Be(
    '{"last_by_subj":"KV_bucket.key"}');
end;

procedure TDextNatsJetStreamJsonTests.GetMessage_ShouldMatchWireContract;
begin
  Should(NatsJsBuildGetMessageRequest(42)).Be('{"seq":42}');
end;

procedure TDextNatsJetStreamJsonTests.Writer_ShouldGrowAndPreserveUtf8;
var
  Writer: TDextNatsJsByteWriter;
  Bytes: TBytes;
  Text: string;
begin
  Writer.Reset;
  Text := StringOfChar('x', 600) + ' سلام';
  Bytes := TEncoding.UTF8.GetBytes(Text);
  Writer.WriteBytes(@Bytes[0], Length(Bytes));
  Should(Writer.ToUtf8String).Be(Text);
  Should(Length(Writer.ToBytes)).Be(Length(Bytes));
end;

procedure TDextNatsJetStreamJsonTests.StreamConfigCodec_ShouldMatchFacadeToJson;
var
  Config: TNatsStreamConfig;
  Source: TNatsStreamSource;
  Transform: TNatsSubjectTransform;
begin
  Config := TNatsStreamConfig.CreateDefault('ORDERS', ['orders.*']);
  Config.Description := 'Orders stream';
  Config.Storage := ssMemory;
  Config.Discard := sdNew;
  Config.MaxMsgs := 10000;
  Config.MaxBytes := 1024 * 1024;
  Config.AllowDirect := True;
  Config.Compression := scS2;
  Config.Placement.Cluster := 'cluster-a';
  Config.Placement.Tags := ['ssd', 'eu'];
  Config.RePublish.Source := 'orders.*';
  Config.RePublish.Destination := 'audit.orders.>';
  Config.RePublish.HeadersOnly := True;

  Source := Default(TNatsStreamSource);
  Source.Name := 'UPSTREAM';
  Source.FilterSubject := 'orders.created';
  Transform := Default(TNatsSubjectTransform);
  Transform.Source := 'orders.*';
  Transform.Destination := 'imported.*';
  Source.SubjectTransforms := [Transform];
  Source.ExternalStream.ApiPrefix := '$JS.REMOTE.API';
  Config.Sources := [Source];

  Should(NatsJsEncodeStreamConfig(Config)).Be(Config.ToJson);
end;

procedure TDextNatsJetStreamJsonTests.ConsumerConfigCodec_ShouldMatchFacadeToJson;
var
  Config: TNatsConsumerConfig;
begin
  Config := TNatsConsumerConfig.CreateDefault('WORKER', 'orders.*');
  Config.Description := 'worker consumer';
  Config.DeliverPolicy := dpByStartSequence;
  Config.OptStartSeq := 42;
  Config.AckPolicy := apExplicit;
  Config.AckWait := 30000000000;
  Config.MaxDeliver := 5;
  Config.MaxAckPending := 2048;
  Config.MaxWaiting := 128;
  Config.ReplayPolicy := rpOriginal;
  Config.HeadersOnly := True;
  Config.FlowControl := True;
  Config.IdleHeartbeat := 5000000000;
  Config.InactiveThreshold := 60000000000;
  Config.MemoryStorage := True;
  Config.NumReplicas := 1;

  Should(NatsJsEncodeConsumerConfig(Config)).Be(Config.ToJson);

  Config.FilterSubjects := ['orders.created', 'orders.updated'];
  Config.FilterSubject := 'ignored.when.multi';
  Should(NatsJsEncodeConsumerConfig(Config)).Be(Config.ToJson);
end;

procedure TDextNatsJetStreamJsonTests.PurgeCodec_ShouldMatchFacadeToJson;
var
  Request: TNatsStreamPurgeRequest;
begin
  Request := TNatsStreamPurgeRequest.CreateDefault;
  Should(NatsJsEncodePurgeRequest(Request)).Be(Request.ToJson);

  Request.Subject := 'orders.old';
  Request.Sequence := 100;
  Request.Keep := 2;
  Should(NatsJsEncodePurgeRequest(Request)).Be(Request.ToJson);
end;

end.
