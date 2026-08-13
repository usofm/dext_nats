{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream JSON and parser parity tests                          }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Json.Tests;

interface

uses
  System.SysUtils,
  System.NetEncoding,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Json,
  Dext.Net.Nats.JetStream.Codecs,
  Dext.Net.Nats.JetStream.Parsers;

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

    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure StreamInfoParser_ShouldMatchFacade;
    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure ConsumerInfoParser_ShouldMatchFacade;
    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure StoredMsgParser_ShouldMatchFacade;
    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure PublishAckParser_ShouldMatchFacade;
    [Test, Category('Unit'), Category('JetStream'), Category('Parity')]
    procedure ParserError_ShouldPreserveJetStreamErrorSemantics;
    [Test, Category('Unit'), Category('JetStream')]
    procedure StreamInfoParser_ShouldParseUtf8BytesWithoutStringRoundTrip;
    [Test, Category('Unit'), Category('JetStream')]
    procedure PublishAckParser_ShouldParseUtf8Bytes;
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

procedure TDextNatsJetStreamJsonTests.StreamInfoParser_ShouldMatchFacade;
const
  Json = '{"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits","storage":"memory","max_consumers":-1,"max_msgs":100,"max_bytes":4096,"max_age":0,"max_msg_size":-1,"discard":"old","num_replicas":1,"duplicate_window":120000000000,"allow_direct":true,"compression":"s2","placement":{"cluster":"c1","tags":["ssd"]}},"state":{"messages":12,"bytes":345,"first_seq":2,"last_seq":20,"consumer_count":3}}';
var
  OldInfo, NewInfo: TNatsStreamInfo;
begin
  OldInfo := TNatsStreamInfo.Parse(Json);
  NewInfo := NatsJsParseStreamInfo(Json);
  Should(NewInfo.Name).Be(OldInfo.Name);
  Should(NewInfo.Messages).Be(OldInfo.Messages);
  Should(NewInfo.Bytes).Be(OldInfo.Bytes);
  Should(NewInfo.FirstSeq).Be(OldInfo.FirstSeq);
  Should(NewInfo.LastSeq).Be(OldInfo.LastSeq);
  Should(NewInfo.ConsumerCount).Be(OldInfo.ConsumerCount);
  Should(NewInfo.Config.ToJson).Be(OldInfo.Config.ToJson);
end;

procedure TDextNatsJetStreamJsonTests.ConsumerInfoParser_ShouldMatchFacade;
const
  Json = '{"stream_name":"ORDERS","name":"WORKER","num_pending":7,"num_ack_pending":2,"num_redelivered":1,"num_waiting":4,"config":{"durable_name":"WORKER","filter_subject":"orders.*","deliver_subject":"_INBOX.x","deliver_group":"workers"}}';
var
  OldInfo, NewInfo: TNatsConsumerInfo;
begin
  OldInfo := TNatsConsumerInfo.Parse(Json);
  NewInfo := NatsJsParseConsumerInfo(Json);
  Should(NewInfo.StreamName).Be(OldInfo.StreamName);
  Should(NewInfo.Name).Be(OldInfo.Name);
  Should(NewInfo.DurableName).Be(OldInfo.DurableName);
  Should(NewInfo.FilterSubject).Be(OldInfo.FilterSubject);
  Should(NewInfo.DeliverSubject).Be(OldInfo.DeliverSubject);
  Should(NewInfo.DeliverGroup).Be(OldInfo.DeliverGroup);
  Should(NewInfo.NumPending).Be(OldInfo.NumPending);
  Should(NewInfo.NumAckPending).Be(OldInfo.NumAckPending);
  Should(NewInfo.NumRedelivered).Be(OldInfo.NumRedelivered);
  Should(NewInfo.NumWaiting).Be(OldInfo.NumWaiting);
end;

procedure TDextNatsJetStreamJsonTests.StoredMsgParser_ShouldMatchFacade;
var
  Json: string;
  DataB64, HeadersB64: string;
  HeaderWire: string;
  OldMsg, NewMsg: TNatsStoredMsg;
begin
  DataB64 := TNetEncoding.Base64.EncodeBytesToString(TEncoding.UTF8.GetBytes('hello'));
  HeaderWire := 'NATS/1.0'#13#10'KV-Operation: PUT'#13#10#13#10;
  HeadersB64 := TNetEncoding.Base64.EncodeBytesToString(TEncoding.UTF8.GetBytes(HeaderWire));
  Json := Format('{"message":{"subject":"KV_demo.key","seq":9,"data":"%s","hdrs":"%s","time":"2026-08-13T00:00:00Z"}}', [DataB64, HeadersB64]);

  OldMsg := TNatsStoredMsg.Parse(Json);
  NewMsg := NatsJsParseStoredMsg(Json);
  Should(NewMsg.Subject).Be(OldMsg.Subject);
  Should(NewMsg.Sequence).Be(OldMsg.Sequence);
  Should(TEncoding.UTF8.GetString(NewMsg.Data)).Be(TEncoding.UTF8.GetString(OldMsg.Data));
  Should(NewMsg.TimeStamp).Be(OldMsg.TimeStamp);
  Should(NewMsg.Headers.GetValue('KV-Operation')).Be(OldMsg.Headers.GetValue('KV-Operation'));
end;

procedure TDextNatsJetStreamJsonTests.PublishAckParser_ShouldMatchFacade;
const
  Json = '{"stream":"ORDERS","seq":55,"duplicate":true,"domain":"EU"}';
var
  OldAck, NewAck: TNatsPublishAck;
begin
  OldAck := TNatsPublishAck.Parse(Json);
  NewAck := NatsJsParsePublishAck(Json);
  Should(NewAck.Stream).Be(OldAck.Stream);
  Should(NewAck.Sequence).Be(OldAck.Sequence);
  Should(NewAck.Duplicate).Be(OldAck.Duplicate);
  Should(NewAck.Domain).Be(OldAck.Domain);
end;

procedure TDextNatsJetStreamJsonTests.ParserError_ShouldPreserveJetStreamErrorSemantics;
const
  Json = '{"error":{"code":404,"err_code":10059,"description":"stream not found"}}';
var
  OldCode, OldErrCode, NewCode, NewErrCode: Integer;
begin
  OldCode := 0;
  OldErrCode := 0;
  NewCode := 0;
  NewErrCode := 0;

  try
    TNatsStreamInfo.Parse(Json);
  except
    on E: EDextNatsJetStreamError do
    begin
      OldCode := E.Code;
      OldErrCode := E.ErrCode;
    end;
  end;

  try
    NatsJsParseStreamInfo(Json);
  except
    on E: EDextNatsJetStreamError do
    begin
      NewCode := E.Code;
      NewErrCode := E.ErrCode;
    end;
  end;

  Should(NewCode).Be(OldCode);
  Should(NewErrCode).Be(OldErrCode);
  Should(NewCode).Be(404);
  Should(NewErrCode).Be(10059);
end;

procedure TDextNatsJetStreamJsonTests.StreamInfoParser_ShouldParseUtf8BytesWithoutStringRoundTrip;
const
  Json = '{"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits","storage":"memory","max_consumers":-1,"max_msgs":100,"max_bytes":4096,"max_age":0,"max_msg_size":-1,"discard":"old","num_replicas":1,"duplicate_window":120000000000},"state":{"messages":12,"bytes":345,"first_seq":2,"last_seq":20,"consumer_count":3}}';
var
  Bytes: TBytes;
  FromString, FromBytes: TNatsStreamInfo;
begin
  Bytes := TEncoding.UTF8.GetBytes(Json);
  FromString := NatsJsParseStreamInfo(Json);
  FromBytes := NatsJsParseStreamInfo(Bytes);
  Should(FromBytes.Name).Be(FromString.Name);
  Should(FromBytes.Messages).Be(FromString.Messages);
  Should(FromBytes.LastSeq).Be(FromString.LastSeq);
  Should(TNatsStreamInfo.Parse(Bytes).Name).Be('ORDERS');
end;

procedure TDextNatsJetStreamJsonTests.PublishAckParser_ShouldParseUtf8Bytes;
const
  Json = '{"stream":"ORDERS","seq":55,"duplicate":true,"domain":"EU"}';
var
  Bytes: TBytes;
  Ack: TNatsPublishAck;
begin
  Bytes := TEncoding.UTF8.GetBytes(Json);
  Ack := NatsJsParsePublishAck(Bytes);
  Should(Ack.Stream).Be('ORDERS');
  Should(Ack.Sequence).Be(UInt64(55));
  Should(Ack.Duplicate).BeTrue;
  Should(TNatsPublishAck.Parse(Bytes).Domain).Be('EU');
end;

end.
