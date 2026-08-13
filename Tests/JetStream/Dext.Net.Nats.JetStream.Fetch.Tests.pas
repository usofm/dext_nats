unit Dext.Net.Nats.JetStream.Fetch.Tests;

interface

uses
  System.SysUtils,
  Dext.Core.Span,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream.Fetch;

type
  [TestFixture('NATS JetStream Fetch')]
  TDextNatsJetStreamFetchTests = class
  public
    [Test, Category('Unit'), Category('JetStream')]
    procedure BuildRequest_ShouldNormalizeBatchAndExpires;

    [Test, Category('Unit'), Category('JetStream')]
    procedure ControlDetection_ShouldUseStatusCode;

    [Test, Category('Unit'), Category('JetStream')]
    procedure ControlDetection_ShouldUsePayloadSpanNotOwnedField;
  end;

implementation

procedure TDextNatsJetStreamFetchTests.BuildRequest_ShouldNormalizeBatchAndExpires;
begin
  Should(NatsJsBuildFetchRequest(5, 1000)).Be(
    '{"batch":5,"expires":1000000000}');
  Should(NatsJsBuildFetchRequest(0, 0)).Be('{"batch":1}');
  Should(NatsJsBuildFetchRequest(-5, -1)).Be('{"batch":1}');
end;

procedure TDextNatsJetStreamFetchTests.ControlDetection_ShouldUseStatusCode;
var
  Msg: TNatsMsg;
begin
  Msg := Default(TNatsMsg);
  Msg.StatusCode := 0;
  Should(NatsJsIsControlMessage(Msg)).Be(False);
  Msg.StatusCode := 408;
  Should(NatsJsIsControlMessage(Msg)).Be(True);
  Msg.StatusCode := 100;
  Should(NatsJsIsControlMessage(Msg)).Be(True);
  Msg.StatusCode := 404;
  Should(NatsJsIsControlMessage(Msg)).Be(True);
  Msg.StatusCode := 409;
  Should(NatsJsIsControlMessage(Msg)).Be(True);
end;

procedure TDextNatsJetStreamFetchTests.ControlDetection_ShouldUsePayloadSpanNotOwnedField;
var
  Msg: TNatsMsg;
  Bytes: TBytes;
begin
  Bytes := TEncoding.UTF8.GetBytes('body');
  Msg := Default(TNatsMsg);
  Msg.StatusCode := 200;
  Msg.BindBorrowedPayload(TByteSpan.FromBytes(Bytes));
  Should(Length(Msg.Payload)).Be(0);
  Should(NatsJsIsControlMessage(Msg)).BeFalse;
end;

end.
