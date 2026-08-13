unit Dext.Net.Nats.JetStream.Fetch.Tests;

interface

uses
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

end.
