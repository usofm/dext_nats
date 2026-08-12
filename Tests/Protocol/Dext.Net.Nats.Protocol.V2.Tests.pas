unit Dext.Net.Nats.Protocol.V2.Tests;

interface

uses
  System.SysUtils,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Protocol.Headers,
  Dext.Net.Nats.Protocol.Control;

type
  [TestFixture('NATS Protocol V2 Codecs')]
  TDextNatsProtocolV2Tests = class
  public
    [Test, Category('Unit'), Category('Protocol'), Category('Parity')]
    procedure HeaderCodec_ShouldMatchFacade;

    [Test, Category('Unit'), Category('Protocol'), Category('Parity')]
    procedure ControlFrames_ShouldMatchFacade;
  end;

implementation

procedure TDextNatsProtocolV2Tests.HeaderCodec_ShouldMatchFacade;
var
  Headers, Parsed: TNatsHeaders;
  Block: TBytes;
  Status: Integer;
begin
  Headers := nil;
  Headers.Add('X-Test', 'one');
  Headers.Add('X-Test', 'two');
  Block := NatsEncodeHeaderBlock(Headers);
  Should(TEncoding.UTF8.GetString(Block)).Be(
    TEncoding.UTF8.GetString(Headers.Encode));
  NatsDecodeHeaderBlock('NATS/1.0 503 No Responders' + NATS_CRLF +
    'X-Test: one' + NATS_CRLF + NATS_CRLF, Parsed, Status);
  Should(Status).Be(503);
  Should(Parsed.GetValue('X-Test')).Be('one');
end;

procedure TDextNatsProtocolV2Tests.ControlFrames_ShouldMatchFacade;
begin
  Should(TEncoding.ASCII.GetString(NatsControlPing)).Be(
    TEncoding.ASCII.GetString(NatsEncodePing));
  Should(TEncoding.ASCII.GetString(NatsControlPong)).Be(
    TEncoding.ASCII.GetString(NatsEncodePong));
  Should(TEncoding.ASCII.GetString(NatsControlSub('a.b', 'q', 7))).Be(
    TEncoding.ASCII.GetString(NatsEncodeSub('a.b', 'q', 7)));
  Should(TEncoding.ASCII.GetString(NatsControlUnsub(7, 10))).Be(
    TEncoding.ASCII.GetString(NatsEncodeUnsub(7, 10)));
end;

end.
