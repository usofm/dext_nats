unit Dext.Net.Nats.Protocol.V2.Tests;

interface

uses
  System.SysUtils,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Protocol.Headers,
  Dext.Net.Nats.Protocol.Control,
  Dext.Net.Nats.Protocol.Writer;

type
  [TestFixture('NATS Protocol V2 Codecs')]
  TDextNatsProtocolV2Tests = class
  public
    [Test, Category('Unit'), Category('Protocol'), Category('Parity')]
    procedure HeaderCodec_ShouldMatchFacade;
    [Test, Category('Unit'), Category('Protocol'), Category('Parity')]
    procedure ControlFrames_ShouldMatchFacade;
    [Test, Category('Unit'), Category('Protocol'), Category('Parity')]
    procedure PublishFrames_ShouldMatchFacade;
    [Test, Category('Unit'), Category('Protocol'), Category('Parity')]
    procedure ConnectFrame_ShouldMatchFacade;
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

procedure TDextNatsProtocolV2Tests.PublishFrames_ShouldMatchFacade;
var
  Payload: TBytes;
  Headers: TNatsHeaders;
begin
  Payload := TEncoding.UTF8.GetBytes('hello');
  Headers := nil;
  Headers.Add('X-Test', '1');
  Should(TEncoding.UTF8.GetString(NatsV2EncodePub('orders.a', '', Payload))).Be(
    TEncoding.UTF8.GetString(NatsEncodePub('orders.a', '', Payload)));
  Should(TEncoding.UTF8.GetString(NatsV2EncodePub('orders.a', '_INBOX.x', Payload))).Be(
    TEncoding.UTF8.GetString(NatsEncodePub('orders.a', '_INBOX.x', Payload)));
  Should(TEncoding.UTF8.GetString(NatsV2EncodeHPub('orders.a', '', Headers, Payload))).Be(
    TEncoding.UTF8.GetString(NatsEncodeHPub('orders.a', '', Headers, Payload)));
end;

procedure TDextNatsProtocolV2Tests.ConnectFrame_ShouldMatchFacade;
var
  Options: TNatsConnectOptions;
begin
  Options := TNatsConnectOptions.CreateDefault;
  Options.Name := 'dext-test';
  Options.User := 'u';
  Options.Password := 'p';
  Should(TEncoding.UTF8.GetString(NatsV2EncodeConnect(Options))).Be(
    TEncoding.UTF8.GetString(NatsEncodeConnect(Options)));
end;

end.
