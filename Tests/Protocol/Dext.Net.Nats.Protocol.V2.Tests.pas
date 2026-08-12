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
  [TestFixture('NATS Protocol Codecs')]
  TDextNatsProtocolV2Tests = class
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure HeaderCodec_ShouldEncodeAndDecode;
    [Test, Category('Unit'), Category('Protocol')]
    procedure ControlFrames_ShouldMatchWireContract;
    [Test, Category('Unit'), Category('Protocol')]
    procedure PublishFrames_ShouldMatchWireContract;
    [Test, Category('Unit'), Category('Protocol')]
    procedure ConnectFrame_ShouldMatchWireContract;
  end;

implementation

procedure TDextNatsProtocolV2Tests.HeaderCodec_ShouldEncodeAndDecode;
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
    'NATS/1.0' + NATS_CRLF + 'X-Test: one' + NATS_CRLF +
    'X-Test: two' + NATS_CRLF + NATS_CRLF);

  NatsDecodeHeaderBlock('NATS/1.0 503 No Responders' + NATS_CRLF +
    'X-Test: one' + NATS_CRLF + NATS_CRLF, Parsed, Status);
  Should(Status).Be(503);
  Should(Parsed.GetValue('X-Test')).Be('one');
end;

procedure TDextNatsProtocolV2Tests.ControlFrames_ShouldMatchWireContract;
begin
  Should(TEncoding.ASCII.GetString(NatsControlPing)).Be('PING' + NATS_CRLF);
  Should(TEncoding.ASCII.GetString(NatsControlPong)).Be('PONG' + NATS_CRLF);
  Should(TEncoding.UTF8.GetString(NatsControlSub('a.b', 'q', 7))).Be(
    'SUB a.b q 7' + NATS_CRLF);
  Should(TEncoding.UTF8.GetString(NatsControlSub('a.b', '', 7))).Be(
    'SUB a.b 7' + NATS_CRLF);
  Should(TEncoding.ASCII.GetString(NatsControlUnsub(7, 10))).Be(
    'UNSUB 7 10' + NATS_CRLF);
  Should(TEncoding.ASCII.GetString(NatsControlUnsub(7, 0))).Be(
    'UNSUB 7' + NATS_CRLF);
end;

procedure TDextNatsProtocolV2Tests.PublishFrames_ShouldMatchWireContract;
var
  Payload: TBytes;
  Headers: TNatsHeaders;
  HeaderBlock: TBytes;
  Wire: string;
begin
  Payload := TEncoding.UTF8.GetBytes('hello');
  Should(TEncoding.UTF8.GetString(NatsV2EncodePub('orders.a', '', Payload))).Be(
    'PUB orders.a 5' + NATS_CRLF + 'hello' + NATS_CRLF);
  Should(TEncoding.UTF8.GetString(NatsV2EncodePub('orders.a', '_INBOX.x', Payload))).Be(
    'PUB orders.a _INBOX.x 5' + NATS_CRLF + 'hello' + NATS_CRLF);

  Headers := nil;
  Headers.Add('X-Test', '1');
  HeaderBlock := NatsEncodeHeaderBlock(Headers);
  Wire := TEncoding.UTF8.GetString(NatsV2EncodeHPub('orders.a', '', Headers, Payload));
  Should(Wire.StartsWith('HPUB orders.a ' + IntToStr(Length(HeaderBlock)) + ' ' +
    IntToStr(Length(HeaderBlock) + Length(Payload)) + NATS_CRLF)).BeTrue;
  Should(Wire.Contains('X-Test: 1')).BeTrue;
  Should(Wire.EndsWith('hello' + NATS_CRLF)).BeTrue;
end;

procedure TDextNatsProtocolV2Tests.ConnectFrame_ShouldMatchWireContract;
var
  Options: TNatsConnectOptions;
  Wire: string;
begin
  Options := TNatsConnectOptions.CreateDefault;
  Options.Name := 'dext-test';
  Options.User := 'u';
  Options.Password := 'p';
  Wire := TEncoding.UTF8.GetString(NatsV2EncodeConnect(Options));
  Should(Wire.StartsWith('CONNECT {')).BeTrue;
  Should(Wire.EndsWith(NATS_CRLF)).BeTrue;
  Should(Wire.Contains('"name":"dext-test"')).BeTrue;
  Should(Wire.Contains('"user":"u"')).BeTrue;
  Should(Wire.Contains('"pass":"p"')).BeTrue;
  Should(Wire.Contains('"no_responders":true')).BeTrue;
end;

end.
