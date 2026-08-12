unit Dext.Net.Nats.JetStream.ObjectPaging.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.JetStream.ObjectPaging;

type
  [TestFixture('NATS JetStream Object Paging')]
  TDextNatsJetStreamObjectPagingTests = class
  public
    [Test, Category('Unit'), Category('JetStream')]
    procedure StreamPage_ShouldParseMultipleItems;

    [Test, Category('Unit'), Category('JetStream')]
    procedure ConsumerPage_ShouldParseMultipleItems;
  end;

implementation

procedure TDextNatsJetStreamObjectPagingTests.StreamPage_ShouldParseMultipleItems;
var
  Page: TNatsJsStreamInfoPage;
begin
  Page := NatsJsParseStreamInfoPage(
    '{"total":2,"offset":0,"limit":1024,"streams":[' +
    '{"config":{"name":"A"},"state":{"messages":1}},' +
    '{"config":{"name":"B"},"state":{"messages":2}}]}');
  Should(Page.Total).Be(2);
  Should(Length(Page.Items)).Be(2);
  Should(Page.Items[0].Name).Be('A');
  Should(Page.Items[1].Name).Be('B');
  Should(Page.Items[1].Messages).Be(UInt64(2));
end;

procedure TDextNatsJetStreamObjectPagingTests.ConsumerPage_ShouldParseMultipleItems;
var
  Page: TNatsJsConsumerInfoPage;
begin
  Page := NatsJsParseConsumerInfoPage(
    '{"total":2,"offset":0,"limit":1024,"consumers":[' +
    '{"stream_name":"ORDERS","name":"A","config":{}},' +
    '{"stream_name":"ORDERS","name":"B","config":{}}]}');
  Should(Page.Total).Be(2);
  Should(Length(Page.Items)).Be(2);
  Should(Page.Items[0].Name).Be('A');
  Should(Page.Items[1].Name).Be('B');
end;

end.
