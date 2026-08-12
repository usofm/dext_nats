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
  Dext.Net.Nats.JetStream.Json;

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

end.
