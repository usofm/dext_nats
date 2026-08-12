unit Dext.Net.Nats.ParserRuntime.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.ParserRuntime;

type
  [TestFixture('NATS Runtime Parser')]
  TDextNatsParserRuntimeTests = class
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure RuntimeParser_ShouldBeV2;
  end;

implementation

procedure TDextNatsParserRuntimeTests.RuntimeParser_ShouldBeV2;
begin
  Should(string(DEXT_NATS_RUNTIME_PARSER)).Be('v2');
end;

end.
