unit Dext.Net.Nats.ParserRuntime.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.ParserRuntime;

type
  [TestFixture('NATS Runtime Parser Selector')]
  TDextNatsParserRuntimeTests = class
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure RuntimeParser_ShouldMatchCompileDefine;
  end;

implementation

procedure TDextNatsParserRuntimeTests.RuntimeParser_ShouldMatchCompileDefine;
begin
{$IFDEF DEXT_NATS_PARSER_V2}
  Should(string(DEXT_NATS_RUNTIME_PARSER)).Be('v2');
{$ELSE}
  Should(string(DEXT_NATS_RUNTIME_PARSER)).Be('v1');
{$ENDIF}
end;

end.
