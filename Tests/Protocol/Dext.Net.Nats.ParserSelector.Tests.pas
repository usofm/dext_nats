unit Dext.Net.Nats.ParserSelector.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Internal.ParserSelector;

type
  [TestFixture('NATS Parser Selector')]
  TDextNatsParserSelectorTests = class
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure SelectedParser_ShouldMatchCompileDefine;
  end;

implementation

procedure TDextNatsParserSelectorTests.SelectedParser_ShouldMatchCompileDefine;
begin
{$IFDEF DEXT_NATS_PARSER_V2}
  Should(string(DEXT_NATS_SELECTED_PARSER)).Be('v2');
{$ELSE}
  Should(string(DEXT_NATS_SELECTED_PARSER)).Be('v1');
{$ENDIF}
end;

end.
