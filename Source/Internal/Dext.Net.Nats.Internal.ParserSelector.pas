unit Dext.Net.Nats.Internal.ParserSelector;

interface

uses
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Internal.Parser;

type
{$IFDEF DEXT_NATS_PARSER_V2}
  TDextNatsSelectedFrameParser = TDextNatsFrameParserV2;
{$ELSE}
  TDextNatsSelectedFrameParser = TDextNatsFrameParser;
{$ENDIF}

const
{$IFDEF DEXT_NATS_PARSER_V2}
  DEXT_NATS_SELECTED_PARSER = 'v2';
{$ELSE}
  DEXT_NATS_SELECTED_PARSER = 'v1';
{$ENDIF}

implementation

end.
