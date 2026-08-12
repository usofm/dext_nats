{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Runtime parser selector facade                                  }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.ParserRuntime;

interface

uses
  Dext.Net.Nats.Protocol
{$IFDEF DEXT_NATS_PARSER_V2}
  , Dext.Net.Nats.Internal.Parser
{$ENDIF}
  ;

type
{$IFDEF DEXT_NATS_PARSER_V2}
  /// <summary>
  /// Cursor-based parser selected for explicit V2 validation builds.
  /// </summary>
  TDextNatsRuntimeFrameParser = TDextNatsFrameParserV2;
{$ELSE}
  /// <summary>
  /// Stable parser selected by default until the Delphi 13 cutover gate passes.
  /// </summary>
  TDextNatsRuntimeFrameParser = TDextNatsFrameParser;
{$ENDIF}

const
{$IFDEF DEXT_NATS_PARSER_V2}
  DEXT_NATS_RUNTIME_PARSER = 'v2';
{$ELSE}
  DEXT_NATS_RUNTIME_PARSER = 'v1';
{$ENDIF}

implementation

end.
