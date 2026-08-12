{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Runtime parser facade                                           }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.ParserRuntime;

interface

uses
  Dext.Net.Nats.Internal.Parser;

type
  /// <summary>
  /// Production NATS frame parser. Dext.Nats has no legacy-runtime
  /// compatibility requirement yet, so the cursor-based parser is the only
  /// runtime implementation.
  /// </summary>
  TDextNatsRuntimeFrameParser = TDextNatsFrameParserV2;

const
  DEXT_NATS_RUNTIME_PARSER = 'v2';

implementation

end.
