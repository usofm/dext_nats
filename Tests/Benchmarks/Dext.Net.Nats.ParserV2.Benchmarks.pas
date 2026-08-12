{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Production parser benchmark harness                             }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.ParserV2.Benchmarks;

interface

uses
  System.SysUtils,
  System.Diagnostics,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Internal.Parser;

type
  [TestFixture('NATS Parser V2 Benchmark')]
  TDextNatsParserV2BenchmarkTests = class
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure CursorBuffer_ShouldNotCompactPerFrame;
    [Test, Category('Benchmark'), Explicit('Set DEXT_NATS_RUN_BENCH=1')]
    procedure Throughput_ShouldReportFramesPerSec;
  end;

implementation

function BuildPingBatch(ACount: Integer): TBytes;
var
  Wire: RawByteString;
  I: Integer;
begin
  Wire := '';
  for I := 1 to ACount do
    Wire := Wire + 'PING'#13#10;
  Result := BytesOf(Wire);
end;

procedure TDextNatsParserV2BenchmarkTests.CursorBuffer_ShouldNotCompactPerFrame;
const
  FRAME_COUNT = 512;
var
  Parser: TDextNatsFrameParserV2;
  Wire: TBytes;
  Frame: TNatsFrame;
  Count: Integer;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    Wire := BuildPingBatch(FRAME_COUNT);
    Parser.Append(Wire, Length(Wire));
    Count := 0;
    while Parser.TryReadFrame(Frame) do
      Inc(Count);
    Should(Count).Be(FRAME_COUNT);
    Should(Parser.BufferCompactions).Be(Int64(0));
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2BenchmarkTests.Throughput_ShouldReportFramesPerSec;
const
  FRAME_COUNT = 20000;
var
  Parser: TDextNatsFrameParserV2;
  Wire: TBytes;
  Frame: TNatsFrame;
  Count: Integer;
  Sw: TStopwatch;
  ElapsedMs: Int64;
  Rate: Double;
begin
  Wire := BuildPingBatch(FRAME_COUNT);
  Parser := TDextNatsFrameParserV2.Create;
  try
    Parser.Append(Wire, Length(Wire));
    Count := 0;
    Sw := TStopwatch.StartNew;
    while Parser.TryReadFrame(Frame) do
      Inc(Count);
    Sw.Stop;
    ElapsedMs := Sw.ElapsedMilliseconds;
    if ElapsedMs <= 0 then
      ElapsedMs := 1;

    Should(Count).Be(FRAME_COUNT);
    Should(Parser.BufferCompactions).Be(Int64(0));
    Rate := FRAME_COUNT * 1000.0 / ElapsedMs;
    WriteLn(Format('Parser V2: %.0f frames/sec (%d ms)', [Rate, ElapsedMs]));
  finally
    Parser.Free;
  end;
end;

end.
