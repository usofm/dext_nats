{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Parser V1/V2 benchmark harness                                  }
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
    procedure V1VsV2_Throughput_ShouldReportFramesPerSec;
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
    { All bytes were appended up-front; consuming frames must only advance the
      cursor. The old parser shifts the unread tail after every frame. }
    Should(Parser.BufferCompactions).Be(Int64(0));
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2BenchmarkTests.V1VsV2_Throughput_ShouldReportFramesPerSec;
const
  FRAME_COUNT = 20000;
var
  V1: TDextNatsFrameParser;
  V2: TDextNatsFrameParserV2;
  Wire: TBytes;
  Frame: TNatsFrame;
  Count: Integer;
  Sw: TStopwatch;
  V1Ms, V2Ms: Int64;
  V1Rate, V2Rate: Double;
begin
  Wire := BuildPingBatch(FRAME_COUNT);

  V1 := TDextNatsFrameParser.Create;
  try
    V1.Append(Wire, Length(Wire));
    Count := 0;
    Sw := TStopwatch.StartNew;
    while V1.TryReadFrame(Frame) do
      Inc(Count);
    Sw.Stop;
    V1Ms := Sw.ElapsedMilliseconds;
    Should(Count).Be(FRAME_COUNT);
  finally
    V1.Free;
  end;

  V2 := TDextNatsFrameParserV2.Create;
  try
    V2.Append(Wire, Length(Wire));
    Count := 0;
    Sw := TStopwatch.StartNew;
    while V2.TryReadFrame(Frame) do
      Inc(Count);
    Sw.Stop;
    V2Ms := Sw.ElapsedMilliseconds;
    Should(Count).Be(FRAME_COUNT);
    Should(V2.BufferCompactions).Be(Int64(0));
  finally
    V2.Free;
  end;

  if V1Ms <= 0 then V1Ms := 1;
  if V2Ms <= 0 then V2Ms := 1;
  V1Rate := FRAME_COUNT * 1000.0 / V1Ms;
  V2Rate := FRAME_COUNT * 1000.0 / V2Ms;

  WriteLn(Format('Parser V1: %.0f frames/sec (%d ms)', [V1Rate, V1Ms]));
  WriteLn(Format('Parser V2: %.0f frames/sec (%d ms)', [V2Rate, V2Ms]));
  WriteLn(Format('Parser V2/V1 ratio: %.2fx', [V2Rate / V1Rate]));
end;

end.
