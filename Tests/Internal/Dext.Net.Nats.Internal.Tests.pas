{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Internal performance primitives tests                           }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Internal.Tests;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  Dext.Core.Span,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Internal.Buffer,
  Dext.Net.Nats.Internal.Dispatcher;

type
  [TestFixture('NATS Internal Performance Primitives')]
  TDextNatsInternalTests = class
  public
    [Test, Category('Unit'), Category('Performance')]
    procedure ReadBuffer_Consume_ShouldAdvanceWithoutLosingUnreadBytes;
    [Test, Category('Unit'), Category('Performance')]
    procedure ReadBuffer_AppendAfterConsume_ShouldReuseCapacity;
    [Test, Category('Unit'), Category('Performance')]
    procedure ReadBuffer_WritableSpan_ShouldFillWithoutAppendCopy;
    [Test, Category('Unit'), Category('Performance')]
    procedure ReadBuffer_CommitWrite_ShouldRejectOverflow;
    [Test, Category('Unit'), Category('Performance')]
    procedure ReadBuffer_IndexOfCrLf_ShouldFindPairWithSimdFallback;
    [Test, Category('Unit'), Category('Concurrency')]
    procedure Dispatcher_ShouldProcessQueuedItems;
    [Test, Category('Unit'), Category('Concurrency')]
    procedure Dispatcher_TryDispatch_ShouldApplyBoundedBackpressure;
    [Test, Category('Unit'), Category('Concurrency')]
    procedure Dispatcher_Stop_ShouldDrainAcceptedItems;
  end;

implementation

function BytesOf(const S: string): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(S);
end;

function SpanText(const ASpan: TByteSpan): string;
var
  B: TBytes;
begin
  SetLength(B, ASpan.Length);
  if ASpan.Length > 0 then
    Move(ASpan.Data^, B[0], ASpan.Length);
  Result := TEncoding.UTF8.GetString(B);
end;

procedure TDextNatsInternalTests.ReadBuffer_Consume_ShouldAdvanceWithoutLosingUnreadBytes;
var
  Buffer: TDextNatsReadBuffer;
  Data: TBytes;
begin
  Buffer := TDextNatsReadBuffer.Create(256);
  try
    Data := BytesOf('INFO 12345');
    Buffer.Append(Data, Length(Data));
    Should(Buffer.Available).Be(10);
    Buffer.Consume(5);
    Should(Buffer.Available).Be(5);
    Should(SpanText(Buffer.DataSpan)).Be('12345');
  finally
    Buffer.Free;
  end;
end;

procedure TDextNatsInternalTests.ReadBuffer_AppendAfterConsume_ShouldReuseCapacity;
var
  Buffer: TDextNatsReadBuffer;
  A, B: TBytes;
  InitialCapacity: Integer;
begin
  Buffer := TDextNatsReadBuffer.Create(256);
  try
    InitialCapacity := Buffer.Capacity;
    A := BytesOf(StringOfChar('A', 200));
    B := BytesOf(StringOfChar('B', 100));
    Buffer.Append(A, Length(A));
    Buffer.Consume(150);
    Buffer.Append(B, Length(B));
    Should(Buffer.Available).Be(150);
    Should(Buffer.Capacity).Be(InitialCapacity);
    Should(SpanText(Buffer.DataSpan)).Be(StringOfChar('A', 50) + StringOfChar('B', 100));
  finally
    Buffer.Free;
  end;
end;

procedure TDextNatsInternalTests.ReadBuffer_WritableSpan_ShouldFillWithoutAppendCopy;
var
  Buffer: TDextNatsReadBuffer;
  Dest: TByteSpan;
  Src: TBytes;
  InitialCapacity: Integer;
begin
  Buffer := TDextNatsReadBuffer.Create(256);
  try
    InitialCapacity := Buffer.Capacity;
    Src := BytesOf('PING');
    Dest := Buffer.WritableSpan(Length(Src));
    Should(Dest.Length >= Length(Src)).BeTrue;
    Move(Src[0], Dest.Data^, Length(Src));
    Buffer.CommitWrite(Length(Src));
    Should(Buffer.Available).Be(4);
    Should(Buffer.Capacity).Be(InitialCapacity);
    Should(SpanText(Buffer.DataSpan)).Be('PING');

    Buffer.Consume(4);
    Should(Buffer.Available).Be(0);
    Dest := Buffer.WritableSpan(8);
    Src := BytesOf('PONG');
    Move(Src[0], Dest.Data^, Length(Src));
    Buffer.CommitWrite(Length(Src));
    Should(SpanText(Buffer.DataSpan)).Be('PONG');
  finally
    Buffer.Free;
  end;
end;

procedure TDextNatsInternalTests.ReadBuffer_CommitWrite_ShouldRejectOverflow;
var
  Buffer: TDextNatsReadBuffer;
  Dest: TByteSpan;
begin
  Buffer := TDextNatsReadBuffer.Create(256);
  try
    Dest := Buffer.WritableSpan(16);
    Should(procedure begin Buffer.CommitWrite(Dest.Length + 1); end)
      .Throw(EArgumentOutOfRangeException);
    Buffer.CommitWrite(0);
    Should(Buffer.Available).Be(0);
  finally
    Buffer.Free;
  end;
end;

procedure TDextNatsInternalTests.ReadBuffer_IndexOfCrLf_ShouldFindPairWithSimdFallback;
var
  Buffer: TDextNatsReadBuffer;
  Data: TBytes;
begin
  Buffer := TDextNatsReadBuffer.Create(256);
  try
    Data := BytesOf('abc' + #13#10 + 'de' + #13 + 'f' + #13#10);
    Buffer.Append(Data, Length(Data));
    Should(Buffer.IndexOfCrLf(0)).Be(3);
    Should(Buffer.IndexOfCrLf(4)).Be(9);
    Should(Buffer.IndexOfCrLf(10)).Be(-1);

    Buffer.Consume(4);
    Should(Buffer.IndexOfCrLf(0)).Be(5);

    Buffer.Clear;
    Data := BytesOf('no-terminator' + #13);
    Buffer.Append(Data, Length(Data));
    Should(Buffer.IndexOfCrLf(0)).Be(-1);

    Buffer.Clear;
    Data := BytesOf(#13#10);
    Buffer.Append(Data, Length(Data));
    Should(Buffer.IndexOfCrLf(0)).Be(0);
  finally
    Buffer.Free;
  end;
end;

procedure TDextNatsInternalTests.Dispatcher_ShouldProcessQueuedItems;
var
  Dispatcher: TDextNatsBoundedDispatcher<Integer>;
  Done: TEvent;
  Sum: Integer;
begin
  Done := TEvent.Create(nil, True, False, '');
  Sum := 0;
  Dispatcher := TDextNatsBoundedDispatcher<Integer>.Create(1, 8,
    procedure(const AItem: Integer)
    begin
      TInterlocked.Add(Sum, AItem);
      if TInterlocked.CompareExchange(Sum, 0, 0) = 6 then
        Done.SetEvent;
    end);
  try
    Dispatcher.Dispatch(1);
    Dispatcher.Dispatch(2);
    Dispatcher.Dispatch(3);
    Should(Done.WaitFor(2000) = wrSignaled).BeTrue;
    Should(TInterlocked.CompareExchange(Sum, 0, 0)).Be(6);
  finally
    Dispatcher.Free;
    Done.Free;
  end;
end;

procedure TDextNatsInternalTests.Dispatcher_TryDispatch_ShouldApplyBoundedBackpressure;
var
  Dispatcher: TDextNatsBoundedDispatcher<Integer>;
  Entered, ReleaseWorker: TEvent;
begin
  Entered := TEvent.Create(nil, True, False, '');
  ReleaseWorker := TEvent.Create(nil, True, False, '');
  Dispatcher := TDextNatsBoundedDispatcher<Integer>.Create(1, 1,
    procedure(const AItem: Integer)
    begin
      if AItem = 1 then
      begin
        Entered.SetEvent;
        ReleaseWorker.WaitFor(2000);
      end;
    end);
  try
    Should(Dispatcher.TryDispatch(1)).BeTrue;
    Should(Entered.WaitFor(2000) = wrSignaled).BeTrue;
    Should(Dispatcher.TryDispatch(2)).BeTrue;
    Should(Dispatcher.TryDispatch(3)).BeFalse;
    ReleaseWorker.SetEvent;
  finally
    ReleaseWorker.SetEvent;
    Dispatcher.Free;
    ReleaseWorker.Free;
    Entered.Free;
  end;
end;

procedure TDextNatsInternalTests.Dispatcher_Stop_ShouldDrainAcceptedItems;
var
  Dispatcher: TDextNatsBoundedDispatcher<Integer>;
  Sum: Integer;
begin
  Sum := 0;
  Dispatcher := TDextNatsBoundedDispatcher<Integer>.Create(1, 8,
    procedure(const AItem: Integer)
    begin
      TInterlocked.Add(Sum, AItem);
    end,
    dopBlock);
  try
    Dispatcher.Dispatch(1);
    Dispatcher.Dispatch(2);
    Dispatcher.Dispatch(3);
    Dispatcher.Stop;
    Should(TInterlocked.CompareExchange(Sum, 0, 0)).Be(6);
    Should(Dispatcher.IsRunning).BeFalse;
  finally
    Dispatcher.Free;
  end;
end;

end.
