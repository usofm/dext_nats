{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Internal bounded message dispatcher                             }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Internal.Dispatcher;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Collections.Channels;

type
  EDextNatsBackpressure = class(Exception);

  TNatsDispatchOverflowPolicy = (
    dopBlock,
    dopReject
  );

  TDextNatsDispatchHandler<T> = reference to procedure(const AItem: T);
  TDextNatsDispatchErrorHandler = reference to procedure(const AError: Exception);

  /// <summary>
  ///   Small worker pool backed by Dext's bounded channel. It is intentionally
  ///   generic so the core NATS client can queue a strongly typed work record
  ///   without allocating an anonymous closure per message.
  /// </summary>
  TDextNatsBoundedDispatcher<T> = class
  private
    FChannel: IChannel<T>;
    FWorkers: TArray<TThread>;
    FHandler: TDextNatsDispatchHandler<T>;
    FOnError: TDextNatsDispatchErrorHandler;
    FOverflowPolicy: TNatsDispatchOverflowPolicy;
    FState: Integer;
    procedure WorkerLoop;
  public
    constructor Create(AWorkerCount, ACapacity: Integer;
      const AHandler: TDextNatsDispatchHandler<T>;
      AOverflowPolicy: TNatsDispatchOverflowPolicy = dopReject);
    destructor Destroy; override;

    procedure Start;
    procedure Stop;
    procedure Dispatch(const AItem: T);
    function TryDispatch(const AItem: T): Boolean;
    function IsRunning: Boolean;

    property OnError: TDextNatsDispatchErrorHandler read FOnError write FOnError;
    property OverflowPolicy: TNatsDispatchOverflowPolicy read FOverflowPolicy write FOverflowPolicy;
  end;

implementation

constructor TDextNatsBoundedDispatcher<T>.Create(AWorkerCount, ACapacity: Integer;
  const AHandler: TDextNatsDispatchHandler<T>;
  AOverflowPolicy: TNatsDispatchOverflowPolicy);
var
  I: Integer;
begin
  inherited Create;
  if AWorkerCount <= 0 then
    raise EArgumentOutOfRangeException.Create('AWorkerCount must be > 0');
  if ACapacity <= 0 then
    raise EArgumentOutOfRangeException.Create('ACapacity must be > 0');
  if not Assigned(AHandler) then
    raise EArgumentNilException.Create('AHandler');

  FHandler := AHandler;
  FOverflowPolicy := AOverflowPolicy;
  FChannel := TChannel<T>.CreateBounded(ACapacity);
  SetLength(FWorkers, AWorkerCount);
  for I := 0 to High(FWorkers) do
  begin
    FWorkers[I] := TThread.CreateAnonymousThread(WorkerLoop);
    FWorkers[I].FreeOnTerminate := False;
  end;
  Start;
end;

destructor TDextNatsBoundedDispatcher<T>.Destroy;
begin
  Stop;
  inherited;
end;

function TDextNatsBoundedDispatcher<T>.IsRunning: Boolean;
begin
  Result := TInterlocked.Read(FState) = 1;
end;

procedure TDextNatsBoundedDispatcher<T>.Start;
var
  I: Integer;
begin
  if TInterlocked.CompareExchange(FState, 1, 0) <> 0 then
    Exit;
  for I := 0 to High(FWorkers) do
    FWorkers[I].Start;
end;

procedure TDextNatsBoundedDispatcher<T>.Stop;
var
  I: Integer;
begin
  if TInterlocked.Exchange(FState, 0) = 0 then
    Exit;

  FChannel.Close;
  for I := 0 to High(FWorkers) do
    if Assigned(FWorkers[I]) then
    begin
      FWorkers[I].WaitFor;
      FreeAndNil(FWorkers[I]);
    end;
end;

function TDextNatsBoundedDispatcher<T>.TryDispatch(const AItem: T): Boolean;
begin
  Result := IsRunning and FChannel.TryWrite(AItem);
end;

procedure TDextNatsBoundedDispatcher<T>.Dispatch(const AItem: T);
begin
  if not IsRunning then
    raise EDextNatsBackpressure.Create('NATS dispatcher is stopped');

  case FOverflowPolicy of
    dopBlock:
      FChannel.Write(AItem);
    dopReject:
      if not FChannel.TryWrite(AItem) then
        raise EDextNatsBackpressure.Create('NATS dispatcher queue is full');
  end;
end;

procedure TDextNatsBoundedDispatcher<T>.WorkerLoop;
var
  Item: T;
begin
  while IsRunning or not FChannel.IsClosed do
  begin
    try
      Item := FChannel.Read;
    except
      on E: Exception do
      begin
        if FChannel.IsClosed then
          Break;
        if Assigned(FOnError) then
        begin
          try
            FOnError(E);
          except
          end;
        end;
        Continue;
      end;
    end;

    try
      FHandler(Item);
    except
      on E: Exception do
        if Assigned(FOnError) then
        begin
          try
            FOnError(E);
          except
          end;
        end;
    end;
  end;
end;

end.
