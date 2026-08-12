{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Opt-in bounded worker dispatch for subscriptions                }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Dispatching;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  Dext.Net.Nats,
  Dext.Net.Nats.Internal.Dispatcher;

type
  /// <summary>Action to take when the bounded subscription queue is full.</summary>
  TNatsDispatchFullMode = (
    dfmReject,
    dfmBlock
  );

  /// <summary>Configuration for an opt-in worker-dispatched subscription.</summary>
  TNatsDispatchOptions = record
    WorkerCount: Integer;
    Capacity: Integer;
    FullMode: TNatsDispatchFullMode;
    class function CreateDefault: TNatsDispatchOptions; static;
  end;

  TNatsDispatchBackpressureHandler = reference to procedure(const AMsg: TNatsMsg);
  TNatsDispatchErrorHandler = reference to procedure(const AError: Exception);

  /// <summary>
  ///   Subscription adapter that keeps socket parsing on TDextNatsClient's
  ///   receive thread but moves application handlers onto a bounded Dext
  ///   Channel worker pool. The adapter does not own the client.
  /// </summary>
  TDextNatsDispatchedSubscription = class
  private
    FClient: TDextNatsClient;
    FDispatcher: TDextNatsBoundedDispatcher<TNatsMsg>;
    FHandler: TNatsMsgHandler;
    FOnBackpressure: TNatsDispatchBackpressureHandler;
    FOnError: TNatsDispatchErrorHandler;
    FLock: TCriticalSection;
    FSid: Integer;
    FActive: Integer;
    procedure HandleIncoming(const AMsg: TNatsMsg);
    procedure HandleWorkerError(const AError: Exception);
  public
    constructor Create(AClient: TDextNatsClient; const ASubject: string;
      const AHandler: TNatsMsgHandler; const AOptions: TNatsDispatchOptions;
      const AQueue: string = ''); overload;
    constructor Create(AClient: TDextNatsClient; const ASubject: string;
      const AHandler: TNatsMsgHandler; const AQueue: string = ''); overload;
    destructor Destroy; override;

    procedure Stop;
    function IsActive: Boolean;

    property Sid: Integer read FSid;
    property OnBackpressure: TNatsDispatchBackpressureHandler read FOnBackpressure write FOnBackpressure;
    property OnError: TNatsDispatchErrorHandler read FOnError write FOnError;
  end;

implementation

class function TNatsDispatchOptions.CreateDefault: TNatsDispatchOptions;
begin
  Result.WorkerCount := 4;
  Result.Capacity := 8192;
  Result.FullMode := dfmReject;
end;

constructor TDextNatsDispatchedSubscription.Create(AClient: TDextNatsClient;
  const ASubject: string; const AHandler: TNatsMsgHandler; const AQueue: string);
begin
  Create(AClient, ASubject, AHandler, TNatsDispatchOptions.CreateDefault, AQueue);
end;

constructor TDextNatsDispatchedSubscription.Create(AClient: TDextNatsClient;
  const ASubject: string; const AHandler: TNatsMsgHandler;
  const AOptions: TNatsDispatchOptions; const AQueue: string);
var
  Overflow: TNatsDispatchOverflowPolicy;
begin
  inherited Create;
  if AClient = nil then
    raise EArgumentNilException.Create('AClient');
  if ASubject = '' then
    raise EArgumentException.Create('ASubject must not be empty');
  if not Assigned(AHandler) then
    raise EArgumentException.Create('AHandler must be assigned');
  if AOptions.WorkerCount <= 0 then
    raise EArgumentOutOfRangeException.Create('WorkerCount must be > 0');
  if AOptions.Capacity <= 0 then
    raise EArgumentOutOfRangeException.Create('Capacity must be > 0');

  FClient := AClient;
  FHandler := AHandler;
  FLock := TCriticalSection.Create;

  if AOptions.FullMode = dfmBlock then
    Overflow := dopBlock
  else
    Overflow := dopReject;

  FDispatcher := TDextNatsBoundedDispatcher<TNatsMsg>.Create(
    AOptions.WorkerCount,
    AOptions.Capacity,
    procedure(const AMsg: TNatsMsg)
    begin
      FHandler(AMsg);
    end,
    Overflow);
  FDispatcher.OnError := HandleWorkerError;

  TInterlocked.Exchange(FActive, 1);
  try
    FSid := FClient.Subscribe(ASubject, HandleIncoming, AQueue);
  except
    TInterlocked.Exchange(FActive, 0);
    FDispatcher.Free;
    FDispatcher := nil;
    FLock.Free;
    FLock := nil;
    raise;
  end;
end;

destructor TDextNatsDispatchedSubscription.Destroy;
begin
  Stop;
  FLock.Free;
  inherited;
end;

function TDextNatsDispatchedSubscription.IsActive: Boolean;
begin
  Result := TInterlocked.Read(FActive) = 1;
end;

procedure TDextNatsDispatchedSubscription.HandleWorkerError(const AError: Exception);
begin
  if Assigned(FOnError) then
  try
    FOnError(AError);
  except
  end;
end;

procedure TDextNatsDispatchedSubscription.HandleIncoming(const AMsg: TNatsMsg);
var
  Accepted: Boolean;
begin
  if not IsActive then
    Exit;

  FLock.Enter;
  try
    if not IsActive or (FDispatcher = nil) then
      Exit;

    if FDispatcher.OverflowPolicy = dopBlock then
    begin
      FDispatcher.Dispatch(AMsg);
      Accepted := True;
    end
    else
      Accepted := FDispatcher.TryDispatch(AMsg);
  finally
    FLock.Leave;
  end;

  if not Accepted and Assigned(FOnBackpressure) then
  try
    FOnBackpressure(AMsg);
  except
  end;
end;

procedure TDextNatsDispatchedSubscription.Stop;
begin
  if TInterlocked.Exchange(FActive, 0) = 0 then
    Exit;

  if (FClient <> nil) and (FSid > 0) then
  begin
    try
      FClient.Unsubscribe(FSid);
    except
    end;
    FSid := 0;
  end;

  { Wait for any receive-thread enqueue already in progress before freeing the
    worker pool. This avoids racing a late callback with dispatcher teardown. }
  FLock.Enter;
  try
    FreeAndNil(FDispatcher);
  finally
    FLock.Leave;
  end;
end;

end.
