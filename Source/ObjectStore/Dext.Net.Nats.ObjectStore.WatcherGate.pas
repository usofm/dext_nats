unit Dext.Net.Nats.ObjectStore.WatcherGate;

interface

uses
  System.SyncObjs,
  Dext.Net.Nats.ObjectStore;

type
  TDextNatsObjectWatchGate = class
  private
    FLock: TCriticalSection;
    FHandler: TNatsObjectStoreWatchHandler;
    FUpdatesOnly: Boolean;
    FIgnoreDeletes: Boolean;
    FStopped: Boolean;
    FInitDone: Boolean;
    FInitPendingKnown: Boolean;
    FInitPending: UInt64;
    FReceived: UInt64;
  public
    constructor Create(AHandler: TNatsObjectStoreWatchHandler;
      AUpdatesOnly, AIgnoreDeletes: Boolean);
    destructor Destroy; override;
    procedure Stop;
    procedure HandleInfo(const AInfo: TNatsObjectInfo; ANumPending: Integer);
    procedure NotifyConsumerPending(ANumPending: UInt64);
    function InitialDone: Boolean;
  end;

implementation

constructor TDextNatsObjectWatchGate.Create(AHandler: TNatsObjectStoreWatchHandler;
  AUpdatesOnly, AIgnoreDeletes: Boolean);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FHandler := AHandler;
  FUpdatesOnly := AUpdatesOnly;
  FIgnoreDeletes := AIgnoreDeletes;
  FInitDone := AUpdatesOnly;
  FInitPendingKnown := AUpdatesOnly;
end;

destructor TDextNatsObjectWatchGate.Destroy;
begin
  Stop;
  FLock.Free;
  inherited;
end;

procedure TDextNatsObjectWatchGate.Stop;
begin
  FLock.Enter;
  try FStopped := True; finally FLock.Leave; end;
end;

function TDextNatsObjectWatchGate.InitialDone: Boolean;
begin
  FLock.Enter;
  try Result := FInitDone; finally FLock.Leave; end;
end;

procedure TDextNatsObjectWatchGate.NotifyConsumerPending(ANumPending: UInt64);
var
  FireMarker: Boolean;
  Handler: TNatsObjectStoreWatchHandler;
begin
  FireMarker := False;
  Handler := nil;
  FLock.Enter;
  try
    if FStopped or FUpdatesOnly then Exit;
    if not FInitPendingKnown then
    begin
      FInitPending := ANumPending;
      FInitPendingKnown := True;
    end;
    if (not FInitDone) and (FReceived >= FInitPending) then
    begin
      FInitDone := True;
      FireMarker := True;
      Handler := FHandler;
    end;
  finally
    FLock.Leave;
  end;
  if FireMarker and Assigned(Handler) then
    Handler(TNatsObjectInfo.EndOfInitialMarker);
end;

procedure TDextNatsObjectWatchGate.HandleInfo(const AInfo: TNatsObjectInfo;
  ANumPending: Integer);
var
  FireMarker: Boolean;
  DeliverInfo: Boolean;
  Handler: TNatsObjectStoreWatchHandler;
  Pending: UInt64;
begin
  FireMarker := False;
  Handler := nil;
  DeliverInfo := True;
  if ANumPending < 0 then Pending := 0 else Pending := UInt64(ANumPending);

  FLock.Enter;
  try
    if FStopped then Exit;
    Handler := FHandler;
    if FIgnoreDeletes and AInfo.Deleted then DeliverInfo := False;
    if not FInitDone then
    begin
      if not FInitPendingKnown then
      begin
        FInitPending := Pending + 1;
        FInitPendingKnown := True;
      end;
      Inc(FReceived);
    end;
  finally
    FLock.Leave;
  end;

  if DeliverInfo and Assigned(Handler) then Handler(AInfo);

  FLock.Enter;
  try
    if FStopped or FInitDone or FUpdatesOnly then Exit;
    if (FReceived >= FInitPending) or (Pending = 0) then
    begin
      FInitDone := True;
      FireMarker := True;
      Handler := FHandler;
    end;
  finally
    FLock.Leave;
  end;

  if FireMarker and Assigned(Handler) then
    Handler(TNatsObjectInfo.EndOfInitialMarker);
end;

end.
