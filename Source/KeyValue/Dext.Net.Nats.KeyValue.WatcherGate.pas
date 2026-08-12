unit Dext.Net.Nats.KeyValue.WatcherGate;

interface

uses
  System.SyncObjs,
  Dext.Net.Nats.KeyValue;

type
  TDextNatsKeyValueWatchGate = class
  private
    FLock: TCriticalSection;
    FHandler: TNatsKeyValueWatchHandler;
    FUpdatesOnly: Boolean;
    FIgnoreDeletes: Boolean;
    FStopped: Boolean;
    FInitDone: Boolean;
    FInitPendingKnown: Boolean;
    FInitPending: UInt64;
    FReceived: UInt64;
  public
    constructor Create(AHandler: TNatsKeyValueWatchHandler;
      AUpdatesOnly, AIgnoreDeletes: Boolean);
    destructor Destroy; override;
    procedure Stop;
    procedure HandleEntry(const AEntry: TNatsKeyValueEntry; ANumPending: Integer);
    procedure NotifyConsumerPending(ANumPending: UInt64);
    function InitialDone: Boolean;
  end;

implementation

constructor TDextNatsKeyValueWatchGate.Create(AHandler: TNatsKeyValueWatchHandler;
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

destructor TDextNatsKeyValueWatchGate.Destroy;
begin
  Stop;
  FLock.Free;
  inherited;
end;

procedure TDextNatsKeyValueWatchGate.Stop;
begin
  FLock.Enter;
  try FStopped := True; finally FLock.Leave; end;
end;

function TDextNatsKeyValueWatchGate.InitialDone: Boolean;
begin
  FLock.Enter;
  try Result := FInitDone; finally FLock.Leave; end;
end;

procedure TDextNatsKeyValueWatchGate.NotifyConsumerPending(ANumPending: UInt64);
var
  FireMarker: Boolean;
  Handler: TNatsKeyValueWatchHandler;
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
    Handler(TNatsKeyValueEntry.EndOfInitialMarker);
end;

procedure TDextNatsKeyValueWatchGate.HandleEntry(const AEntry: TNatsKeyValueEntry;
  ANumPending: Integer);
var
  FireMarker: Boolean;
  DeliverEntry: Boolean;
  Handler: TNatsKeyValueWatchHandler;
  Pending: UInt64;
begin
  FireMarker := False;
  Handler := nil;
  DeliverEntry := True;
  if ANumPending < 0 then Pending := 0 else Pending := UInt64(ANumPending);

  FLock.Enter;
  try
    if FStopped then Exit;
    Handler := FHandler;
    if FIgnoreDeletes and ((AEntry.Operation = kvoDelete) or
      (AEntry.Operation = kvoPurge)) then
      DeliverEntry := False;
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

  if DeliverEntry and Assigned(Handler) then Handler(AEntry);

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
    Handler(TNatsKeyValueEntry.EndOfInitialMarker);
end;

end.
