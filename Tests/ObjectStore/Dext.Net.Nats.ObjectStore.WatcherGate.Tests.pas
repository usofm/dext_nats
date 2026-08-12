unit Dext.Net.Nats.ObjectStore.WatcherGate.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.ObjectStore,
  Dext.Net.Nats.ObjectStore.WatcherGate;

type
  [TestFixture('NATS ObjectStore Watcher Gate')]
  TDextNatsObjectStoreWatcherGateTests = class
  public
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure Snapshot_ShouldDeliverMarkerAfterPendingReachesZero;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure IgnoreDeletes_ShouldStillAdvanceSnapshot;
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure UpdatesOnly_ShouldStartInitialDoneWithoutMarker;
  end;

implementation

procedure TDextNatsObjectStoreWatcherGateTests.Snapshot_ShouldDeliverMarkerAfterPendingReachesZero;
var
  Gate: TDextNatsObjectWatchGate;
  Names: TArray<string>;
  Info: TNatsObjectInfo;
begin
  SetLength(Names, 0);
  Gate := TDextNatsObjectWatchGate.Create(
    procedure(const AInfo: TNatsObjectInfo)
    begin
      SetLength(Names, Length(Names) + 1);
      if AInfo.IsEndOfInitial then Names[High(Names)] := '<end>'
      else Names[High(Names)] := AInfo.Name;
    end, False, False);
  try
    Info := Default(TNatsObjectInfo);
    Info.Name := 'a';
    Gate.HandleInfo(Info, 1);
    Info.Name := 'b';
    Gate.HandleInfo(Info, 0);
    Should(Length(Names)).Be(3);
    Should(Names[0]).Be('a');
    Should(Names[1]).Be('b');
    Should(Names[2]).Be('<end>');
    Should(Gate.InitialDone).Be(True);
  finally
    Gate.Free;
  end;
end;

procedure TDextNatsObjectStoreWatcherGateTests.IgnoreDeletes_ShouldStillAdvanceSnapshot;
var
  Gate: TDextNatsObjectWatchGate;
  Delivered: Integer;
  Marker: Boolean;
  Info: TNatsObjectInfo;
begin
  Delivered := 0;
  Marker := False;
  Gate := TDextNatsObjectWatchGate.Create(
    procedure(const AInfo: TNatsObjectInfo)
    begin
      if AInfo.IsEndOfInitial then Marker := True else Inc(Delivered);
    end, False, True);
  try
    Info := Default(TNatsObjectInfo);
    Info.Name := 'deleted';
    Info.Deleted := True;
    Gate.HandleInfo(Info, 0);
    Should(Delivered).Be(0);
    Should(Marker).Be(True);
  finally
    Gate.Free;
  end;
end;

procedure TDextNatsObjectStoreWatcherGateTests.UpdatesOnly_ShouldStartInitialDoneWithoutMarker;
var
  Gate: TDextNatsObjectWatchGate;
  Delivered: Integer;
begin
  Delivered := 0;
  Gate := TDextNatsObjectWatchGate.Create(
    procedure(const AInfo: TNatsObjectInfo)
    begin
      Inc(Delivered);
    end, True, False);
  try
    Should(Gate.InitialDone).Be(True);
    Gate.NotifyConsumerPending(0);
    Should(Delivered).Be(0);
  finally
    Gate.Free;
  end;
end;

end.
