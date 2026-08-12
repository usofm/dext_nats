unit Dext.Net.Nats.KeyValue.WatcherGate.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.KeyValue,
  Dext.Net.Nats.KeyValue.WatcherGate;

type
  [TestFixture('NATS KeyValue Watcher Gate')]
  TDextNatsKeyValueWatcherGateTests = class
  public
    [Test, Category('Unit'), Category('KeyValue')]
    procedure Snapshot_ShouldDeliverEndMarker;
    [Test, Category('Unit'), Category('KeyValue')]
    procedure IgnoreDeletes_ShouldStillFinishInitialSnapshot;
  end;

implementation

procedure TDextNatsKeyValueWatcherGateTests.Snapshot_ShouldDeliverEndMarker;
var
  Gate: TDextNatsKeyValueWatchGate;
  Entry: TNatsKeyValueEntry;
  Delivered, Markers: Integer;
begin
  Delivered := 0;
  Markers := 0;
  Gate := TDextNatsKeyValueWatchGate.Create(
    procedure(const AEntry: TNatsKeyValueEntry)
    begin
      if AEntry.IsEndOfInitial then Inc(Markers) else Inc(Delivered);
    end, False, False);
  try
    Entry := Default(TNatsKeyValueEntry);
    Entry.Key := 'a';
    Entry.Operation := kvoPut;
    Gate.HandleEntry(Entry, 1);
    Entry.Key := 'b';
    Gate.HandleEntry(Entry, 0);
    Should(Delivered).Be(2);
    Should(Markers).Be(1);
    Should(Gate.InitialDone).Be(True);
  finally
    Gate.Free;
  end;
end;

procedure TDextNatsKeyValueWatcherGateTests.IgnoreDeletes_ShouldStillFinishInitialSnapshot;
var
  Gate: TDextNatsKeyValueWatchGate;
  Entry: TNatsKeyValueEntry;
  Delivered, Markers: Integer;
begin
  Delivered := 0;
  Markers := 0;
  Gate := TDextNatsKeyValueWatchGate.Create(
    procedure(const AEntry: TNatsKeyValueEntry)
    begin
      if AEntry.IsEndOfInitial then Inc(Markers) else Inc(Delivered);
    end, False, True);
  try
    Entry := Default(TNatsKeyValueEntry);
    Entry.Key := 'gone';
    Entry.Operation := kvoDelete;
    Gate.HandleEntry(Entry, 0);
    Should(Delivered).Be(0);
    Should(Markers).Be(1);
  finally
    Gate.Free;
  end;
end;

end.
