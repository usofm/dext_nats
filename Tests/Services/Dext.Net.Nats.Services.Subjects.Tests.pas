unit Dext.Net.Nats.Services.Subjects.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Services,
  Dext.Net.Nats.Services.Subjects;

type
  [TestFixture('NATS Services Subjects')]
  TDextNatsServicesSubjectsTests = class
  public
    [Test, Category('Unit'), Category('Services')]
    procedure DiscoverySubjects_ShouldMatchAdr32;
  end;

implementation

procedure TDextNatsServicesSubjectsTests.DiscoverySubjects_ShouldMatchAdr32;
begin
  Should(NatsServiceDiscoverySubject(svPing)).Be('$SRV.PING');
  Should(NatsServiceDiscoverySubject(svInfo, 'billing')).Be('$SRV.INFO.billing');
  Should(NatsServiceDiscoverySubject(svStats, 'billing', 'abc')).Be(
    '$SRV.STATS.billing.abc');
  Should(NatsServiceJoinSubject('orders', 'create')).Be('orders.create');
  Should(NatsServiceIsValidName('billing-v2')).Be(True);
  Should(NatsServiceIsValidName('bad.name')).Be(False);
end;

end.
