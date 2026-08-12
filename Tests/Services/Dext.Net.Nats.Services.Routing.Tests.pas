unit Dext.Net.Nats.Services.Routing.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Services.Routing;

type
  [TestFixture('NATS Services Routing')]
  TDextNatsServicesRoutingTests = class
  public
    [Test, Category('Unit'), Category('Services')]
    procedure QueueResolution_ShouldHonorDisableOverrideAndInheritance;
    [Test, Category('Unit'), Category('Services')]
    procedure SubjectResolution_ShouldStackGroups;
  end;

implementation

procedure TDextNatsServicesRoutingTests.QueueResolution_ShouldHonorDisableOverrideAndInheritance;
var
  Q: string;
  Disabled: Boolean;
begin
  NatsServiceResolveQueue('', False, 'q', False, Q, Disabled);
  Should(Q).Be('q');
  Should(Disabled).Be(False);
  NatsServiceResolveQueue('fast', False, 'q', False, Q, Disabled);
  Should(Q).Be('fast');
  NatsServiceResolveQueue('', True, 'q', False, Q, Disabled);
  Should(Q).Be('');
  Should(Disabled).Be(True);
end;

procedure TDextNatsServicesRoutingTests.SubjectResolution_ShouldStackGroups;
begin
  Should(NatsServiceResolveNestedPrefix('orders', 'v2')).Be('orders.v2');
  Should(NatsServiceResolveEndpointSubject('orders.v2', '', 'create')).Be(
    'orders.v2.create');
  Should(NatsServiceResolveEndpointSubject('', 'custom.subject', 'ignored')).Be(
    'custom.subject');
end;

end.
