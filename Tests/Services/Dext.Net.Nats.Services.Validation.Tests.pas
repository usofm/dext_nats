unit Dext.Net.Nats.Services.Validation.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Services.Validation;

type
  [TestFixture('NATS Services Validation')]
  TDextNatsServicesValidationTests = class
  public
    [Test, Category('Unit'), Category('Services')]
    procedure Validation_ShouldMatchAdr32Contracts;
  end;

implementation

procedure TDextNatsServicesValidationTests.Validation_ShouldMatchAdr32Contracts;
begin
  Should(NatsServiceValidateName('billing_v2')).Be(True);
  Should(NatsServiceValidateName('billing.v2')).Be(False);
  Should(NatsServiceValidateSemVer('1.2.3')).Be(True);
  Should(NatsServiceValidateSemVer('1.2')).Be(False);
  Should(NatsServiceValidateSubject('orders.create')).Be(True);
  Should(NatsServiceValidateSubject('orders.>')).Be(True);
  Should(NatsServiceValidateSubject('orders.>.bad')).Be(False);
end;

end.
