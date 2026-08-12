unit Dext.Net.Nats.KeyValue.Subjects.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.KeyValue.Subjects;

type
  [TestFixture('NATS KeyValue Subjects')]
  TDextNatsKeyValueSubjectsTests = class
  public
    [Test, Category('Unit'), Category('KeyValue')]
    procedure SubjectMapping_ShouldRoundTrip;

    [Test, Category('Unit'), Category('KeyValue')]
    procedure Validation_ShouldAcceptWildcardsOnlyInSearch;
  end;

implementation

procedure TDextNatsKeyValueSubjectsTests.SubjectMapping_ShouldRoundTrip;
begin
  Should(NatsKvStreamName('CACHE')).Be('KV_CACHE');
  Should(NatsKvSubjectForKey('CACHE', 'user.42')).Be('$KV.CACHE.user.42');
  Should(NatsKvKeyFromSubject('CACHE', '$KV.CACHE.user.42')).Be('user.42');
end;

procedure TDextNatsKeyValueSubjectsTests.Validation_ShouldAcceptWildcardsOnlyInSearch;
begin
  NatsKvValidateBucket('CACHE_1');
  NatsKvValidateKey('customer/42=name');
  NatsKvValidateSearchKey('customer.*');
  NatsKvValidateSearchKey('customer.>');
end;

end.
