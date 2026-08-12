unit Dext.Net.Nats.ObjectStore.Subjects.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.ObjectStore.Subjects;

type
  [TestFixture('NATS ObjectStore Subjects')]
  TDextNatsObjectStoreSubjectsTests = class
  public
    [Test, Category('Unit'), Category('ObjectStore')]
    procedure SubjectAndNameRoundTrip_ShouldMatchAdr20;
  end;

implementation

procedure TDextNatsObjectStoreSubjectsTests.SubjectAndNameRoundTrip_ShouldMatchAdr20;
var
  Encoded: string;
begin
  Should(NatsObjectStreamName('FILES')).Be('OBJ_FILES');
  Encoded := NatsObjectEncodeName('folder/test.pdf');
  Should(NatsObjectDecodeName(Encoded)).Be('folder/test.pdf');
  Should(NatsObjectMetaSubject('FILES', 'folder/test.pdf')).Be(
    '$O.FILES.M.' + Encoded);
  Should(NatsObjectMetaWildcard('FILES')).Be('$O.FILES.M.>');
  Should(NatsObjectChunkSubject('FILES', 'abc')).Be('$O.FILES.C.abc');
end;

end.
