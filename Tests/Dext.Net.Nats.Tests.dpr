program DextNetNatsTests;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Dext.Testing,
  Dext.Testing.Runner,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats.Protocol.Headers in '..\Source\Protocol\Dext.Net.Nats.Protocol.Headers.pas',
  Dext.Net.Nats.Protocol.Control in '..\Source\Protocol\Dext.Net.Nats.Protocol.Control.pas',
  Dext.Net.Nats.Protocol.Writer in '..\Source\Protocol\Dext.Net.Nats.Protocol.Writer.pas',
  Dext.Net.Nats.NKeys in '..\Source\Dext.Net.Nats.NKeys.pas',
  Dext.Net.Nats.ParserRuntime in '..\Source\Dext.Net.Nats.ParserRuntime.pas',
  Dext.Net.Nats in '..\Source\Dext.Net.Nats.pas',
  Dext.Net.Nats.JetStream in '..\Source\Dext.Net.Nats.JetStream.pas',
  Dext.Net.Nats.JetStream.Json in '..\Source\JetStream\Dext.Net.Nats.JetStream.Json.pas',
  Dext.Net.Nats.JetStream.Codecs in '..\Source\JetStream\Dext.Net.Nats.JetStream.Codecs.pas',
  Dext.Net.Nats.JetStream.Parsers in '..\Source\JetStream\Dext.Net.Nats.JetStream.Parsers.pas',
  Dext.Net.Nats.JetStream.Paging in '..\Source\JetStream\Dext.Net.Nats.JetStream.Paging.pas',
  Dext.Net.Nats.JetStream.ObjectPaging in '..\Source\JetStream\Dext.Net.Nats.JetStream.ObjectPaging.pas',
  Dext.Net.Nats.JetStream.Transport in '..\Source\JetStream\Dext.Net.Nats.JetStream.Transport.pas',
  Dext.Net.Nats.JetStream.Streams in '..\Source\JetStream\Dext.Net.Nats.JetStream.Streams.pas',
  Dext.Net.Nats.JetStream.Consumers in '..\Source\JetStream\Dext.Net.Nats.JetStream.Consumers.pas',
  Dext.Net.Nats.JetStream.Fetch in '..\Source\JetStream\Dext.Net.Nats.JetStream.Fetch.pas',
  Dext.Net.Nats.JetStream.Push in '..\Source\JetStream\Dext.Net.Nats.JetStream.Push.pas',
  Dext.Net.Nats.JetStream.Ordered in '..\Source\JetStream\Dext.Net.Nats.JetStream.Ordered.pas',
  Dext.Net.Nats.JetStream.Runtime in '..\Source\JetStream\Dext.Net.Nats.JetStream.Runtime.pas',
  Dext.Net.Nats.KeyValue in '..\Source\Dext.Net.Nats.KeyValue.pas',
  Dext.Net.Nats.KeyValue.Subjects in '..\Source\KeyValue\Dext.Net.Nats.KeyValue.Subjects.pas',
  Dext.Net.Nats.KeyValue.WatcherGate in '..\Source\KeyValue\Dext.Net.Nats.KeyValue.WatcherGate.pas',
  Dext.Net.Nats.ObjectStore in '..\Source\Dext.Net.Nats.ObjectStore.pas',
  Dext.Net.Nats.ObjectStore.Subjects in '..\Source\ObjectStore\Dext.Net.Nats.ObjectStore.Subjects.pas',
  Dext.Net.Nats.ObjectStore.Crypto in '..\Source\ObjectStore\Dext.Net.Nats.ObjectStore.Crypto.pas',
  Dext.Net.Nats.ObjectStore.WatcherGate in '..\Source\ObjectStore\Dext.Net.Nats.ObjectStore.WatcherGate.pas',
  Dext.Net.Nats.ObjectStore.Reader in '..\Source\ObjectStore\Dext.Net.Nats.ObjectStore.Reader.pas',
  Dext.Net.Nats.Services in '..\Source\Dext.Net.Nats.Services.pas',
  Dext.Net.Nats.Services.Subjects in '..\Source\Services\Dext.Net.Nats.Services.Subjects.pas',
  Dext.Net.Nats.Services.Validation in '..\Source\Services\Dext.Net.Nats.Services.Validation.pas',
  Dext.Net.Nats.Services.Routing in '..\Source\Services\Dext.Net.Nats.Services.Routing.pas',
  Dext.Net.Nats.DependencyInjection in '..\Source\Dext.Net.Nats.DependencyInjection.pas',
  Dext.Net.Nats.HealthChecks in '..\Source\Dext.Net.Nats.HealthChecks.pas',
  Dext.Net.Nats.Internal.Buffer in '..\Source\Internal\Dext.Net.Nats.Internal.Buffer.pas',
  Dext.Net.Nats.Internal.Dispatcher in '..\Source\Internal\Dext.Net.Nats.Internal.Dispatcher.pas',
  Dext.Net.Nats.Internal.Parser in '..\Source\Internal\Dext.Net.Nats.Internal.Parser.pas',
  Dext.Net.Nats.Tests in 'Dext.Net.Nats.Tests.pas',
  Dext.Net.Nats.Drain.Tests in 'Core\Dext.Net.Nats.Drain.Tests.pas',
  Dext.Net.Nats.BorrowedPayload.Tests in 'Core\Dext.Net.Nats.BorrowedPayload.Tests.pas',
  Dext.Net.Nats.Internal.Tests in 'Internal\Dext.Net.Nats.Internal.Tests.pas',
  Dext.Net.Nats.ParserV2.Tests in 'Protocol\Dext.Net.Nats.ParserV2.Tests.pas',
  Dext.Net.Nats.ParserRuntime.Tests in 'Protocol\Dext.Net.Nats.ParserRuntime.Tests.pas',
  Dext.Net.Nats.Protocol.V2.Tests in 'Protocol\Dext.Net.Nats.Protocol.V2.Tests.pas',
  Dext.Net.Nats.ParserV2.Benchmarks in 'Benchmarks\Dext.Net.Nats.ParserV2.Benchmarks.pas',
  Dext.Net.Nats.JetStream.Json.Tests in 'JetStream\Dext.Net.Nats.JetStream.Json.Tests.pas',
  Dext.Net.Nats.JetStream.Streams.Tests in 'JetStream\Dext.Net.Nats.JetStream.Streams.Tests.pas',
  Dext.Net.Nats.JetStream.Consumers.Tests in 'JetStream\Dext.Net.Nats.JetStream.Consumers.Tests.pas',
  Dext.Net.Nats.JetStream.ObjectPaging.Tests in 'JetStream\Dext.Net.Nats.JetStream.ObjectPaging.Tests.pas',
  Dext.Net.Nats.JetStream.Fetch.Tests in 'JetStream\Dext.Net.Nats.JetStream.Fetch.Tests.pas',
  Dext.Net.Nats.ObjectStore.Subjects.Tests in 'ObjectStore\Dext.Net.Nats.ObjectStore.Subjects.Tests.pas',
  Dext.Net.Nats.ObjectStore.WatcherGate.Tests in 'ObjectStore\Dext.Net.Nats.ObjectStore.WatcherGate.Tests.pas',
  Dext.Net.Nats.KeyValue.Subjects.Tests in 'KeyValue\Dext.Net.Nats.KeyValue.Subjects.Tests.pas',
  Dext.Net.Nats.KeyValue.WatcherGate.Tests in 'KeyValue\Dext.Net.Nats.KeyValue.WatcherGate.Tests.pas',
  Dext.Net.Nats.Services.Subjects.Tests in 'Services\Dext.Net.Nats.Services.Subjects.Tests.pas',
  Dext.Net.Nats.Services.Validation.Tests in 'Services\Dext.Net.Nats.Services.Validation.Tests.pas',
  Dext.Net.Nats.Services.Routing.Tests in 'Services\Dext.Net.Nats.Services.Routing.Tests.pas';

var
  Config: TTestConfigurator;
  RunStress: Boolean;
  RunBench: Boolean;
begin
  SetConsoleCharSet;
  try
    SafeWriteLn;
    SafeWriteLn('Dext.Net.Nats Tests');
    SafeWriteLn('===================');
    RunStress := SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_STRESS')), '1')
      or SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_STRESS')), 'true');
    RunBench := SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_BENCH')), '1')
      or SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_BENCH')), 'true');

    Config := ConfigureTests.Verbose;
    if RunStress or RunBench then Config := Config.IncludeExplicitTests;

    RunTests(Config.RegisterFixtures([
      TDextNatsProtocolTests,
      TDextNatsIntegrationTests,
      TDextNatsDrainTests,
      TDextNatsBorrowedPayloadTests,
      TDextNatsJetStreamTests,
      TDextNatsKeyValueTests,
      TDextNatsObjectStoreTests,
      TDextNatsServicesTests,
      TDextNatsTlsIntegrationTests,
      TDextNatsNKeyIntegrationTests,
      TDextNatsStressTests,
      TDextNatsBenchmarkTests,
      TDextNatsDiTests,
      TDextNatsObservabilityTests,
      TDextNatsInternalTests,
      TDextNatsParserV2Tests,
      TDextNatsParserRuntimeTests,
      TDextNatsProtocolV2Tests,
      TDextNatsParserV2BenchmarkTests,
      TDextNatsJetStreamJsonTests,
      TDextNatsJetStreamStreamsTests,
      TDextNatsJetStreamConsumersTests,
      TDextNatsJetStreamObjectPagingTests,
      TDextNatsJetStreamFetchTests,
      TDextNatsObjectStoreSubjectsTests,
      TDextNatsObjectStoreWatcherGateTests,
      TDextNatsKeyValueSubjectsTests,
      TDextNatsKeyValueWatcherGateTests,
      TDextNatsServicesSubjectsTests,
      TDextNatsServicesValidationTests,
      TDextNatsServicesRoutingTests
    ]));
  except
    on E: Exception do
    begin
      SafeWriteLn('FATAL ERROR: ' + E.ClassName + ': ' + E.Message);
      ExitCode := 1;
    end;
  end;
end.
