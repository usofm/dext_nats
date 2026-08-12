{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                     }
{                                                                           }
{           A native NATS client library for the Dext Framework            }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License"); }
{           you may not use this file except in compliance with the License.}
{           You may obtain a copy of the License at                         }
{                                                                           }
{               http://www.apache.org/licenses/LICENSE-2.0                  }
{                                                                           }
{           Unless required by applicable law or agreed to in writing,      }
{           software distributed under the License is distributed on an     }
{           "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,    }
{           either express or implied. See the License for the specific     }
{           language governing permissions and limitations under the        }
{           License.                                                        }
{                                                                           }
{***************************************************************************}
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
  Dext.Net.Nats.NKeys in '..\Source\Dext.Net.Nats.NKeys.pas',
  Dext.Net.Nats in '..\Source\Dext.Net.Nats.pas',
  Dext.Net.Nats.Dispatching in '..\Source\Dext.Net.Nats.Dispatching.pas',
  Dext.Net.Nats.JetStream in '..\Source\Dext.Net.Nats.JetStream.pas',
  Dext.Net.Nats.JetStream.Json in '..\Source\JetStream\Dext.Net.Nats.JetStream.Json.pas',
  Dext.Net.Nats.KeyValue in '..\Source\Dext.Net.Nats.KeyValue.pas',
  Dext.Net.Nats.ObjectStore in '..\Source\Dext.Net.Nats.ObjectStore.pas',
  Dext.Net.Nats.Services in '..\Source\Dext.Net.Nats.Services.pas',
  Dext.Net.Nats.DependencyInjection in '..\Source\Dext.Net.Nats.DependencyInjection.pas',
  Dext.Net.Nats.HealthChecks in '..\Source\Dext.Net.Nats.HealthChecks.pas',
  Dext.Net.Nats.Internal.Buffer in '..\Source\Internal\Dext.Net.Nats.Internal.Buffer.pas',
  Dext.Net.Nats.Internal.Dispatcher in '..\Source\Internal\Dext.Net.Nats.Internal.Dispatcher.pas',
  Dext.Net.Nats.Internal.Parser in '..\Source\Internal\Dext.Net.Nats.Internal.Parser.pas',
  Dext.Net.Nats.Tests in 'Dext.Net.Nats.Tests.pas',
  Dext.Net.Nats.Drain.Tests in 'Core\Dext.Net.Nats.Drain.Tests.pas',
  Dext.Net.Nats.Dispatching.Tests in 'Core\Dext.Net.Nats.Dispatching.Tests.pas',
  Dext.Net.Nats.Internal.Tests in 'Internal\Dext.Net.Nats.Internal.Tests.pas',
  Dext.Net.Nats.ParserV2.Tests in 'Protocol\Dext.Net.Nats.ParserV2.Tests.pas',
  Dext.Net.Nats.ParserV2.Benchmarks in 'Benchmarks\Dext.Net.Nats.ParserV2.Benchmarks.pas',
  Dext.Net.Nats.JetStream.Json.Tests in 'JetStream\Dext.Net.Nats.JetStream.Json.Tests.pas';

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
    if Trim(GetEnvironmentVariable('DEXT_NATS_TLS_PORT')) = '' then
      SafeWriteLn('TLS: soft-skip (set DEXT_NATS_TLS_PORT to enable live TLS tests)');
    if Trim(GetEnvironmentVariable('DEXT_NATS_NKEY_PORT')) = '' then
      SafeWriteLn('NKey: soft-skip (set DEXT_NATS_NKEY_PORT + SEED to enable live NKey tests)');
    RunStress := SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_STRESS')), '1')
      or SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_STRESS')), 'true');
    RunBench := SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_BENCH')), '1')
      or SameText(Trim(GetEnvironmentVariable('DEXT_NATS_RUN_BENCH')), 'true');
    if not RunStress then
      SafeWriteLn('Stress: explicit tests omitted (set DEXT_NATS_RUN_STRESS=1 to include)');
    if not RunBench then
      SafeWriteLn('Bench: explicit tests omitted (set DEXT_NATS_RUN_BENCH=1 to include)');
    SafeWriteLn;

    Config := ConfigureTests.Verbose;
    if RunStress or RunBench then
      Config := Config.IncludeExplicitTests;

    RunTests(Config.RegisterFixtures([
      TDextNatsProtocolTests,
      TDextNatsIntegrationTests,
      TDextNatsDrainTests,
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
      TDextNatsDispatchingTests,
      TDextNatsParserV2Tests,
      TDextNatsParserV2BenchmarkTests,
      TDextNatsJetStreamJsonTests
    ]));
  except
    on E: Exception do
    begin
      SafeWriteLn('FATAL ERROR: ' + E.ClassName + ': ' + E.Message);
      ExitCode := 1;
    end;
  end;
end.
