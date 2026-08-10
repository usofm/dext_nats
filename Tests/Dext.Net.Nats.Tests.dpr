{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                     }
{                                                                           }
{           A native NATS client library for the Dext Framework            }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License");}
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
  Dext.Net.Nats in '..\Source\Dext.Net.Nats.pas',
  Dext.Net.Nats.JetStream in '..\Source\Dext.Net.Nats.JetStream.pas',
  Dext.Net.Nats.Tests in 'Dext.Net.Nats.Tests.pas';

begin
  SetConsoleCharSet;
  try
    SafeWriteLn;
    SafeWriteLn('Dext.Net.Nats Tests');
    SafeWriteLn('===================');
    SafeWriteLn;

    RunTests(ConfigureTests
      .Verbose
      .RegisterFixtures([
        TDextNatsProtocolTests,
        TDextNatsIntegrationTests,
        TDextNatsJetStreamTests,
        TDextNatsTlsIntegrationTests
      ]));
  except
    on E: Exception do
    begin
      SafeWriteLn('FATAL ERROR: ' + E.ClassName + ': ' + E.Message);
      ExitCode := 1;
    end;
  end;
end.
