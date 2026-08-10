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
{                                                                           }
{***************************************************************************}
{                                                                           }
{  Lightweight Connected probe for TDextNatsClient. Stays free of           }
{  Dext.Web; map TNatsHealthResult onto Dext.HealthChecks.IHealthCheck in   }
{  web apps (see README).                                                   }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.HealthChecks;

interface

uses
  System.SysUtils,
  System.Classes,
  Dext.DI.Interfaces,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats;

type
  TNatsHealthStatus = (nhsHealthy, nhsUnhealthy);

  /// <summary>Result of a NATS liveness probe (mirrors Dext.HealthChecks shape without Web deps).</summary>
  TNatsHealthResult = record
    Status: TNatsHealthStatus;
    Description: string;
    class function Healthy(const ADescription: string = ''): TNatsHealthResult; static;
    class function Unhealthy(const ADescription: string = ''): TNatsHealthResult; static;
  end;

  /// <summary>
  ///   Reports Healthy when the injected <see cref="TDextNatsClient"/> is connected;
  ///   Unhealthy otherwise. Register with <see cref="AddNatsHealthCheck"/> after AddNatsClient.
  /// </summary>
  TNatsHealthCheck = class
  private
    FClient: TDextNatsClient;
  public
    constructor Create(AClient: TDextNatsClient);
    function CheckHealth: TNatsHealthResult;
  end;

/// <summary>
///   Registers <see cref="TNatsHealthCheck"/> as transient (factory resolves the singleton client).
///   Call after <c>AddNatsClient</c>. Transient instances are not freed by the container — Free them
///   after probing, or resolve once and hold.
/// </summary>
procedure AddNatsHealthCheck(const AServices: IServiceCollection);

implementation

{ TNatsHealthResult }

class function TNatsHealthResult.Healthy(const ADescription: string): TNatsHealthResult;
begin
  Result.Status := nhsHealthy;
  Result.Description := ADescription;
end;

class function TNatsHealthResult.Unhealthy(const ADescription: string): TNatsHealthResult;
begin
  Result.Status := nhsUnhealthy;
  Result.Description := ADescription;
end;

{ TNatsHealthCheck }

constructor TNatsHealthCheck.Create(AClient: TDextNatsClient);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('TNatsHealthCheck requires a TDextNatsClient');
  FClient := AClient;
end;

function TNatsHealthCheck.CheckHealth: TNatsHealthResult;
begin
  if FClient.Connected then
    Result := TNatsHealthResult.Healthy(
      Format('NATS connected to %s:%d', [FClient.Host, FClient.Port]))
  else
    Result := TNatsHealthResult.Unhealthy('NATS disconnected');
end;

procedure AddNatsHealthCheck(const AServices: IServiceCollection);
var
  Factory: TFunc<IServiceProvider, TObject>;
begin
  Factory := function(C: IServiceProvider): TObject
    var
      Client: TDextNatsClient;
    begin
      Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(C);
      Result := TNatsHealthCheck.Create(Client);
    end;
  AServices.AddTransient(TServiceType.FromClass(TNatsHealthCheck), TNatsHealthCheck, Factory);
end;

end.
