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
{  Lightweight health probe for TDextNatsClient. Stays free of Dext.Web;    }
{  map TNatsHealthResult onto Dext.HealthChecks.IHealthCheck in web apps    }
{  (see README). Default is Connected-only; opt in to a short Flush         }
{  (PING/PONG) for deeper liveness — see TNatsHealthCheckOptions.           }
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

const
  /// <summary>
  ///   Suggested Flush timeout (ms) for deep health probes (HLTH P2b). Keeps
  ///   health endpoints from blocking on <c>Options.RequestTimeoutMs</c>.
  /// </summary>
  NATS_HEALTH_FLUSH_TIMEOUT_MS = 500;

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
  ///   Options for <see cref="TNatsHealthCheck"/>. Default is Connected-only
  ///   (<c>FlushTimeoutMs = 0</c>) so health endpoints stay cheap.
  /// </summary>
  TNatsHealthCheckOptions = record
    /// <summary>
    ///   When <c>&gt; 0</c>, <see cref="TNatsHealthCheck.CheckHealth"/> also
    ///   calls <see cref="TDextNatsClient.Flush"/> with this timeout (ms).
    ///   Healthy means connected and a PING/PONG round-trip succeeded within
    ///   the timeout. Timeout or Flush error → Unhealthy (never raises to the
    ///   caller). When <c>&lt;= 0</c> (default), only <c>Connected</c> is
    ///   checked — no RTT and no risk of blocking beyond a flag read.
    ///   Prefer a short budget (e.g. <see cref="NATS_HEALTH_FLUSH_TIMEOUT_MS"/>);
    ///   do not pass 0 intending "use RequestTimeoutMs" — that opt-in is
    ///   intentionally disabled here to avoid hanging scrapers.
    /// </summary>
    FlushTimeoutMs: Integer;
    /// <summary>Connected-only probe (<c>FlushTimeoutMs = 0</c>).</summary>
    class function CreateDefault: TNatsHealthCheckOptions; static;
    /// <summary>
    ///   Deep probe with a short Flush. <c>ATimeoutMs &lt;= 0</c> uses
    ///   <see cref="NATS_HEALTH_FLUSH_TIMEOUT_MS"/>.
    /// </summary>
    class function CreateWithFlush(ATimeoutMs: Integer = NATS_HEALTH_FLUSH_TIMEOUT_MS): TNatsHealthCheckOptions; static;
  end;

  /// <summary>
  ///   Reports Healthy when the injected <see cref="TDextNatsClient"/> is connected;
  ///   optionally also when a short <see cref="TDextNatsClient.Flush"/> succeeds
  ///   (<see cref="TNatsHealthCheckOptions.FlushTimeoutMs"/>). Register with
  ///   <see cref="AddNatsHealthCheck"/> after AddNatsClient.
  /// </summary>
  TNatsHealthCheck = class
  private
    FClient: TDextNatsClient;
    FOptions: TNatsHealthCheckOptions;
  public
    constructor Create(AClient: TDextNatsClient); overload;
    constructor Create(AClient: TDextNatsClient; const AOptions: TNatsHealthCheckOptions); overload;
    /// <summary>
    ///   Connected-only, or Connected + Flush when <c>Options.FlushTimeoutMs &gt; 0</c>.
    ///   Exceptions from Flush (including <see cref="EDextNatsTimeoutError"/>) are
    ///   converted to Unhealthy; the probe does not re-raise. Worst-case wait is
    ///   bounded by <c>FlushTimeoutMs</c> when deep probe is enabled.
    /// </summary>
    function CheckHealth: TNatsHealthResult;
    property Options: TNatsHealthCheckOptions read FOptions;
  end;

/// <summary>
///   Registers <see cref="TNatsHealthCheck"/> as transient with default
///   Connected-only options (factory resolves the singleton client).
///   Call after <c>AddNatsClient</c>. Transient instances are not freed by the
///   container — Free them after probing, or resolve once and hold.
/// </summary>
procedure AddNatsHealthCheck(const AServices: IServiceCollection); overload;

/// <summary>
///   Like the parameterless overload, but applies <c>AOptions</c> (e.g.
///   <see cref="TNatsHealthCheckOptions.CreateWithFlush"/> for a deep probe).
/// </summary>
procedure AddNatsHealthCheck(const AServices: IServiceCollection;
  const AOptions: TNatsHealthCheckOptions); overload;

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

{ TNatsHealthCheckOptions }

class function TNatsHealthCheckOptions.CreateDefault: TNatsHealthCheckOptions;
begin
  Result.FlushTimeoutMs := 0;
end;

class function TNatsHealthCheckOptions.CreateWithFlush(ATimeoutMs: Integer): TNatsHealthCheckOptions;
begin
  if ATimeoutMs <= 0 then
    Result.FlushTimeoutMs := NATS_HEALTH_FLUSH_TIMEOUT_MS
  else
    Result.FlushTimeoutMs := ATimeoutMs;
end;

{ TNatsHealthCheck }

constructor TNatsHealthCheck.Create(AClient: TDextNatsClient);
begin
  Create(AClient, TNatsHealthCheckOptions.CreateDefault);
end;

constructor TNatsHealthCheck.Create(AClient: TDextNatsClient;
  const AOptions: TNatsHealthCheckOptions);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('TNatsHealthCheck requires a TDextNatsClient');
  FClient := AClient;
  FOptions := AOptions;
end;

function TNatsHealthCheck.CheckHealth: TNatsHealthResult;
var
  flushMs: Integer;
begin
  if not FClient.Connected then
    Exit(TNatsHealthResult.Unhealthy('NATS disconnected'));

  flushMs := FOptions.FlushTimeoutMs;
  if flushMs <= 0 then
    Exit(TNatsHealthResult.Healthy(
      Format('NATS connected to %s:%d', [FClient.Host, FClient.Port])));

  try
    FClient.Flush(flushMs);
    Result := TNatsHealthResult.Healthy(
      Format('NATS connected and responsive (%s:%d, flush within %d ms)',
        [FClient.Host, FClient.Port, flushMs]));
  except
    on E: EDextNatsTimeoutError do
      Result := TNatsHealthResult.Unhealthy(
        Format('NATS flush timed out after %d ms', [flushMs]));
    on E: Exception do
      Result := TNatsHealthResult.Unhealthy(
        Format('NATS flush failed: %s', [E.Message]));
  end;
end;

procedure AddNatsHealthCheck(const AServices: IServiceCollection);
begin
  AddNatsHealthCheck(AServices, TNatsHealthCheckOptions.CreateDefault);
end;

procedure AddNatsHealthCheck(const AServices: IServiceCollection;
  const AOptions: TNatsHealthCheckOptions);
var
  Opts: TNatsHealthCheckOptions;
  Factory: TFunc<IServiceProvider, TObject>;
begin
  Opts := AOptions;
  Factory := function(C: IServiceProvider): TObject
    var
      Client: TDextNatsClient;
    begin
      Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(C);
      Result := TNatsHealthCheck.Create(Client, Opts);
    end;
  AServices.AddTransient(TServiceType.FromClass(TNatsHealthCheck), TNatsHealthCheck, Factory);
end;

end.
