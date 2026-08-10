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
{  Dext.DI registration helpers for TDextNatsClient (singleton) and         }
{  TDextNatsJetStreamContext (transient, does not own the client).          }
{  Factories do not connect unless AddNatsClientAndConnect is used.         }
{  Optional ILogger is resolved from ILoggerFactory when present.           }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.DependencyInjection;

interface

uses
  System.SysUtils,
  Dext.DI.Interfaces,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

type
  /// <summary>Callback used to mutate options before the singleton client is created.</summary>
  TConfigureNatsOptions = reference to procedure(var AOptions: TDextNatsOptions);

/// <summary>
///   Registers <see cref="TDextNatsClient"/> as a singleton with default options
///   (localhost:4222). Does not call Connect — do that from hosted startup or app code.
/// </summary>
procedure AddNatsClient(const AServices: IServiceCollection); overload;

/// <summary>
///   Registers <see cref="TDextNatsClient"/> as a singleton using the given options.
///   Does not call Connect.
/// </summary>
procedure AddNatsClient(const AServices: IServiceCollection; const AOptions: TDextNatsOptions); overload;

/// <summary>
///   Registers <see cref="TDextNatsClient"/> as a singleton with default options and Host/Port set.
///   Does not call Connect.
/// </summary>
procedure AddNatsClient(const AServices: IServiceCollection; const AHost: string;
  APort: Word = NATS_DEFAULT_PORT); overload;

/// <summary>
///   Registers <see cref="TDextNatsClient"/> as a singleton after running <c>AConfigure</c>
///   on a default options record (set Host/Port/TLS/auth/etc.). Does not call Connect.
/// </summary>
procedure AddNatsClient(const AServices: IServiceCollection;
  const AConfigure: TConfigureNatsOptions); overload;

/// <summary>
///   Like <see cref="AddNatsClient"/> with Host/Port, then connects inside the factory
///   on first resolve (singleton). Prefer explicit Connect in apps that need error handling
///   before the container builds.
/// </summary>
procedure AddNatsClientAndConnect(const AServices: IServiceCollection; const AHost: string;
  APort: Word = NATS_DEFAULT_PORT); overload;

/// <summary>
///   Registers a singleton client from options and connects to <c>AOptions.Host</c>:<c>AOptions.Port</c>
///   on first resolve.
/// </summary>
procedure AddNatsClientAndConnect(const AServices: IServiceCollection;
  const AOptions: TDextNatsOptions); overload;

/// <summary>
///   Registers <see cref="TDextNatsJetStreamContext"/> as transient. Resolves the shared
///   <see cref="TDextNatsClient"/> singleton and does not own/free it.
///   Call after <c>AddNatsClient</c>. Caller must Free transient contexts.
/// </summary>
procedure AddNatsJetStream(const AServices: IServiceCollection;
  const AApiPrefix: string = '$JS.API.');

/// <summary>Redis-style alias for <see cref="AddNatsClient"/> with Host/Port.</summary>
procedure RegisterNatsClient(const AServices: IServiceCollection; const AHost: string = 'localhost';
  APort: Word = NATS_DEFAULT_PORT);

implementation

uses
  System.TypInfo,
  Dext.Logging;

function TryAttachLogger(AProvider: IServiceProvider; AClient: TDextNatsClient): Boolean;
var
  FactoryObj: IInterface;
  Factory: ILoggerFactory;
begin
  Result := False;
  if (AProvider = nil) or (AClient = nil) then
    Exit;
  FactoryObj := AProvider.GetServiceAsInterface(TServiceType.FromInterface(TypeInfo(ILoggerFactory)));
  if FactoryObj = nil then
    Exit;
  if not Supports(FactoryObj, ILoggerFactory, Factory) then
    Exit;
  AClient.Logger := Factory.CreateLogger('Dext.Net.Nats');
  Result := Assigned(AClient.Logger);
end;

procedure AddNatsClient(const AServices: IServiceCollection);
begin
  AddNatsClient(AServices, TDextNatsOptions.CreateDefault);
end;

procedure AddNatsClient(const AServices: IServiceCollection; const AOptions: TDextNatsOptions);
var
  Opts: TDextNatsOptions;
  Factory: TFunc<IServiceProvider, TObject>;
begin
  Opts := AOptions;
  Factory := function(C: IServiceProvider): TObject
    var
      Client: TDextNatsClient;
    begin
      Client := TDextNatsClient.Create(Opts);
      TryAttachLogger(C, Client);
      Result := Client;
    end;
  AServices.AddSingleton(TServiceType.FromClass(TDextNatsClient), TDextNatsClient, Factory);
end;

procedure AddNatsClient(const AServices: IServiceCollection; const AHost: string; APort: Word);
var
  Opts: TDextNatsOptions;
begin
  Opts := TDextNatsOptions.CreateDefault;
  Opts.Host := AHost;
  Opts.Port := APort;
  AddNatsClient(AServices, Opts);
end;

procedure AddNatsClient(const AServices: IServiceCollection; const AConfigure: TConfigureNatsOptions);
var
  Opts: TDextNatsOptions;
begin
  Opts := TDextNatsOptions.CreateDefault;
  if Assigned(AConfigure) then
    AConfigure(Opts);
  AddNatsClient(AServices, Opts);
end;

procedure AddNatsClientAndConnect(const AServices: IServiceCollection; const AHost: string;
  APort: Word);
var
  Opts: TDextNatsOptions;
begin
  Opts := TDextNatsOptions.CreateDefault;
  Opts.Host := AHost;
  Opts.Port := APort;
  AddNatsClientAndConnect(AServices, Opts);
end;

procedure AddNatsClientAndConnect(const AServices: IServiceCollection;
  const AOptions: TDextNatsOptions);
var
  Opts: TDextNatsOptions;
  Host: string;
  Port: Word;
  Factory: TFunc<IServiceProvider, TObject>;
begin
  Opts := AOptions;
  Host := Opts.Host;
  if Host = '' then
    Host := 'localhost';
  Port := Opts.Port;
  if Port = 0 then
    Port := NATS_DEFAULT_PORT;

  Factory := function(C: IServiceProvider): TObject
    var
      Client: TDextNatsClient;
    begin
      Client := TDextNatsClient.Create(Opts);
      TryAttachLogger(C, Client);
      try
        Client.Connect(Host, Port);
      except
        Client.Free;
        raise;
      end;
      Result := Client;
    end;
  AServices.AddSingleton(TServiceType.FromClass(TDextNatsClient), TDextNatsClient, Factory);
end;

procedure AddNatsJetStream(const AServices: IServiceCollection; const AApiPrefix: string);
var
  Prefix: string;
  Factory: TFunc<IServiceProvider, TObject>;
begin
  Prefix := AApiPrefix;
  Factory := function(C: IServiceProvider): TObject
    var
      Client: TDextNatsClient;
    begin
      Client := TDextServices.GetRequiredServiceObject<TDextNatsClient>(C);
      Result := TDextNatsJetStreamContext.Create(Client, Prefix);
    end;
  AServices.AddTransient(TServiceType.FromClass(TDextNatsJetStreamContext),
    TDextNatsJetStreamContext, Factory);
end;

procedure RegisterNatsClient(const AServices: IServiceCollection; const AHost: string; APort: Word);
begin
  AddNatsClient(AServices, AHost, APort);
end;

end.
