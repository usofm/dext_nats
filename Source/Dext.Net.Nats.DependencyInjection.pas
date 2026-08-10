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
{  SPEC-DI-02: bind section "Nats" via TDextNatsClientSettings.             }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.DependencyInjection;

interface

uses
  System.SysUtils,
  System.Classes,
  Dext.DI.Interfaces,
  Dext.Configuration.Interfaces,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

type
  /// <summary>Callback used to mutate options before the singleton client is created.</summary>
  TConfigureNatsOptions = reference to procedure(var AOptions: TDextNatsOptions);

  /// <summary>
  ///   Bindable TLS subset for configuration section <c>Nats:TLS</c>
  ///   (Dext.Options binder requires a class, not <see cref="TDextTLSOptions"/>).
  /// </summary>
  TDextNatsTlsSettings = class
  private
    FEnabled: Boolean;
    FVerifyServerCertificate: Boolean;
  public
    constructor Create;
  published
    property Enabled: Boolean read FEnabled write FEnabled;
    property VerifyServerCertificate: Boolean read FVerifyServerCertificate write FVerifyServerCertificate;
  end;

  /// <summary>
  ///   Bindable NATS client settings for section <c>Nats</c> (or a custom section name).
  ///   Use <see cref="ToOptions"/> / <see cref="BindNatsOptions"/> to obtain <see cref="TDextNatsOptions"/>.
  /// </summary>
  TDextNatsClientSettings = class
  private
    FHost: string;
    FPort: Integer;
    FName: string;
    FUser: string;
    FPassword: string;
    FAuthToken: string;
    FJWT: string;
    FNKeySeed: string;
    FCredentialsFile: string;
    FVerbose: Boolean;
    FPedantic: Boolean;
    FEcho: Boolean;
    FConnectTimeoutMs: Integer;
    FRequestTimeoutMs: Integer;
    FPingIntervalMs: Integer;
    FMaxPingsOutstanding: Integer;
    FAllowReconnect: Boolean;
    FMaxReconnectAttempts: Integer;
    FReconnectWaitMs: Integer;
    FMaxPendingBufferBytes: Int64;
    FEnableMetrics: Boolean;
    FTLS: TDextNatsTlsSettings;
    procedure SetTLS(AValue: TDextNatsTlsSettings);
  public
    constructor Create;
    destructor Destroy; override;
    /// <summary>Maps published fields onto a <see cref="TDextNatsOptions"/> record (with CreateDefault baseline).</summary>
    function ToOptions: TDextNatsOptions;
  published
    property Host: string read FHost write FHost;
    property Port: Integer read FPort write FPort;
    property Name: string read FName write FName;
    property User: string read FUser write FUser;
    property Password: string read FPassword write FPassword;
    property AuthToken: string read FAuthToken write FAuthToken;
    property JWT: string read FJWT write FJWT;
    property NKeySeed: string read FNKeySeed write FNKeySeed;
    property CredentialsFile: string read FCredentialsFile write FCredentialsFile;
    property Verbose: Boolean read FVerbose write FVerbose;
    property Pedantic: Boolean read FPedantic write FPedantic;
    property Echo: Boolean read FEcho write FEcho;
    property ConnectTimeoutMs: Integer read FConnectTimeoutMs write FConnectTimeoutMs;
    property RequestTimeoutMs: Integer read FRequestTimeoutMs write FRequestTimeoutMs;
    property PingIntervalMs: Integer read FPingIntervalMs write FPingIntervalMs;
    property MaxPingsOutstanding: Integer read FMaxPingsOutstanding write FMaxPingsOutstanding;
    property AllowReconnect: Boolean read FAllowReconnect write FAllowReconnect;
    property MaxReconnectAttempts: Integer read FMaxReconnectAttempts write FMaxReconnectAttempts;
    property ReconnectWaitMs: Integer read FReconnectWaitMs write FReconnectWaitMs;
    property MaxPendingBufferBytes: Int64 read FMaxPendingBufferBytes write FMaxPendingBufferBytes;
    property EnableMetrics: Boolean read FEnableMetrics write FEnableMetrics;
    property TLS: TDextNatsTlsSettings read FTLS write SetTLS;
  end;

/// <summary>
///   Binds configuration section <c>ASectionName</c> (default <c>Nats</c>) to options.
///   Missing keys keep <see cref="TDextNatsOptions.CreateDefault"/> values.
/// </summary>
function BindNatsOptions(const AConfiguration: IConfiguration;
  const ASectionName: string = 'Nats'): TDextNatsOptions;

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
///   Registers a singleton client by binding section <c>ASectionName</c> from <c>AConfiguration</c>
///   (SPEC-DI-02). Does not call Connect.
/// </summary>
procedure AddNatsClient(const AServices: IServiceCollection; const AConfiguration: IConfiguration;
  const ASectionName: string = 'Nats'); overload;

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
  Dext.Logging,
  Dext.Configuration.Binder,
  Dext.Net.Security;

{ TDextNatsTlsSettings }

constructor TDextNatsTlsSettings.Create;
begin
  inherited Create;
  FEnabled := False;
  FVerifyServerCertificate := True;
end;

{ TDextNatsClientSettings }

constructor TDextNatsClientSettings.Create;
var
  Def: TDextNatsOptions;
begin
  inherited Create;
  Def := TDextNatsOptions.CreateDefault;
  FHost := Def.Host;
  FPort := Def.Port;
  FName := Def.Name;
  FUser := Def.User;
  FPassword := Def.Password;
  FAuthToken := Def.AuthToken;
  FJWT := Def.JWT;
  FNKeySeed := Def.NKeySeed;
  FCredentialsFile := Def.CredentialsFile;
  FVerbose := Def.Verbose;
  FPedantic := Def.Pedantic;
  FEcho := Def.Echo;
  FConnectTimeoutMs := Def.ConnectTimeoutMs;
  FRequestTimeoutMs := Def.RequestTimeoutMs;
  FPingIntervalMs := Def.PingIntervalMs;
  FMaxPingsOutstanding := Def.MaxPingsOutstanding;
  FAllowReconnect := Def.AllowReconnect;
  FMaxReconnectAttempts := Def.MaxReconnectAttempts;
  FReconnectWaitMs := Def.ReconnectWaitMs;
  FMaxPendingBufferBytes := Def.MaxPendingBufferBytes;
  FEnableMetrics := Def.EnableMetrics;
  FTLS := TDextNatsTlsSettings.Create;
  FTLS.Enabled := Def.TLS.Enabled;
  FTLS.VerifyServerCertificate := Def.TLS.VerifyServerCertificate;
end;

destructor TDextNatsClientSettings.Destroy;
begin
  FTLS.Free;
  inherited;
end;

procedure TDextNatsClientSettings.SetTLS(AValue: TDextNatsTlsSettings);
begin
  if FTLS = AValue then
    Exit;
  FTLS.Free;
  FTLS := AValue;
  if FTLS = nil then
    FTLS := TDextNatsTlsSettings.Create;
end;

function TDextNatsClientSettings.ToOptions: TDextNatsOptions;
begin
  Result := TDextNatsOptions.CreateDefault;
  if FHost <> '' then
    Result.Host := FHost;
  if FPort > 0 then
    Result.Port := Word(FPort);
  Result.Name := FName;
  Result.User := FUser;
  Result.Password := FPassword;
  Result.AuthToken := FAuthToken;
  Result.JWT := FJWT;
  Result.NKeySeed := FNKeySeed;
  Result.CredentialsFile := FCredentialsFile;
  Result.Verbose := FVerbose;
  Result.Pedantic := FPedantic;
  Result.Echo := FEcho;
  if FConnectTimeoutMs > 0 then
    Result.ConnectTimeoutMs := FConnectTimeoutMs;
  if FRequestTimeoutMs > 0 then
    Result.RequestTimeoutMs := FRequestTimeoutMs;
  if FPingIntervalMs > 0 then
    Result.PingIntervalMs := FPingIntervalMs;
  Result.MaxPingsOutstanding := FMaxPingsOutstanding;
  Result.AllowReconnect := FAllowReconnect;
  Result.MaxReconnectAttempts := FMaxReconnectAttempts;
  if FReconnectWaitMs > 0 then
    Result.ReconnectWaitMs := FReconnectWaitMs;
  if FMaxPendingBufferBytes > 0 then
    Result.MaxPendingBufferBytes := FMaxPendingBufferBytes;
  Result.EnableMetrics := FEnableMetrics;
  if Assigned(FTLS) then
  begin
    Result.TLS.Enabled := FTLS.Enabled;
    Result.TLS.VerifyServerCertificate := FTLS.VerifyServerCertificate;
  end;
end;

function BindNatsOptions(const AConfiguration: IConfiguration;
  const ASectionName: string): TDextNatsOptions;
var
  Section: IConfigurationSection;
  Settings: TDextNatsClientSettings;
begin
  if AConfiguration = nil then
    Exit(TDextNatsOptions.CreateDefault);

  if ASectionName <> '' then
    Section := AConfiguration.GetSection(ASectionName)
  else
    Section := AConfiguration.GetSection('');

  Settings := TConfigurationBinder.Bind<TDextNatsClientSettings>(Section);
  try
    Result := Settings.ToOptions;
  finally
    Settings.Free;
  end;
end;

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

procedure AddNatsClient(const AServices: IServiceCollection; const AConfiguration: IConfiguration;
  const ASectionName: string);
begin
  AddNatsClient(AServices, BindNatsOptions(AConfiguration, ASectionName));
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
