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
{           License.                                                        }
{                                                                           }
{***************************************************************************}
{                                                                           }
{  Client for the NATS messaging system, built on top of Dext.Net.Tcp.      }
{  Supports publish/subscribe (with optional queue groups and message       }
{  headers), synchronous and asynchronous request/reply, automatic          }
{  reconnection with subscription replay (rotating INFO connect_urls),      }
{  keepalive PING/PONG, graceful Drain (UNSUB → flush → disconnect), and    }
{  optional TLS (upgrade after INFO when the server sets tls_required or    }
{  Options.TLS.Enabled is True).                                            }
{                                                                           }
{  JetStream API lives in Dext.Net.Nats.JetStream. NKey/JWT auth is in      }
{  Dext.Net.Nats.NKeys (seed / .creds → CONNECT jwt|nkey + sig).            }
{  DI: Dext.Net.Nats.DependencyInjection. Health: Dext.Net.Nats.HealthChecks.}
{  Optional ILogger + opt-in TMetrics (EnableMetrics).                       }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Diagnostics,
  Dext.Collections.Dict,
  Dext.Collections,
  Dext.Core.Span,
  Dext.Logging,
  Dext.Threading.Async,
  Dext.Net.Tcp,
  Dext.Net.Security,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.NKeys,
  Dext.Net.Nats.ParserRuntime;

const
  /// <summary>Messages delivered to a subscription handler (MSG/HMSG).</summary>
  NATS_METRIC_MSGS_RECEIVED = 'nats.msgs.received';
  /// <summary>PUB/HPUB frames handed to the socket send path.</summary>
  NATS_METRIC_MSGS_PUBLISHED = 'nats.msgs.published';
  /// <summary>Successful automatic reconnects.</summary>
  NATS_METRIC_RECONNECTS = 'nats.reconnects';
  /// <summary>Counted FireError paths (-ERR, handler failures, reconnect failures).</summary>
  NATS_METRIC_ERRORS = 'nats.errors';
  /// <summary>1 while connected after handshake; 0 otherwise.</summary>
  NATS_METRIC_CONNECTED = 'nats.connected';

type
  /// <summary>A fully decoded application message delivered to a subscription handler.</summary>
  TNatsMsg = record
    Subject: string;
    ReplyTo: string;
    /// <summary>Owned payload bytes (stable API). Copy or keep this field to retain data past the handler.</summary>
    Payload: TBytes;
    Headers: TNatsHeaders;
    Sid: Integer;
    /// <summary>Inline status from an HMSG header block (e.g. 503), or 0 when absent.</summary>
    StatusCode: Integer;
    /// <summary>Decodes the payload as a UTF-8 string.</summary>
    function AsString: string;
    /// <summary>
    ///   Zero-copy view over <see cref="Payload"/> storage (PERF-04).
    ///   Lifetime: valid only while this record's <c>Payload</c> dynamic array remains allocated
    ///   and is not reassigned or resized. Safe for the duration of a subscription handler that
    ///   receives <c>const AMsg</c>. Do not store the span alone across awaits, queues, or after
    ///   the handler returns — keep <c>Payload</c> (or call <c>PayloadSpan.ToBytes</c>) instead.
    ///   Empty payload yields an empty span (<c>Data = nil</c>, <c>Length = 0</c>).
    /// </summary>
    function PayloadSpan: TByteSpan;
    /// <summary>True when the message carries a reply subject the handler can publish to.</summary>
    function HasReplyTo: Boolean;
    /// <summary>True when the server reported no responders for a request (status 503).</summary>
    function IsNoResponders: Boolean;
  end;

  TNatsMsgHandler = reference to procedure(const AMsg: TNatsMsg);
  TNatsErrorEvent = reference to procedure(const AErrorMessage: string);
  TNatsConnectedEvent = reference to procedure(const AInfo: TNatsServerInfo; AIsReconnect: Boolean);
  TNatsDisconnectedEvent = reference to procedure;
  TNatsRequestTimeoutHandler = reference to procedure;

  /// <summary>Process-local counters for a <see cref="TDextNatsClient"/> (thread-safe snapshot).</summary>
  TNatsClientMetrics = record
    MessagesReceived: Int64;
    MessagesPublished: Int64;
    Reconnects: Int64;
    Errors: Int64;
  end;

  /// <summary>Tunable behaviour for a <see cref="TDextNatsClient"/> instance.</summary>
  TDextNatsOptions = record
    /// <summary>Optional client name advertised to the server (shown in `nats server info`, monitoring, etc.).</summary>
    Name: string;
    User: string;
    Password: string;
    AuthToken: string;
    /// <summary>User JWT for decentralized auth (often loaded from a <c>.creds</c> file).</summary>
    JWT: string;
    /// <summary>NKey seed (<c>SU…</c>) used to sign the server INFO nonce for CONNECT <c>sig</c>.</summary>
    NKeySeed: string;
    /// <summary>
    ///   Optional path to a NATS credentials file (<c>.creds</c> with JWT + seed, or a bare seed file).
    ///   Loaded on each handshake when set; field JWT/NKeySeed override file values when non-empty.
    /// </summary>
    CredentialsFile: string;
    Verbose: Boolean;
    Pedantic: Boolean;
    /// <summary>When True (default) the server echoes this client's own publishes back to it if subscribed.</summary>
    Echo: Boolean;
    /// <summary>Deadline for the initial TCP connect + CONNECT/PING handshake, and for each reconnect attempt.</summary>
    ConnectTimeoutMs: Integer;
    /// <summary>Default timeout used by <see cref="TDextNatsClient.Request"/> and <see cref="TDextNatsClient.Flush"/> when 0 is passed.</summary>
    RequestTimeoutMs: Integer;
    /// <summary>How often a keepalive PING is sent while idle.</summary>
    PingIntervalMs: Integer;
    /// <summary>Consecutive un-answered keepalive PINGs tolerated before the connection is considered stale.</summary>
    MaxPingsOutstanding: Integer;
    /// <summary>Whether the client should try to reconnect automatically after an unexpected disconnect.</summary>
    AllowReconnect: Boolean;
    /// <summary>Maximum reconnect attempts; -1 retries forever.</summary>
    MaxReconnectAttempts: Integer;
    /// <summary>Delay between reconnect attempts.</summary>
    ReconnectWaitMs: Integer;
    /// <summary>Upper bound, in bytes, for outgoing data buffered while a reconnect is in progress.</summary>
    MaxPendingBufferBytes: Int64;
    /// <summary>
    ///   Optional TLS settings (same record as Redis/Security). When Enabled is True the client
    ///   upgrades the TCP socket after the cleartext INFO frame. A server that advertises
    ///   <c>tls_required</c> also triggers an upgrade even if Enabled is False.
    /// </summary>
    TLS: TDextTLSOptions;
    /// <summary>Default host used by DI helpers and parameterless <c>Connect</c> (default <c>localhost</c>).</summary>
    Host: string;
    /// <summary>Default port used by DI helpers and parameterless <c>Connect</c> (default 4222).</summary>
    Port: Word;
    /// <summary>
    ///   When True, also publish counters/gauges to <c>TMetrics</c> (<c>nats.*</c> names).
    ///   Default False (opt-in).
    /// </summary>
    EnableMetrics: Boolean;
    /// <summary>Sensible defaults: localhost:4222, 5s timeouts, 2 minute keepalive, unlimited reconnects, TLS/metrics off.</summary>
    class function CreateDefault: TDextNatsOptions; static;
  end;

  /// <summary>Bookkeeping for a single active subscription.</summary>
  TDextNatsSubscription = class
  public
    Sid: Integer;
    Subject: string;
    Queue: string;
    Handler: TNatsMsgHandler;
    /// <summary>Absolute message count after which the subscription auto-cancels; -1 means unlimited.</summary>
    MaxMsgs: Integer;
    Received: Integer;
    constructor Create(ASid: Integer; const ASubject, AQueue: string; AHandler: TNatsMsgHandler);
  end;

  /// <summary>
  ///   NATS client. Thread-safe: Publish/Subscribe/Request/Flush may be called
  ///   from any thread while message handlers run on an internal receive thread.
  /// </summary>
  TDextNatsClient = class
  private
    FTcpClient: TDextTcpClient;
    FParser: TDextNatsRuntimeFrameParser;
    FOptions: TDextNatsOptions;
    FHost: string;
    FPort: Word;
    FServerInfo: TNatsServerInfo;

    FLock: TCriticalSection;
    FSendLock: TCriticalSection;
    /// <summary>Serializes OpenSSL engine use across SendRaw and RecvLoop (SSL is not thread-safe).</summary>
    FTlsIoLock: TCriticalSection;
    FTLSEngine: IDextTLSEngine;
    FTLSNetworkBuffer: TBytes;
    FTlsActive: Boolean;

    FSubscriptions: IDictionary<Integer, TDextNatsSubscription>;
    FNextSid: Integer;

    FPongWaiters: IQueue<TEvent>;
    FOutstandingPings: Integer;

    FPendingOutbox: IQueue<TBytes>;
    FPendingOutboxBytes: Int64;

    /// <summary>True while the client should keep a connection alive, including across reconnects.</summary>
    FRunning: Boolean;
    /// <summary>True only while the socket is open and the CONNECT handshake has completed.</summary>
    FConnected: Boolean;
    /// <summary>True once Disconnect() has been requested; suppresses automatic reconnection.</summary>
    FClosing: Boolean;
    /// <summary>True while <see cref="Drain"/> is winding the connection down (UNSUB → flush → close).</summary>
    FDraining: Boolean;
    /// <summary>Handlers currently executing on the receive thread; Drain waits for this to reach 0.</summary>
    FInFlightHandlers: Integer;

    FRecvThread: TThread;
    FPingThread: TThread;

    FOnConnected: TNatsConnectedEvent;
    FOnDisconnected: TNatsDisconnectedEvent;
    FOnError: TNatsErrorEvent;
    FLogger: ILogger;

    FMetricMessagesReceived: Int64;
    FMetricMessagesPublished: Int64;
    FMetricReconnects: Int64;
    FMetricErrors: Int64;

    function GetConnected: Boolean;
    function GetIsDraining: Boolean;
    function GetSubscriptionCount: Integer;
    function GetMetrics: TNatsClientMetrics;
    function NextSid: Integer;

    procedure RecvLoop;
    procedure PingLoop;
    procedure InterruptibleSleep(AMilliseconds: Integer);

    procedure HandleFrame(const AFrame: TNatsFrame);
    procedure HandleMsgFrame(const AFrame: TNatsFrame);
    procedure HandlePongFrame;
    procedure HandleConnectionLost(const AReason: string);

    procedure DoHandshake;
    procedure ApplyAuthCredentials(var AConnectOpts: TNatsConnectOptions);
    procedure UpgradeToTlsIfNeeded;
    procedure PerformTlsHandshake;
    procedure DrainTlsOutput;
    procedure FeedTlsInput(ATimeoutMs: Integer);
    procedure WriteTlsPlaintext(const AData: TBytes);
    function ReceiveBytes(var ABuffer: TBytes; ATimeoutMs: Integer): Integer;
    procedure ResetTls;

    function TryReconnect: Boolean;
    procedure ResendSubscriptions;
    procedure FlushOutbox;
    procedure EnsurePayloadAllowed(const APayload: TBytes);

    function ReceiveFrameBlocking(ATimeoutMs: Integer): TNatsFrame;
    procedure SendRaw(const ABytes: TBytes);
    procedure DispatchOutgoing(const AData: TBytes);

    procedure NoteMetric(const AName: string; var ACounter: Int64);
    procedure SetConnectedGauge(AConnected: Boolean);
    procedure FireError(const AMessage: string);
    procedure FireConnected(AIsReconnect: Boolean);
    procedure FireDisconnected;
  public
    constructor Create(const AOptions: TDextNatsOptions); overload;
    constructor Create; overload;
    destructor Destroy; override;

    /// <summary>Opens a TCP connection using <c>Options.Host</c>/<c>Options.Port</c> (defaults localhost:4222).</summary>
    procedure Connect; overload;
    /// <summary>Opens a TCP connection to a NATS server and performs the CONNECT handshake.
    /// Any subscriptions registered before this call are sent once the handshake completes.</summary>
    procedure Connect(const AHost: string; APort: Word = NATS_DEFAULT_PORT); overload;
    /// <summary>Gracefully closes the connection. Automatic reconnection is disabled until Connect is called again.</summary>
    procedure Disconnect;
    /// <summary>
    ///   Graceful NATS drain then close: stop new interest/publishes, UNSUB all subscriptions
    ///   (keeping local handlers for in-flight MSG), wait for in-flight handlers, Flush outbound
    ///   work, then <see cref="Disconnect"/>. <c>ATimeoutMs &lt;= 0</c> uses
    ///   <c>Options.RequestTimeoutMs</c>. Raises <see cref="EDextNatsTimeoutError"/> if the
    ///   wait/flush budget is exhausted (the connection is still closed).
    /// </summary>
    procedure Drain(ATimeoutMs: Integer = 0);
    /// <summary>Fluent async <see cref="Drain"/> via <see cref="TAsyncBuilder{T}"/> (result is always True on success).</summary>
    function DrainAsync(ATimeoutMs: Integer = 0): TAsyncBuilder<Boolean>;

    /// <summary>Publishes a raw payload to ASubject, optionally requesting replies on AReplyTo.</summary>
    procedure Publish(const ASubject: string; const APayload: TBytes; const AReplyTo: string = ''); overload;
    /// <summary>Publishes a UTF-8 string payload to ASubject.</summary>
    procedure Publish(const ASubject, AMessage: string; const AReplyTo: string = ''); overload;
    /// <summary>Publishes a raw payload together with NATS message headers (requires a server that advertises header support).</summary>
    procedure PublishWithHeaders(const ASubject: string; const APayload: TBytes; const AHeaders: TNatsHeaders;
      const AReplyTo: string = '');

    /// <summary>Subscribes AHandler to ASubject (optionally as part of queue group AQueue) and returns the subscription id.
    /// May be called before Connect(); the SUB frame is sent once the client is connected.</summary>
    function Subscribe(const ASubject: string; const AHandler: TNatsMsgHandler; const AQueue: string = ''): Integer;
    /// <summary>Cancels a subscription. If AMaxMsgs &gt; 0, it auto-cancels after that many additional messages instead of immediately.</summary>
    procedure Unsubscribe(ASid: Integer; AMaxMsgs: Integer = 0);
    /// <summary>Cancels every subscription currently registered for ASubject.</summary>
    procedure UnsubscribeSubject(const ASubject: string);

    /// <summary>Publishes to ASubject and blocks the calling thread until a reply arrives on a private inbox
    /// or ATimeoutMs elapses (0 uses Options.RequestTimeoutMs). Raises EDextNatsTimeoutError on timeout.</summary>
    function Request(const ASubject: string; const APayload: TBytes; ATimeoutMs: Integer = 0): TNatsMsg; overload;
    /// <summary>String payload overload of <see cref="Request"/>.</summary>
    function Request(const ASubject, AMessage: string; ATimeoutMs: Integer = 0): TNatsMsg; overload;
    /// <summary>Same as <see cref="Request"/>, but publishes the request with NATS message headers
    /// (via <see cref="PublishWithHeaders"/>) when AHeaders is non-empty; behaves exactly like Request
    /// when AHeaders is empty, without requiring the server to advertise header support in that case.</summary>
    function RequestWithHeaders(const ASubject: string; const APayload: TBytes; const AHeaders: TNatsHeaders;
      ATimeoutMs: Integer = 0): TNatsMsg;
    /// <summary>Non-blocking request/reply: AOnReply fires on the receive thread when a reply arrives;
    /// AOnTimeout (optional) fires from a helper thread if no reply arrives within ATimeoutMs.</summary>
    procedure RequestAsync(const ASubject: string; const APayload: TBytes; const AOnReply: TNatsMsgHandler;
      const AOnTimeout: TNatsRequestTimeoutHandler = nil; ATimeoutMs: Integer = 0); overload;
    /// <summary>
    ///   Fluent async request/reply via <see cref="TAsyncBuilder{T}"/> (same claim-gate as
    ///   <see cref="Request"/>). Use <c>.Await</c> on the calling thread or <c>.Start</c> on the pool.
    ///   The client must outlive the operation.
    /// </summary>
    function RequestAsync(const ASubject: string; const APayload: TBytes;
      ATimeoutMs: Integer = 0): TAsyncBuilder<TNatsMsg>; overload;

    /// <summary>Generates a new process-unique inbox subject, suitable as a reply-to for request/reply patterns.</summary>
    function NewInbox: string;
    /// <summary>Round-trips a PING/PONG with the server, guaranteeing every command sent before this call
    /// has reached and been processed by the server. Raises EDextNatsTimeoutError on timeout.</summary>
    procedure Flush(ATimeoutMs: Integer = 0);
    /// <summary>Fluent async <see cref="Flush"/> via <see cref="TAsyncBuilder{T}"/> (result is always True on success).</summary>
    function FlushAsync(ATimeoutMs: Integer = 0): TAsyncBuilder<Boolean>;
    /// <summary>Sends a bare PING without waiting for the PONG.</summary>
    procedure Ping;
    /// <summary>
    ///   Records an application-visible error through the same path as server <c>-ERR</c>
    ///   (metrics, optional logger, <see cref="OnError"/>). Useful for tests and host adapters.
    /// </summary>
    procedure NotifyError(const AMessage: string);

    /// <summary>True while the socket is open and the CONNECT handshake has completed.</summary>
    property Connected: Boolean read GetConnected;
    /// <summary>True while <see cref="Drain"/> is in progress (before the final Disconnect).</summary>
    property IsDraining: Boolean read GetIsDraining;
    /// <summary>
    ///   Snapshot of the last INFO JSON from the server (handshake and later async
    ///   INFO refreshes). Includes limits (<c>MaxPayload</c>), cluster/domain,
    ///   account bind hints (<c>RemoteAccount</c> / <c>IsSystemAccount</c>), and
    ///   <c>LameDuckMode</c> when advertised — see <see cref="TNatsServerInfo"/>.
    /// </summary>
    property ServerInfo: TNatsServerInfo read FServerInfo;
    property Options: TDextNatsOptions read FOptions write FOptions;
    property Host: string read FHost;
    property Port: Word read FPort;
    property SubscriptionCount: Integer read GetSubscriptionCount;
    /// <summary>Thread-safe snapshot of process-local message/reconnect/error counters.</summary>
    property Metrics: TNatsClientMetrics read GetMetrics;
    /// <summary>
    ///   Optional structured logger (category typically <c>Dext.Net.Nats</c>). Nil keeps prior behaviour.
    ///   Never logs passwords, tokens, or NKey seeds.
    /// </summary>
    property Logger: ILogger read FLogger write FLogger;

    /// <summary>Fires after a successful (re)connect, on the connecting thread.</summary>
    property OnConnected: TNatsConnectedEvent read FOnConnected write FOnConnected;
    /// <summary>Fires once per connection loss, on the receive thread, before any reconnect attempt.</summary>
    property OnDisconnected: TNatsDisconnectedEvent read FOnDisconnected write FOnDisconnected;
    /// <summary>Fires for server -ERR frames, unhandled handler exceptions, and reconnect failures.</summary>
    property OnError: TNatsErrorEvent read FOnError write FOnError;
  end;

implementation

uses
  Dext.Net.Security.OpenSSL,
  Dext.Telemetry.Metrics,
  Dext.Net.Nats.Protocol.Writer,
  Dext.Net.Nats.Protocol.Control;

{ TNatsMsg }

function TNatsMsg.AsString: string;
begin
  if Length(Payload) = 0 then
    Result := ''
  else
    Result := TEncoding.UTF8.GetString(Payload);
end;

function TNatsMsg.PayloadSpan: TByteSpan;
begin
  Result := TByteSpan.FromBytes(Payload);
end;

function TNatsMsg.HasReplyTo: Boolean;
begin
  Result := ReplyTo <> '';
end;

function TNatsMsg.IsNoResponders: Boolean;
begin
  Result := StatusCode = 503;
end;

{ TDextNatsOptions }

class function TDextNatsOptions.CreateDefault: TDextNatsOptions;
begin
  FillChar(Result, SizeOf(Result), 0);
  Result.Host := 'localhost';
  Result.Port := NATS_DEFAULT_PORT;
  Result.Verbose := False;
  Result.Pedantic := False;
  Result.Echo := True;
  Result.ConnectTimeoutMs := 5000;
  Result.RequestTimeoutMs := 5000;
  Result.PingIntervalMs := 2 * 60 * 1000;
  Result.MaxPingsOutstanding := 2;
  Result.AllowReconnect := True;
  Result.MaxReconnectAttempts := -1;
  Result.ReconnectWaitMs := 2000;
  Result.MaxPendingBufferBytes := 8 * 1024 * 1024;
  Result.EnableMetrics := False;
  FillChar(Result.TLS, SizeOf(Result.TLS), 0);
  Result.TLS.Enabled := False;
  Result.TLS.Mode := tlsmClient;
  Result.TLS.Protocols := [tls1_2, tls1_3];
  Result.TLS.VerifyServerCertificate := True;
  Result.TLS.Provider := 'Auto';
end;

{ TDextNatsSubscription }

constructor TDextNatsSubscription.Create(ASid: Integer; const ASubject, AQueue: string; AHandler: TNatsMsgHandler);
begin
  inherited Create;
  Sid := ASid;
  Subject := ASubject;
  Queue := AQueue;
  Handler := AHandler;
  MaxMsgs := -1;
  Received := 0;
end;

{ Internal request/reply completion gate: guarantees exactly one of the reply
  handler or the timeout handler runs, even when both race on separate threads. }

type
  INatsRequestGate = interface
    ['{7B1E2C3A-4F5D-4E6A-9B7C-8D9E0F1A2B3C}']
    function TryClaim: Boolean;
  end;

  TNatsRequestGate = class(TInterfacedObject, INatsRequestGate)
  private
    FGateLock: TObject;
    FClaimed: Boolean;
  public
    constructor Create;
    destructor Destroy; override;
    function TryClaim: Boolean;
  end;

constructor TNatsRequestGate.Create;
begin
  inherited Create;
  FGateLock := TObject.Create;
end;

destructor TNatsRequestGate.Destroy;
begin
  FGateLock.Free;
  inherited;
end;

function TNatsRequestGate.TryClaim: Boolean;
begin
  TMonitor.Enter(FGateLock);
  try
    Result := not FClaimed;
    FClaimed := True;
  finally
    TMonitor.Exit(FGateLock);
  end;
end;

{ TDextNatsClient }

constructor TDextNatsClient.Create;
begin
  Create(TDextNatsOptions.CreateDefault);
end;

constructor TDextNatsClient.Create(const AOptions: TDextNatsOptions);
begin
  inherited Create;
  FOptions := AOptions;
  FPort := NATS_DEFAULT_PORT;

  FTcpClient := TDextTcpClient.Create;
  FParser := TDextNatsRuntimeFrameParser.Create;
  FLock := TCriticalSection.Create;
  FSendLock := TCriticalSection.Create;
  FTlsIoLock := TCriticalSection.Create;
  SetLength(FTLSNetworkBuffer, 16 * 1024);
  FSubscriptions := TCollections.CreateDictionary<Integer, TDextNatsSubscription>(True);
  FPongWaiters := TCollections.CreateQueue<TEvent>;
  FPendingOutbox := TCollections.CreateQueue<TBytes>;
end;

destructor TDextNatsClient.Destroy;
begin
  Disconnect;
  ResetTls;
  FTlsIoLock.Free;
  FSendLock.Free;
  FLock.Free;
  FParser.Free;
  FTcpClient.Free;
  inherited;
end;

function TDextNatsClient.GetConnected: Boolean;
begin
  FLock.Enter;
  try
    Result := FConnected;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsClient.GetIsDraining: Boolean;
begin
  FLock.Enter;
  try
    Result := FDraining;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsClient.GetSubscriptionCount: Integer;
begin
  FLock.Enter;
  try
    Result := FSubscriptions.Count;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsClient.NextSid: Integer;
begin
  FLock.Enter;
  try
    Inc(FNextSid);
    Result := FNextSid;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsClient.NewInbox: string;
begin
  Result := NatsNewInbox;
end;

procedure TDextNatsClient.ResetTls;
begin
  FTlsIoLock.Enter;
  try
    FTlsActive := False;
    FTLSEngine := nil;
  finally
    FTlsIoLock.Leave;
  end;
end;

procedure TDextNatsClient.DrainTlsOutput;
var
  BytesWritten: Integer;
begin
  repeat
    BytesWritten := FTLSEngine.EncryptedOutgoing(
      @FTLSNetworkBuffer[0], Length(FTLSNetworkBuffer));
    if BytesWritten > 0 then
      FTcpClient.Send(TByteSpan.Create(@FTLSNetworkBuffer[0], BytesWritten));
  until BytesWritten = 0;
end;

procedure TDextNatsClient.FeedTlsInput(ATimeoutMs: Integer);
var
  BytesRead: Integer;
begin
  BytesRead := FTcpClient.Receive(
    TByteSpan.Create(@FTLSNetworkBuffer[0], Length(FTLSNetworkBuffer)), ATimeoutMs);
  if BytesRead <= 0 then
    raise EDextNatsException.Create(
      'NATS server closed the TLS connection unexpectedly');
  if FTLSEngine.EncryptedIncoming(
    @FTLSNetworkBuffer[0], BytesRead) <> BytesRead then
    raise EDextNatsException.Create(
      'OpenSSL input BIO did not accept all encrypted bytes');
end;

procedure TDextNatsClient.PerformTlsHandshake;
var
  Provider: IDextTLSContextProvider;
  Status: TDextTLSEngineStatus;
  LoopCount: Integer;
  tlsOpts: TDextTLSOptions;
begin
  tlsOpts := FOptions.TLS;
  tlsOpts.Enabled := True;
  tlsOpts.Mode := tlsmClient;
  if tlsOpts.Host = '' then
    tlsOpts.Host := FHost;
  if tlsOpts.Protocols = [] then
    tlsOpts.Protocols := [tls1_2, tls1_3];
  if tlsOpts.Provider = '' then
    tlsOpts.Provider := 'Auto';

  Provider := TDextOpenSSLContextProvider.Create(tlsOpts);
  FTLSEngine := Provider.CreateEngine(tlsmClient);

  LoopCount := 0;
  while not FTLSEngine.IsHandshakeCompleted do
  begin
    Inc(LoopCount);
    if LoopCount > 50 then
      raise EDextNatsException.Create('TLS handshake timeout: exceeded 50 iterations');

    Status := FTLSEngine.DoHandshake;
    DrainTlsOutput;

    if FTLSEngine.IsHandshakeCompleted or (Status = tlsHandshakeCompleted) then
      Break;

    if Status = tlsHandshakeNeedRead then
      FeedTlsInput(FOptions.ConnectTimeoutMs)
    else if Status = tlsError then
      raise EDextNatsException.CreateFmt(
        'TLS handshake failed at loop %d (OpenSSL error %d).',
        [LoopCount, FTLSEngine.GetLastErrorCode]);
  end;

  FTlsActive := True;
end;

procedure TDextNatsClient.UpgradeToTlsIfNeeded;
begin
  if FTlsActive then
    Exit;
  if FOptions.TLS.Enabled or FServerInfo.TlsRequired then
    PerformTlsHandshake;
end;

procedure TDextNatsClient.WriteTlsPlaintext(const AData: TBytes);
var
  Offset: Integer;
  Written: Integer;
begin
  Offset := 0;
  while Offset < Length(AData) do
  begin
    Written := FTLSEngine.PlaintextWrite(
      @AData[Offset], Length(AData) - Offset);
    DrainTlsOutput;
    if Written > 0 then
      Inc(Offset, Written)
    else
      case FTLSEngine.GetLastIOStatus of
        tlsIONeedRead:
          FeedTlsInput(FOptions.ConnectTimeoutMs);
        tlsIONeedWrite:
          Continue;
        tlsIOClosed:
          raise EDextNatsException.Create(
            'TLS connection closed while writing to the NATS server');
      else
        raise EDextNatsException.CreateFmt(
          'TLS write failed (OpenSSL error %d)',
          [FTLSEngine.GetLastErrorCode]);
      end;
  end;
  DrainTlsOutput;
end;

function TDextNatsClient.ReceiveBytes(var ABuffer: TBytes; ATimeoutMs: Integer): Integer;
var
  BytesRead: Integer;
begin
  if Length(ABuffer) = 0 then
    Exit(0);
  // Intentional Disconnect sets FClosing before closing the socket. Skip Receive so we
  // do not raise EDextSocketError on the recv thread. Do not key off FRunning — the
  // connect handshake calls ReceiveBytes before FRunning becomes True.
  if FClosing then
    Exit(0);

  if not FTlsActive then
    Exit(FTcpClient.Receive(ABuffer, ATimeoutMs));

  FTlsIoLock.Enter;
  try
    Result := FTLSEngine.PlaintextRead(@ABuffer[0], Length(ABuffer));
    if Result > 0 then
      Exit;

    BytesRead := FTcpClient.Receive(
      TByteSpan.Create(@FTLSNetworkBuffer[0], Length(FTLSNetworkBuffer)), ATimeoutMs);
    if BytesRead <= 0 then
      Exit(0);

    if FTLSEngine.EncryptedIncoming(
      @FTLSNetworkBuffer[0], BytesRead) <> BytesRead then
      raise EDextNatsException.Create(
        'OpenSSL input BIO did not accept all encrypted bytes');

    Result := FTLSEngine.PlaintextRead(@ABuffer[0], Length(ABuffer));
    if (Result = 0) and (FTLSEngine.GetLastIOStatus = tlsIOError) then
      raise EDextNatsException.CreateFmt(
        'TLS read failed (OpenSSL error %d)',
        [FTLSEngine.GetLastErrorCode]);
  finally
    FTlsIoLock.Leave;
  end;
end;

procedure TDextNatsClient.SendRaw(const ABytes: TBytes);
begin
  if FTlsActive then
  begin
    FTlsIoLock.Enter;
    try
      WriteTlsPlaintext(ABytes);
    finally
      FTlsIoLock.Leave;
    end;
  end
  else
  begin
    FSendLock.Enter;
    try
      FTcpClient.Send(ABytes);
    finally
      FSendLock.Leave;
    end;
  end;
end;

function TDextNatsClient.GetMetrics: TNatsClientMetrics;
begin
  Result.MessagesReceived := TInterlocked.Read(FMetricMessagesReceived);
  Result.MessagesPublished := TInterlocked.Read(FMetricMessagesPublished);
  Result.Reconnects := TInterlocked.Read(FMetricReconnects);
  Result.Errors := TInterlocked.Read(FMetricErrors);
end;

procedure TDextNatsClient.NoteMetric(const AName: string; var ACounter: Int64);
begin
  TInterlocked.Increment(ACounter);
  if FOptions.EnableMetrics then
    TMetrics.Increment(AName);
end;

procedure TDextNatsClient.SetConnectedGauge(AConnected: Boolean);
begin
  if FOptions.EnableMetrics then
  begin
    if AConnected then
      TMetrics.Gauge(NATS_METRIC_CONNECTED, 1)
    else
      TMetrics.Gauge(NATS_METRIC_CONNECTED, 0);
  end;
end;

procedure TDextNatsClient.NotifyError(const AMessage: string);
begin
  FireError(AMessage);
end;

procedure TDextNatsClient.FireError(const AMessage: string);
begin
  NoteMetric(NATS_METRIC_ERRORS, FMetricErrors);
  if Assigned(FLogger) and FLogger.IsEnabled(TLogLevel.Error) then
  try
    FLogger.LogError(AMessage);
  except
  end;
  if Assigned(FOnError) then
  try
    FOnError(AMessage);
  except
  end;
end;

procedure TDextNatsClient.FireConnected(AIsReconnect: Boolean);
begin
  SetConnectedGauge(True);
  if AIsReconnect then
    NoteMetric(NATS_METRIC_RECONNECTS, FMetricReconnects);
  if Assigned(FLogger) and FLogger.IsEnabled(TLogLevel.Information) then
  try
    if AIsReconnect then
      FLogger.LogInformation('NATS reconnected to {Host}:{Port} server_id={ServerId}',
        [FHost, FPort, FServerInfo.ServerId])
    else
      FLogger.LogInformation('NATS connected to {Host}:{Port} server_id={ServerId}',
        [FHost, FPort, FServerInfo.ServerId]);
  except
  end;
  if Assigned(FOnConnected) then
  try
    FOnConnected(FServerInfo, AIsReconnect);
  except
  end;
end;

procedure TDextNatsClient.FireDisconnected;
begin
  SetConnectedGauge(False);
  if Assigned(FLogger) and FLogger.IsEnabled(TLogLevel.Information) then
  try
    FLogger.LogInformation('NATS disconnected from {Host}:{Port}', [FHost, FPort]);
  except
  end;
  if Assigned(FOnDisconnected) then
  try
    FOnDisconnected();
  except
  end;
end;

function TDextNatsClient.ReceiveFrameBlocking(ATimeoutMs: Integer): TNatsFrame;
var
  buf: TBytes;
  n: Integer;
  sw: TStopwatch;
  remaining, sliceTimeout: Integer;
begin
  SetLength(buf, 4096);
  sw := TStopwatch.StartNew;
  while True do
  begin
    if FParser.TryReadFrame(Result) then
      Exit;

    remaining := ATimeoutMs - Integer(sw.ElapsedMilliseconds);
    if remaining <= 0 then
      raise EDextNatsTimeoutError.CreateFmt(
        'Timed out after %d ms waiting for a response from the NATS server', [ATimeoutMs]);

    sliceTimeout := remaining;
    if sliceTimeout > 250 then
      sliceTimeout := 250;

    n := ReceiveBytes(buf, sliceTimeout);
    if n > 0 then
      FParser.Append(buf, n);
  end;
end;

procedure TDextNatsClient.ApplyAuthCredentials(var AConnectOpts: TNatsConnectOptions);
var
  Creds: TNatsCredentials;
  JWT, Seed: string;
begin
  JWT := Trim(FOptions.JWT);
  Seed := Trim(FOptions.NKeySeed);

  if Trim(FOptions.CredentialsFile) <> '' then
  begin
    Creds := TNatsCredentials.FromFile(FOptions.CredentialsFile);
    if JWT = '' then
      JWT := Creds.JWT;
    if Seed = '' then
      Seed := Creds.Seed;
  end;

  if (JWT = '') and (Seed = '') then
    Exit;

  NatsApplyCredentialsToConnect(AConnectOpts, JWT, Seed, FServerInfo.Nonce);
end;

procedure TDextNatsClient.DoHandshake;
var
  frame: TNatsFrame;
  connOpts: TNatsConnectOptions;
  sw: TStopwatch;
  remaining: Integer;
  gotPong: Boolean;
begin
  ResetTls;
  FParser.Clear;

  // NATS always speaks cleartext INFO first; TLS (if any) upgrades the same TCP socket next.
  frame := ReceiveFrameBlocking(FOptions.ConnectTimeoutMs);
  if frame.Kind <> nfInfo then
    raise EDextNatsProtocolError.Create('Expected an INFO message as the first reply from the NATS server');

  FServerInfo := TNatsServerInfo.Parse(frame.InfoJson);
  UpgradeToTlsIfNeeded;

  connOpts := TNatsConnectOptions.CreateDefault;
  connOpts.Verbose := FOptions.Verbose;
  connOpts.Pedantic := FOptions.Pedantic;
  connOpts.Echo := FOptions.Echo;
  connOpts.Name := FOptions.Name;
  connOpts.User := FOptions.User;
  connOpts.Password := FOptions.Password;
  connOpts.AuthToken := FOptions.AuthToken;
  connOpts.Headers := FServerInfo.HeadersSupported;
  ApplyAuthCredentials(connOpts);

  if FServerInfo.AuthRequired
    and (connOpts.User = '') and (connOpts.AuthToken = '')
    and (connOpts.JWT = '') and (connOpts.Nkey = '') and (connOpts.Sig = '') then
    raise EDextNatsAuthError.Create(
      'NATS server requires authentication but no credentials were configured ' +
      '(set User/Password, AuthToken, JWT+NKeySeed, or CredentialsFile)');

  SendRaw(NatsV2EncodeConnect(connOpts));
  SendRaw(NatsControlPing);

  sw := TStopwatch.StartNew;
  gotPong := False;
  while not gotPong do
  begin
    remaining := FOptions.ConnectTimeoutMs - Integer(sw.ElapsedMilliseconds);
    if remaining <= 0 then
      raise EDextNatsTimeoutError.Create('Timed out waiting for the NATS server to acknowledge CONNECT');

    frame := ReceiveFrameBlocking(remaining);
    case frame.Kind of
      nfPong: gotPong := True;
      nfOK: ; // verbose acknowledgement of CONNECT/PING; keep waiting for the PONG
      nfInfo: FServerInfo := TNatsServerInfo.Parse(frame.InfoJson); // e.g. updated cluster topology
      nfErr: raise EDextNatsServerError.CreateFmt('NATS server rejected the connection: %s', [frame.ErrorText]);
    else
      raise EDextNatsProtocolError.Create('Unexpected message from the NATS server during the connect handshake');
    end;
  end;
end;

procedure TDextNatsClient.Connect;
var
  Host: string;
  Port: Word;
begin
  Host := FOptions.Host;
  if Host = '' then
    Host := 'localhost';
  Port := FOptions.Port;
  if Port = 0 then
    Port := NATS_DEFAULT_PORT;
  Connect(Host, Port);
end;

procedure TDextNatsClient.Connect(const AHost: string; APort: Word);
begin
  if Connected then
    Exit;

  FHost := AHost;
  FPort := APort;
  FClosing := False;
  FDraining := False;
  TInterlocked.Exchange(FInFlightHandlers, 0);

  ResetTls;
  FTcpClient.Connect(AHost, APort);
  try
    DoHandshake;
  except
    ResetTls;
    FTcpClient.Disconnect;
    raise;
  end;

  FLock.Enter;
  try
    FConnected := True;
  finally
    FLock.Leave;
  end;
  FRunning := True;

  ResendSubscriptions;
  FlushOutbox;

  FRecvThread := TThread.CreateAnonymousThread(RecvLoop);
  FRecvThread.FreeOnTerminate := False;
  FRecvThread.Start;

  FPingThread := TThread.CreateAnonymousThread(PingLoop);
  FPingThread.FreeOnTerminate := False;
  FPingThread.Start;

  FireConnected(False);
end;

procedure TDextNatsClient.Disconnect;
begin
  FLock.Enter;
  try
    FClosing := True;
    FDraining := False;
  finally
    FLock.Leave;
  end;
  FRunning := False;

  try
    FTcpClient.Disconnect;
  except
  end;

  if Assigned(FRecvThread) then
  begin
    FRecvThread.WaitFor;
    FreeAndNil(FRecvThread);
  end;

  if Assigned(FPingThread) then
  begin
    FPingThread.WaitFor;
    FreeAndNil(FPingThread);
  end;

  ResetTls;

  FLock.Enter;
  try
    FConnected := False;
  finally
    FLock.Leave;
  end;
  SetConnectedGauge(False);
end;

procedure TDextNatsClient.InterruptibleSleep(AMilliseconds: Integer);
var
  waited, slice: Integer;
begin
  waited := 0;
  while FRunning and not FClosing and (waited < AMilliseconds) do
  begin
    slice := AMilliseconds - waited;
    if slice > 200 then
      slice := 200;
    Sleep(slice);
    Inc(waited, slice);
  end;
end;

procedure TDextNatsClient.ResendSubscriptions;
var
  sub: TDextNatsSubscription;
  count: Integer;
begin
  count := 0;
  FLock.Enter;
  try
    for sub in FSubscriptions.Values do
    begin
      SendRaw(NatsControlSub(sub.Subject, sub.Queue, sub.Sid));
      if sub.MaxMsgs >= 0 then
        SendRaw(NatsControlUnsub(sub.Sid, sub.MaxMsgs - sub.Received));
      Inc(count);
    end;
  finally
    FLock.Leave;
  end;
  if (count > 0) and Assigned(FLogger) and FLogger.IsEnabled(TLogLevel.Debug) then
  try
    FLogger.LogDebug('NATS replayed {Count} subscription(s) after (re)connect', [count]);
  except
  end;
end;

procedure TDextNatsClient.FlushOutbox;
var
  data: TBytes;
  hasData: Boolean;
begin
  // Never hold the shared client-state lock across socket/TLS I/O. Dequeue one
  // frame at a time under FLock, then send it under the dedicated send lock.
  // FIFO ordering is preserved because this routine is the sole outbox drainer.
  while True do
  begin
    FLock.Enter;
    try
      hasData := FPendingOutbox.Count > 0;
      if hasData then
      begin
        data := FPendingOutbox.Dequeue;
        Dec(FPendingOutboxBytes, Length(data));
      end;
    finally
      FLock.Leave;
    end;

    if not hasData then
      Break;
    SendRaw(data);
  end;
end;

function TryParseNatsEndpoint(const AUrl: string; out AHost: string; out APort: Word): Boolean;
var
  s, portStr: string;
  slashPos, atPos, colonPos, bracketEnd: Integer;
begin
  Result := False;
  AHost := '';
  APort := 0;
  s := Trim(AUrl);
  if s = '' then
    Exit;

  if s.StartsWith('nats://', True) then
    Delete(s, 1, 7)
  else if s.StartsWith('tls://', True) then
    Delete(s, 1, 6);

  slashPos := Pos('/', s);
  if slashPos > 0 then
    s := Copy(s, 1, slashPos - 1);

  atPos := Pos('@', s);
  if atPos > 0 then
    s := Copy(s, atPos + 1, MaxInt);

  if (s <> '') and (s[1] = '[') then
  begin
    bracketEnd := Pos(']', s);
    if bracketEnd <= 1 then
      Exit;
    AHost := Copy(s, 2, bracketEnd - 2);
    if (bracketEnd < Length(s)) and (s[bracketEnd + 1] = ':') then
      portStr := Copy(s, bracketEnd + 2, MaxInt)
    else
      portStr := IntToStr(NATS_DEFAULT_PORT);
  end
  else
  begin
    colonPos := LastDelimiter(':', s);
    if colonPos = 0 then
    begin
      AHost := s;
      APort := NATS_DEFAULT_PORT;
      Exit(AHost <> '');
    end;
    AHost := Copy(s, 1, colonPos - 1);
    portStr := Copy(s, colonPos + 1, MaxInt);
  end;

  APort := Word(StrToIntDef(portStr, 0));
  Result := (AHost <> '') and (APort > 0);
end;

function TDextNatsClient.TryReconnect: Boolean;
type
  TNatsEndpoint = record
    Host: string;
    Port: Word;
  end;
var
  attempt, endpointIndex: Integer;
  endpoints: TArray<TNatsEndpoint>;
  target: TNatsEndpoint;

  procedure AddEndpoint(const AHost: string; APort: Word);
  var
    j: Integer;
    ep: TNatsEndpoint;
  begin
    if (AHost = '') or (APort = 0) then
      Exit;
    for j := 0 to High(endpoints) do
      if SameText(endpoints[j].Host, AHost) and (endpoints[j].Port = APort) then
        Exit;
    ep.Host := AHost;
    ep.Port := APort;
    endpoints := endpoints + [ep];
  end;

  procedure RebuildEndpoints;
  var
    u: string;
    h: string;
    p: Word;
  begin
    endpoints := nil;
    AddEndpoint(FHost, FPort);
    for u in FServerInfo.ConnectUrls do
      if TryParseNatsEndpoint(u, h, p) then
        AddEndpoint(h, p);
  end;
begin
  Result := False;
  attempt := 0;

  while FRunning and not FClosing do
  begin
    Inc(attempt);
    if (FOptions.MaxReconnectAttempts >= 0) and (attempt > FOptions.MaxReconnectAttempts) then
      Exit(False);

    InterruptibleSleep(FOptions.ReconnectWaitMs);
    if not FRunning or FClosing then
      Exit(False);

    // Rebuild each attempt so an INFO refresh can advertise new connect_urls.
    RebuildEndpoints;
    if Length(endpoints) = 0 then
    begin
      target.Host := FHost;
      target.Port := FPort;
    end
    else
    begin
      endpointIndex := (attempt - 1) mod Length(endpoints);
      target := endpoints[endpointIndex];
    end;

    try
      FParser.Clear;
      ResetTls;
      FTcpClient.Connect(target.Host, target.Port);
      try
        DoHandshake;
      except
        ResetTls;
        FTcpClient.Disconnect;
        raise;
      end;

      FHost := target.Host;
      FPort := target.Port;

      FLock.Enter;
      try
        FConnected := True;
      finally
        FLock.Leave;
      end;

      ResendSubscriptions;
      FlushOutbox;
      FireConnected(True);
      Exit(True);
    except
      on E: Exception do
        FireError(Format('NATS reconnect attempt %d to %s:%d failed: %s',
          [attempt, target.Host, target.Port, E.Message]));
    end;
  end;
end;

procedure TDextNatsClient.HandleConnectionLost(const AReason: string);
begin
  FLock.Enter;
  try
    if not FConnected then
      Exit; // already being handled (e.g. RecvLoop and PingLoop both noticed at once)
    FConnected := False;
  finally
    FLock.Leave;
  end;

  try
    FTcpClient.Disconnect;
  except
  end;
  ResetTls;

  FireDisconnected;

  if FClosing or FDraining or not FOptions.AllowReconnect then
  begin
    FRunning := False;
    Exit;
  end;

  if Assigned(FLogger) and FLogger.IsEnabled(TLogLevel.Warning) then
  try
    FLogger.LogWarning('NATS reconnecting after connection loss: {Reason}', [AReason]);
  except
  end;

  if not TryReconnect then
    FRunning := False;
end;

procedure TDextNatsClient.RecvLoop;
var
  buf: TBytes;
  n: Integer;
  frame: TNatsFrame;
begin
  SetLength(buf, 65536);
  while FRunning do
  begin
    n := 0;
    try
      // PingLoop may only Disconnect the socket; detect the closed flag here and
      // drive reconnect. Soft recv results (timeout / WSAEINTR) return n=0.
      if not FTcpClient.Connected then
      begin
        if not FRunning then
          Break;
        HandleConnectionLost('TCP connection closed');
        if not FRunning then
          Break;
        Continue;
      end;
      n := ReceiveBytes(buf, 200);
    except
      on E: Exception do
      begin
        n := 0;
        // Disconnect sets FRunning=False before closing; swallow teardown errors.
        if not FRunning then
          Break;
        HandleConnectionLost(E.Message);
      end;
    end;

    if not FRunning then
      Break;

    if n > 0 then
    begin
      try
        FParser.Append(buf, n);
        while FRunning and FParser.TryReadFrame(frame) do
          HandleFrame(frame);
      except
        on E: Exception do
        begin
          FireError('NATS protocol error: ' + E.Message);
          if FRunning then
            HandleConnectionLost(E.Message);
        end;
      end;
    end;
  end;
end;

procedure TDextNatsClient.PingLoop;
begin
  while FRunning do
  begin
    InterruptibleSleep(FOptions.PingIntervalMs);
    if not FRunning then
      Break;
    if not Connected then
      Continue; // a reconnect is in progress; try again next cycle

    if FOutstandingPings >= FOptions.MaxPingsOutstanding then
    begin
      if Assigned(FLogger) and FLogger.IsEnabled(TLogLevel.Warning) then
      try
        FLogger.LogWarning(
          'NATS stale keepalive: no PONG after {Count} PING(s); closing socket for RecvLoop reconnect',
          [FOutstandingPings]);
      except
      end;
      FireError(Format('No PONG received after %d keepalive PING(s); closing the connection so the receive ' +
        'thread can reconnect', [FOutstandingPings]));
      // Only close the socket here; RecvLoop (the single owner of FTcpClient's read side) will observe the
      // failure on its own next Receive call and drive the actual reconnect. Reconnecting from two threads
      // at once would race on the shared TDextTcpClient instance.
      TInterlocked.Exchange(FOutstandingPings, 0);
      try
        FTcpClient.Disconnect;
      except
      end;
      Continue;
    end;

    try
      FLock.Enter;
      try
        FPongWaiters.Enqueue(nil);
      finally
        FLock.Leave;
      end;
      TInterlocked.Increment(FOutstandingPings);
      SendRaw(NatsControlPing);
    except
      on E: Exception do
        FireError('Failed to send keepalive PING: ' + E.Message);
    end;
  end;
end;

procedure TDextNatsClient.HandlePongFrame;
var
  evt: TEvent;
begin
  TInterlocked.Exchange(FOutstandingPings, 0);
  FLock.Enter;
  try
    evt := nil;
    if FPongWaiters.Count > 0 then
      evt := FPongWaiters.Dequeue;
    if Assigned(evt) then
      evt.SetEvent;
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsClient.HandleFrame(const AFrame: TNatsFrame);
begin
  case AFrame.Kind of
    nfInfo: FServerInfo := TNatsServerInfo.Parse(AFrame.InfoJson);
    nfPing: SendRaw(NatsControlPong);
    nfPong: HandlePongFrame;
    nfMsg, nfHMsg: HandleMsgFrame(AFrame);
    nfOK: ; // nothing to do
    nfErr: FireError('NATS server error: ' + AFrame.ErrorText);
  end;
end;

procedure TDextNatsClient.HandleMsgFrame(const AFrame: TNatsFrame);
var
  sub: TDextNatsSubscription;
  msg: TNatsMsg;
  found, removeAfter: Boolean;
  handler: TNatsMsgHandler;
begin
  found := False;
  removeAfter := False;
  handler := nil;

  FLock.Enter;
  try
    found := FSubscriptions.TryGetValue(AFrame.Sid, sub);
    if found then
    begin
      Inc(sub.Received);
      handler := sub.Handler;
      if (sub.MaxMsgs >= 0) and (sub.Received >= sub.MaxMsgs) then
        removeAfter := True;
    end;
  finally
    FLock.Leave;
  end;

  if not found then
    Exit; // message for a subscription we no longer track (e.g. just unsubscribed); ignore it

  msg.Subject := AFrame.Subject;
  msg.ReplyTo := AFrame.ReplyTo;
  msg.Payload := AFrame.Payload;
  msg.Headers := AFrame.Headers;
  msg.Sid := AFrame.Sid;
  msg.StatusCode := AFrame.StatusCode;

  NoteMetric(NATS_METRIC_MSGS_RECEIVED, FMetricMessagesReceived);

  if Assigned(handler) then
  begin
    TInterlocked.Increment(FInFlightHandlers);
    try
      try
        handler(msg);
      except
        on E: Exception do
          FireError('Unhandled exception in NATS message handler for subject "' + msg.Subject + '": ' + E.Message);
      end;
    finally
      TInterlocked.Decrement(FInFlightHandlers);
    end;
  end;

  if removeAfter then
  begin
    FLock.Enter;
    try
      FSubscriptions.Remove(AFrame.Sid);
    finally
      FLock.Leave;
    end;
  end;
end;

procedure TDextNatsClient.DispatchOutgoing(const AData: TBytes);
type
  TDispatchAction = (daSendNow, daBuffered, daReject);
  TRejectReason = (rrNone, rrDraining, rrNotConnected);
var
  action: TDispatchAction;
  reject: TRejectReason;
begin
  reject := rrNone;
  FLock.Enter;
  try
    if FDraining then
    begin
      action := daReject;
      reject := rrDraining;
    end
    else if FConnected then
      action := daSendNow
    else if FRunning and FOptions.AllowReconnect and not FClosing then
    begin
      if FPendingOutboxBytes + Length(AData) > FOptions.MaxPendingBufferBytes then
      begin
        action := daReject;
        reject := rrNotConnected;
      end
      else
      begin
        FPendingOutbox.Enqueue(AData);
        Inc(FPendingOutboxBytes, Length(AData));
        action := daBuffered;
      end;
    end
    else
    begin
      action := daReject;
      reject := rrNotConnected;
    end;
  finally
    FLock.Leave;
  end;

  case action of
    daSendNow: SendRaw(AData);
    daReject:
      if reject = rrDraining then
        raise EDextNatsException.Create('Publish rejected: NATS client is draining')
      else
        raise EDextNatsException.Create(
          'Not connected to a NATS server (and no room left in the reconnect buffer)');
  end;
end;

procedure TDextNatsClient.EnsurePayloadAllowed(const APayload: TBytes);
begin
  if (FServerInfo.MaxPayload > 0) and (Length(APayload) > FServerInfo.MaxPayload) then
    raise EDextNatsException.CreateFmt(
      'Payload size %d exceeds server max_payload of %d bytes',
      [Length(APayload), FServerInfo.MaxPayload]);
end;

procedure TDextNatsClient.Publish(const ASubject: string; const APayload: TBytes; const AReplyTo: string);
begin
  if ASubject = '' then
    raise EDextNatsException.Create('Publish requires a non-empty subject');
  EnsurePayloadAllowed(APayload);
  DispatchOutgoing(NatsV2EncodePub(ASubject, AReplyTo, APayload));
  NoteMetric(NATS_METRIC_MSGS_PUBLISHED, FMetricMessagesPublished);
end;

procedure TDextNatsClient.Publish(const ASubject, AMessage: string; const AReplyTo: string);
begin
  Publish(ASubject, TEncoding.UTF8.GetBytes(AMessage), AReplyTo);
end;

procedure TDextNatsClient.PublishWithHeaders(const ASubject: string; const APayload: TBytes;
  const AHeaders: TNatsHeaders; const AReplyTo: string);
begin
  if ASubject = '' then
    raise EDextNatsException.Create('Publish requires a non-empty subject');
  if not FServerInfo.HeadersSupported then
    raise EDextNatsNotSupported.Create(
      'The NATS server does not advertise message header support (or the client is not connected yet)');
  EnsurePayloadAllowed(APayload);

  DispatchOutgoing(NatsV2EncodeHPub(ASubject, AReplyTo, AHeaders, APayload));
  NoteMetric(NATS_METRIC_MSGS_PUBLISHED, FMetricMessagesPublished);
end;

function TDextNatsClient.Subscribe(const ASubject: string; const AHandler: TNatsMsgHandler;
  const AQueue: string): Integer;
var
  sub: TDextNatsSubscription;
  sendNow: Boolean;
begin
  if ASubject = '' then
    raise EDextNatsException.Create('Subscribe requires a non-empty subject');
  if not Assigned(AHandler) then
    raise EDextNatsException.Create('Subscribe requires a message handler');

  sub := TDextNatsSubscription.Create(NextSid, ASubject, AQueue, AHandler);

  FLock.Enter;
  try
    if FDraining or FClosing then
    begin
      sub.Free;
      raise EDextNatsException.Create('Subscribe rejected: NATS client is draining or closing');
    end;
    FSubscriptions.Add(sub.Sid, sub);
    sendNow := FConnected;
  finally
    FLock.Leave;
  end;

  if sendNow then
    SendRaw(NatsControlSub(ASubject, AQueue, sub.Sid));

  Result := sub.Sid;
end;

procedure TDextNatsClient.Unsubscribe(ASid: Integer; AMaxMsgs: Integer);
var
  sub: TDextNatsSubscription;
  sendNow: Boolean;
begin
  sendNow := False;
  FLock.Enter;
  try
    if not FSubscriptions.TryGetValue(ASid, sub) then
      Exit;

    sendNow := FConnected;

    if AMaxMsgs <= 0 then
      FSubscriptions.Remove(ASid)
    else
      sub.MaxMsgs := sub.Received + AMaxMsgs;
  finally
    FLock.Leave;
  end;

  if sendNow then
    SendRaw(NatsControlUnsub(ASid, AMaxMsgs));
end;

procedure TDextNatsClient.UnsubscribeSubject(const ASubject: string);
var
  sids: IList<Integer>;
  pair: TPair<Integer, TDextNatsSubscription>;
  sid: Integer;
begin
  sids := TCollections.CreateList<Integer>;

  FLock.Enter;
  try
    for pair in FSubscriptions do
      if SameText(pair.Value.Subject, ASubject) then
        sids.Add(pair.Key);
  finally
    FLock.Leave;
  end;

  for sid in sids do
    Unsubscribe(sid);
end;

function TDextNatsClient.Request(const ASubject: string; const APayload: TBytes; ATimeoutMs: Integer): TNatsMsg;
begin
  Result := RequestWithHeaders(ASubject, APayload, nil, ATimeoutMs);
end;

function TDextNatsClient.Request(const ASubject, AMessage: string; ATimeoutMs: Integer): TNatsMsg;
begin
  Result := Request(ASubject, TEncoding.UTF8.GetBytes(AMessage), ATimeoutMs);
end;

function TDextNatsClient.RequestWithHeaders(const ASubject: string; const APayload: TBytes;
  const AHeaders: TNatsHeaders; ATimeoutMs: Integer): TNatsMsg;
var
  inbox: string;
  sid: Integer;
  evt: TEvent;
  reply: TNatsMsg;
  gate: INatsRequestGate;
begin
  if ATimeoutMs <= 0 then
    ATimeoutMs := FOptions.RequestTimeoutMs;

  inbox := NewInbox;
  evt := TEvent.Create(nil, True, False, '');
  gate := TNatsRequestGate.Create;
  try
    sid := Subscribe(inbox,
      procedure(const AMsg: TNatsMsg)
      begin
        // Only touch `reply`/`evt` after winning the gate, so that once the timeout path below
        // claims it first, the handler is guaranteed never to touch either afterwards - this is
        // what makes it safe for the timeout path to free `evt` right after claiming the gate.
        if gate.TryClaim then
        begin
          reply := AMsg;
          evt.SetEvent;
        end;
      end);
    Unsubscribe(sid, 1); // ask the server to auto-cancel this subscription after exactly one reply

    if Length(AHeaders) > 0 then
      PublishWithHeaders(ASubject, APayload, AHeaders, inbox)
    else
      Publish(ASubject, APayload, inbox);

    if evt.WaitFor(ATimeoutMs) <> wrSignaled then
    begin
      Unsubscribe(sid);
      if gate.TryClaim then
        raise EDextNatsTimeoutError.CreateFmt('No response received on subject "%s" within %d ms',
          [ASubject, ATimeoutMs])
      else if evt.WaitFor(5000) <> wrSignaled then
        // Extremely unlikely: the handler claimed the gate (a reply arrived right at the deadline)
        // but never got around to signaling `evt`. Treat it the same as an ordinary timeout.
        raise EDextNatsTimeoutError.CreateFmt('No response received on subject "%s" within %d ms',
          [ASubject, ATimeoutMs]);
    end;
  finally
    evt.Free;
  end;

  if reply.IsNoResponders then
    raise EDextNatsNoResponders.CreateFmt('No responders available for subject "%s"', [ASubject]);

  Result := reply;
end;

procedure TDextNatsClient.RequestAsync(const ASubject: string; const APayload: TBytes;
  const AOnReply: TNatsMsgHandler; const AOnTimeout: TNatsRequestTimeoutHandler; ATimeoutMs: Integer);
var
  inbox: string;
  sid: Integer;
  gate: INatsRequestGate;
begin
  if not Assigned(AOnReply) then
    raise EDextNatsException.Create('RequestAsync requires AOnReply');
  if ATimeoutMs <= 0 then
    ATimeoutMs := FOptions.RequestTimeoutMs;

  inbox := NewInbox;
  gate := TNatsRequestGate.Create;

  sid := Subscribe(inbox,
    procedure(const AMsg: TNatsMsg)
    begin
      if gate.TryClaim then
        AOnReply(AMsg);
    end);
  Unsubscribe(sid, 1);

  Publish(ASubject, APayload, inbox);

  if Assigned(AOnTimeout) then
  begin
    TThread.CreateAnonymousThread(
      procedure
      begin
        Sleep(ATimeoutMs);
        if gate.TryClaim then
        begin
          Unsubscribe(sid);
          AOnTimeout();
        end;
      end).Start;
  end;
end;

function TDextNatsClient.RequestAsync(const ASubject: string; const APayload: TBytes;
  ATimeoutMs: Integer): TAsyncBuilder<TNatsMsg>;
var
  subject: string;
  payload: TBytes;
  timeoutMs: Integer;
begin
  subject := ASubject;
  payload := APayload;
  timeoutMs := ATimeoutMs;
  Result := TAsyncTask.Run<TNatsMsg>(
    function: TNatsMsg
    begin
      Result := Self.Request(subject, payload, timeoutMs);
    end);
end;

procedure TDextNatsClient.Ping;
begin
  SendRaw(NatsControlPing);
end;

procedure TDextNatsClient.Flush(ATimeoutMs: Integer);
var
  evt, item: TEvent;
  items: TArray<TEvent>;
  removed: Boolean;
begin
  if not Connected then
    raise EDextNatsException.Create('Flush failed: not connected to a NATS server');
  if ATimeoutMs <= 0 then
    ATimeoutMs := FOptions.RequestTimeoutMs;

  evt := TEvent.Create(nil, True, False, '');
  try
    FLock.Enter;
    try
      FPongWaiters.Enqueue(evt);
    finally
      FLock.Leave;
    end;

    SendRaw(NatsControlPing);

    if evt.WaitFor(ATimeoutMs) <> wrSignaled then
    begin
      // Remove our own waiter if HandlePongFrame has not already claimed it; both operations
      // are serialized on FLock, so whichever runs first "wins" and the other is a safe no-op.
      FLock.Enter;
      try
        removed := False;
        items := FPongWaiters.ToArray;
        FPongWaiters.Clear;
        for item in items do
        begin
          if (not removed) and (item = evt) then
            removed := True
          else
            FPongWaiters.Enqueue(item);
        end;
      finally
        FLock.Leave;
      end;

      raise EDextNatsTimeoutError.CreateFmt('Flush timed out after %d ms waiting for the server PONG', [ATimeoutMs]);
    end;
  finally
    evt.Free;
  end;
end;

function TDextNatsClient.FlushAsync(ATimeoutMs: Integer): TAsyncBuilder<Boolean>;
var
  timeoutMs: Integer;
begin
  timeoutMs := ATimeoutMs;
  Result := TAsyncTask.Run<Boolean>(
    function: Boolean
    begin
      Self.Flush(timeoutMs);
      Result := True;
    end);
end;

procedure TDextNatsClient.Drain(ATimeoutMs: Integer);
var
  sids: IList<Integer>;
  pair: TPair<Integer, TDextNatsSubscription>;
  sid: Integer;
  timeoutMs, remaining, slice: Integer;
  sw: TStopwatch;
  timedOut, already: Boolean;
  flushBudget: Integer;
begin
  if ATimeoutMs <= 0 then
    ATimeoutMs := FOptions.RequestTimeoutMs;
  timeoutMs := ATimeoutMs;
  timedOut := False;
  sw := TStopwatch.StartNew;

  FLock.Enter;
  try
    already := FDraining or FClosing;
    if not already then
      FDraining := True;
  finally
    FLock.Leave;
  end;

  if already then
  begin
    // Another Drain/Disconnect owns shutdown; wait briefly for Connected to clear.
    while Connected and (Integer(sw.ElapsedMilliseconds) < timeoutMs) do
      Sleep(20);
    Exit;
  end;

  try
    if not Connected then
      Exit;

    // 1) UNSUB all interest; keep local map so RecvLoop can still deliver in-flight MSG.
    sids := TCollections.CreateList<Integer>;
    FLock.Enter;
    try
      for pair in FSubscriptions do
        sids.Add(pair.Key);
    finally
      FLock.Leave;
    end;

    for sid in sids do
    try
      SendRaw(NatsControlUnsub(sid, 0));
    except
      on E: Exception do
        FireError('Drain UNSUB failed for sid ' + IntToStr(sid) + ': ' + E.Message);
    end;

    // 2) Flush outbound PUB/UNSUB (PING/PONG barrier — server has processed UNSUBs).
    if Connected then
    begin
      flushBudget := timeoutMs - Integer(sw.ElapsedMilliseconds);
      if flushBudget <= 0 then
        timedOut := True
      else
      try
        Flush(flushBudget);
      except
        on E: EDextNatsTimeoutError do
          timedOut := True;
        on E: EDextNatsException do
          // Connection may already be gone; still finish with Disconnect.
          FireError('Drain Flush failed: ' + E.Message);
      end;
    end;

    // 3) Wait for in-flight handlers + a short quiet window for frames already on the wire.
    while True do
    begin
      remaining := timeoutMs - Integer(sw.ElapsedMilliseconds);
      if remaining <= 0 then
      begin
        timedOut := True;
        Break;
      end;
      if TInterlocked.CompareExchange(FInFlightHandlers, 0, 0) = 0 then
      begin
        slice := remaining;
        if slice > 50 then
          slice := 50;
        Sleep(slice);
        if TInterlocked.CompareExchange(FInFlightHandlers, 0, 0) = 0 then
          Break;
      end
      else
      begin
        slice := remaining;
        if slice > 20 then
          slice := 20;
        Sleep(slice);
      end;
    end;

    // Drop local interest before close (server already has UNSUB).
    FLock.Enter;
    try
      FSubscriptions.Clear;
    finally
      FLock.Leave;
    end;
  finally
    // 4) Close — common NATS client semantics: Drain ends disconnected.
    try
      Disconnect;
    except
    end;
  end;

  if timedOut then
    raise EDextNatsTimeoutError.CreateFmt(
      'Drain timed out after %d ms (connection closed)', [timeoutMs]);
end;

function TDextNatsClient.DrainAsync(ATimeoutMs: Integer): TAsyncBuilder<Boolean>;
var
  timeoutMs: Integer;
begin
  timeoutMs := ATimeoutMs;
  Result := TAsyncTask.Run<Boolean>(
    function: Boolean
    begin
      Self.Drain(timeoutMs);
      Result := True;
    end);
end;

end.
