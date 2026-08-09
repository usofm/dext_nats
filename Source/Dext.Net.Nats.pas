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
{  reconnection with subscription replay, and keepalive PING/PONG.          }
{                                                                           }
{  Not yet implemented: TLS ("tls_required" servers are rejected with a     }
{  clear error) and JetStream.                                              }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Diagnostics,
  Dext.Collections,
  Dext.Core.Span,
  Dext.Net.Tcp,
  Dext.Net.Nats.Protocol;

type
  /// <summary>A fully decoded application message delivered to a subscription handler.</summary>
  TNatsMsg = record
    Subject: string;
    ReplyTo: string;
    Payload: TBytes;
    Headers: TNatsHeaders;
    Sid: Integer;
    /// <summary>Decodes the payload as a UTF-8 string.</summary>
    function AsString: string;
    /// <summary>True when the message carries a reply subject the handler can publish to.</summary>
    function HasReplyTo: Boolean;
  end;

  TNatsMsgHandler = reference to procedure(const AMsg: TNatsMsg);
  TNatsErrorEvent = reference to procedure(const AErrorMessage: string);
  TNatsConnectedEvent = reference to procedure(const AInfo: TNatsServerInfo; AIsReconnect: Boolean);
  TNatsDisconnectedEvent = reference to procedure;
  TNatsRequestTimeoutHandler = reference to procedure;

  /// <summary>Tunable behaviour for a <see cref="TDextNatsClient"/> instance.</summary>
  TDextNatsOptions = record
    /// <summary>Optional client name advertised to the server (shown in `nats server info`, monitoring, etc.).</summary>
    Name: string;
    User: string;
    Password: string;
    AuthToken: string;
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
    /// <summary>Sensible defaults: 5s handshake/request timeouts, 2 minute keepalive, unlimited reconnects.</summary>
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
    FParser: TDextNatsFrameParser;
    FOptions: TDextNatsOptions;
    FHost: string;
    FPort: Word;
    FServerInfo: TNatsServerInfo;

    FLock: TCriticalSection;
    FSendLock: TCriticalSection;

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

    FRecvThread: TThread;
    FPingThread: TThread;

    FOnConnected: TNatsConnectedEvent;
    FOnDisconnected: TNatsDisconnectedEvent;
    FOnError: TNatsErrorEvent;

    function GetConnected: Boolean;
    function GetSubscriptionCount: Integer;
    function NextSid: Integer;

    procedure RecvLoop;
    procedure PingLoop;
    procedure InterruptibleSleep(AMilliseconds: Integer);

    procedure HandleFrame(const AFrame: TNatsFrame);
    procedure HandleMsgFrame(const AFrame: TNatsFrame);
    procedure HandlePongFrame;
    procedure HandleConnectionLost(const AReason: string);

    procedure DoHandshake;
    function TryReconnect: Boolean;
    procedure ResendSubscriptions;
    procedure FlushOutbox;

    function ReceiveFrameBlocking(ATimeoutMs: Integer): TNatsFrame;
    procedure SendRaw(const ABytes: TBytes);
    procedure DispatchOutgoing(const AData: TBytes);

    procedure FireError(const AMessage: string);
    procedure FireConnected(AIsReconnect: Boolean);
    procedure FireDisconnected;
  public
    constructor Create(const AOptions: TDextNatsOptions); overload;
    constructor Create; overload;
    destructor Destroy; override;

    /// <summary>Opens a TCP connection to a NATS server and performs the CONNECT handshake.
    /// Any subscriptions registered before this call are sent once the handshake completes.</summary>
    procedure Connect(const AHost: string = 'localhost'; APort: Word = NATS_DEFAULT_PORT);
    /// <summary>Gracefully closes the connection. Automatic reconnection is disabled until Connect is called again.</summary>
    procedure Disconnect;

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
    /// <summary>Non-blocking request/reply: AOnReply fires on the receive thread when a reply arrives;
    /// AOnTimeout (optional) fires from a helper thread if no reply arrives within ATimeoutMs.</summary>
    procedure RequestAsync(const ASubject: string; const APayload: TBytes; const AOnReply: TNatsMsgHandler;
      const AOnTimeout: TNatsRequestTimeoutHandler = nil; ATimeoutMs: Integer = 0);

    /// <summary>Generates a new process-unique inbox subject, suitable as a reply-to for request/reply patterns.</summary>
    function NewInbox: string;
    /// <summary>Round-trips a PING/PONG with the server, guaranteeing every command sent before this call
    /// has reached and been processed by the server. Raises EDextNatsTimeoutError on timeout.</summary>
    procedure Flush(ATimeoutMs: Integer = 0);
    /// <summary>Sends a bare PING without waiting for the PONG.</summary>
    procedure Ping;

    /// <summary>True while the socket is open and the CONNECT handshake has completed.</summary>
    property Connected: Boolean read GetConnected;
    /// <summary>Snapshot of the last INFO message received from the server.</summary>
    property ServerInfo: TNatsServerInfo read FServerInfo;
    property Options: TDextNatsOptions read FOptions write FOptions;
    property Host: string read FHost;
    property Port: Word read FPort;
    property SubscriptionCount: Integer read GetSubscriptionCount;

    /// <summary>Fires after a successful (re)connect, on the connecting thread.</summary>
    property OnConnected: TNatsConnectedEvent read FOnConnected write FOnConnected;
    /// <summary>Fires once per connection loss, on the receive thread, before any reconnect attempt.</summary>
    property OnDisconnected: TNatsDisconnectedEvent read FOnDisconnected write FOnDisconnected;
    /// <summary>Fires for server -ERR frames, unhandled handler exceptions, and reconnect failures.</summary>
    property OnError: TNatsErrorEvent read FOnError write FOnError;
  end;

implementation

{ TNatsMsg }

function TNatsMsg.AsString: string;
begin
  if Length(Payload) = 0 then
    Result := ''
  else
    Result := TEncoding.UTF8.GetString(Payload);
end;

function TNatsMsg.HasReplyTo: Boolean;
begin
  Result := ReplyTo <> '';
end;

{ TDextNatsOptions }

class function TDextNatsOptions.CreateDefault: TDextNatsOptions;
begin
  FillChar(Result, SizeOf(Result), 0);
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
    TMonitor.Leave(FGateLock);
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
  FParser := TDextNatsFrameParser.Create;
  FLock := TCriticalSection.Create;
  FSendLock := TCriticalSection.Create;
  FSubscriptions := TCollections.CreateDictionary<Integer, TDextNatsSubscription>(True);
  FPongWaiters := TCollections.CreateQueue<TEvent>;
  FPendingOutbox := TCollections.CreateQueue<TBytes>;
end;

destructor TDextNatsClient.Destroy;
begin
  Disconnect;
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

procedure TDextNatsClient.SendRaw(const ABytes: TBytes);
begin
  FSendLock.Enter;
  try
    FTcpClient.Send(ABytes);
  finally
    FSendLock.Leave;
  end;
end;

procedure TDextNatsClient.FireError(const AMessage: string);
begin
  if Assigned(FOnError) then
  try
    FOnError(AMessage);
  except
  end;
end;

procedure TDextNatsClient.FireConnected(AIsReconnect: Boolean);
begin
  if Assigned(FOnConnected) then
  try
    FOnConnected(FServerInfo, AIsReconnect);
  except
  end;
end;

procedure TDextNatsClient.FireDisconnected;
begin
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

    n := FTcpClient.Receive(buf, sliceTimeout);
    if n > 0 then
      FParser.Append(buf, n);
  end;
end;

procedure TDextNatsClient.DoHandshake;
var
  frame: TNatsFrame;
  connOpts: TNatsConnectOptions;
  sw: TStopwatch;
  remaining: Integer;
  gotPong: Boolean;
begin
  FParser.Clear;

  frame := ReceiveFrameBlocking(FOptions.ConnectTimeoutMs);
  if frame.Kind <> nfInfo then
    raise EDextNatsProtocolError.Create('Expected an INFO message as the first reply from the NATS server');

  FServerInfo := TNatsServerInfo.Parse(frame.InfoJson);

  if FServerInfo.TlsRequired then
    raise EDextNatsNotSupported.Create(
      'The NATS server requires a TLS connection; Dext.Net.Nats does not implement TLS yet');

  connOpts := TNatsConnectOptions.CreateDefault;
  connOpts.Verbose := FOptions.Verbose;
  connOpts.Pedantic := FOptions.Pedantic;
  connOpts.Echo := FOptions.Echo;
  connOpts.Name := FOptions.Name;
  connOpts.User := FOptions.User;
  connOpts.Password := FOptions.Password;
  connOpts.AuthToken := FOptions.AuthToken;
  connOpts.Headers := FServerInfo.HeadersSupported;

  SendRaw(NatsEncodeConnect(connOpts));
  SendRaw(NatsEncodePing);

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

procedure TDextNatsClient.Connect(const AHost: string; APort: Word);
begin
  if Connected then
    Exit;

  FHost := AHost;
  FPort := APort;
  FClosing := False;

  FTcpClient.Connect(AHost, APort);
  try
    DoHandshake;
  except
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

  FLock.Enter;
  try
    FConnected := False;
  finally
    FLock.Leave;
  end;
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
begin
  FLock.Enter;
  try
    for sub in FSubscriptions.Values do
    begin
      SendRaw(NatsEncodeSub(sub.Subject, sub.Queue, sub.Sid));
      if sub.MaxMsgs >= 0 then
        SendRaw(NatsEncodeUnsub(sub.Sid, sub.MaxMsgs - sub.Received));
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsClient.FlushOutbox;
var
  data: TBytes;
begin
  FLock.Enter;
  try
    while FPendingOutbox.Count > 0 do
    begin
      data := FPendingOutbox.Dequeue;
      Dec(FPendingOutboxBytes, Length(data));
      SendRaw(data);
    end;
  finally
    FLock.Leave;
  end;
end;

function TDextNatsClient.TryReconnect: Boolean;
var
  attempt: Integer;
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

    try
      FParser.Clear;
      FTcpClient.Connect(FHost, FPort);
      try
        DoHandshake;
      except
        FTcpClient.Disconnect;
        raise;
      end;

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
        FireError('NATS reconnect attempt ' + attempt.ToString + ' failed: ' + E.Message);
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

  FireDisconnected;

  if FClosing or not FOptions.AllowReconnect then
  begin
    FRunning := False;
    Exit;
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
    try
      n := FTcpClient.Receive(buf, 200);
    except
      on E: Exception do
      begin
        n := 0;
        if FRunning then
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
      SendRaw(NatsEncodePing);
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
    nfPing: SendRaw(NatsEncodePong);
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

  if Assigned(handler) then
  try
    handler(msg);
  except
    on E: Exception do
      FireError('Unhandled exception in NATS message handler for subject "' + msg.Subject + '": ' + E.Message);
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
var
  action: TDispatchAction;
begin
  FLock.Enter;
  try
    if FConnected then
      action := daSendNow
    else if FRunning and FOptions.AllowReconnect and not FClosing then
    begin
      if FPendingOutboxBytes + Length(AData) > FOptions.MaxPendingBufferBytes then
        action := daReject
      else
      begin
        FPendingOutbox.Enqueue(AData);
        Inc(FPendingOutboxBytes, Length(AData));
        action := daBuffered;
      end;
    end
    else
      action := daReject;
  finally
    FLock.Leave;
  end;

  case action of
    daSendNow: SendRaw(AData);
    daReject: raise EDextNatsException.Create(
      'Not connected to a NATS server (and no room left in the reconnect buffer)');
  end;
end;

procedure TDextNatsClient.Publish(const ASubject: string; const APayload: TBytes; const AReplyTo: string);
begin
  if ASubject = '' then
    raise EDextNatsException.Create('Publish requires a non-empty subject');
  DispatchOutgoing(NatsEncodePub(ASubject, AReplyTo, APayload));
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

  DispatchOutgoing(NatsEncodeHPub(ASubject, AReplyTo, AHeaders, APayload));
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
    FSubscriptions.Add(sub.Sid, sub);
    sendNow := FConnected;
  finally
    FLock.Leave;
  end;

  if sendNow then
    SendRaw(NatsEncodeSub(ASubject, AQueue, sub.Sid));

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
    SendRaw(NatsEncodeUnsub(ASid, AMaxMsgs));
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

  Result := reply;
end;

function TDextNatsClient.Request(const ASubject, AMessage: string; ATimeoutMs: Integer): TNatsMsg;
begin
  Result := Request(ASubject, TEncoding.UTF8.GetBytes(AMessage), ATimeoutMs);
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

procedure TDextNatsClient.Ping;
begin
  SendRaw(NatsEncodePing);
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

    SendRaw(NatsEncodePing);

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

end.
