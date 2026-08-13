{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream pull-consumer fetch                                   }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Fetch;

interface

uses
  Dext.Collections,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

type
  TDextNatsJetStreamFetcher = class
  private
    FClient: TDextNatsClient;
    FApiPrefix: string;
  public
    constructor Create(AClient: TDextNatsClient;
      const AApiPrefix: string = '$JS.API.');
    /// <summary>
    ///   Pulls up to ABatch messages. The inbox collector completes inline on
    ///   RecvLoop (same as Request), so Fetch is safe from a one-worker
    ///   subscription/Services/KV/OS handler.
    /// </summary>
    function Fetch(const AStreamName, AConsumerName: string;
      ABatch: Integer = 1; AExpiresMs: Integer = 1000): IList<TNatsJsMsg>;
  end;

function NatsJsBuildFetchRequest(ABatch, AExpiresMs: Integer): string;
function NatsJsIsControlMessage(const AMsg: TNatsMsg): Boolean;

implementation

uses
  System.SysUtils,
  System.SyncObjs,
  Dext.Net.Nats.Protocol;

type
  // Delphi cracker class: keeps SubscribeCore protected from the public API while
  // allowing this protocol-extension unit to create a tiny inline completion SUB.
  TDextNatsClientFetchAccess = class(TDextNatsClient)
  public
    function SubscribeInline(const ASubject: string; const AHandler: TNatsMsgHandler): Integer;
  end;

  /// <summary>
  ///   Wait-state for a Fetch inbox. Lives as an interface captured by the
  ///   inline handler so RecvLoop can never Enter/SetEvent on a freed
  ///   TCriticalSection/TEvent (Request claim-gate lifetime).
  /// </summary>
  INatsFetchGate = interface
    ['{C4E8A91B-6D2F-4B7A-8E15-3F9C0A1B2D4E}']
    procedure Handle(const AMsg: TNatsMsg);
    procedure Stop;
    function WaitFor(ATimeoutMs: Cardinal): TWaitResult;
  end;

  TNatsFetchGate = class(TInterfacedObject, INatsFetchGate)
  private
    FLock: TCriticalSection;
    FDone: TEvent;
    FStopped: Boolean;
    FMessages: IList<TNatsJsMsg>;
    FReceivedCount: Integer;
    FBatch: Integer;
  public
    constructor Create(AMessages: IList<TNatsJsMsg>; ABatch: Integer);
    destructor Destroy; override;
    procedure Handle(const AMsg: TNatsMsg);
    procedure Stop;
    function WaitFor(ATimeoutMs: Cardinal): TWaitResult;
  end;

function TDextNatsClientFetchAccess.SubscribeInline(const ASubject: string;
  const AHandler: TNatsMsgHandler): Integer;
begin
  Result := SubscribeCore(ASubject, AHandler, '', True);
end;

function NatsJsBuildFetchRequest(ABatch, AExpiresMs: Integer): string;
var
  Batch, ExpiresMs: Integer;
  ExpiresNs: Int64;
begin
  Batch := ABatch;
  if Batch <= 0 then
    Batch := 1;
  ExpiresMs := AExpiresMs;
  if ExpiresMs < 0 then
    ExpiresMs := 0;
  ExpiresNs := Int64(ExpiresMs) * 1000000;
  if ExpiresNs > 0 then
    Result := Format('{"batch":%d,"expires":%d}', [Batch, ExpiresNs])
  else
    Result := Format('{"batch":%d}', [Batch]);
end;

function NatsJsIsControlMessage(const AMsg: TNatsMsg): Boolean;
begin
  { Match TDextNatsJetStreamContext's NatsJSIsFetchControl: 100 heartbeat/FC,
    404 no messages, 408 request timeout, 409 consumer conflicts, plus any
    other non-zero status with an empty payload. }
  case AMsg.StatusCode of
    100, 404, 408, 409:
      Result := True;
  else
    Result := (Length(AMsg.Payload) = 0) and (AMsg.StatusCode <> 0);
  end;
end;

constructor TNatsFetchGate.Create(AMessages: IList<TNatsJsMsg>; ABatch: Integer);
begin
  inherited Create;
  FMessages := AMessages;
  FBatch := ABatch;
  FLock := TCriticalSection.Create;
  FDone := TEvent.Create(nil, True, False, '');
end;

destructor TNatsFetchGate.Destroy;
begin
  FLock.Free;
  FDone.Free;
  inherited;
end;

procedure TNatsFetchGate.Handle(const AMsg: TNatsMsg);
var
  JsMsg: TNatsJsMsg;
  IsControl, Signal: Boolean;
begin
  IsControl := NatsJsIsControlMessage(AMsg);
  Signal := False;
  FLock.Enter;
  try
    if FStopped then
      Exit;
    if not IsControl then
    begin
      JsMsg := TNatsJsMsg.FromNatsMsg(AMsg);
      FMessages.Add(JsMsg);
      Inc(FReceivedCount);
    end;
    Signal := IsControl or (FReceivedCount >= FBatch);
  finally
    FLock.Leave;
  end;
  if Signal then
  begin
    FLock.Enter;
    try
      if not FStopped then
        FDone.SetEvent;
    finally
      FLock.Leave;
    end;
  end;
end;

procedure TNatsFetchGate.Stop;
begin
  FLock.Enter;
  try
    FStopped := True;
  finally
    FLock.Leave;
  end;
end;

function TNatsFetchGate.WaitFor(ATimeoutMs: Cardinal): TWaitResult;
begin
  Result := FDone.WaitFor(ATimeoutMs);
end;

constructor TDextNatsJetStreamFetcher.Create(AClient: TDextNatsClient;
  const AApiPrefix: string);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('JetStream fetcher requires a NATS client');
  FClient := AClient;
  FApiPrefix := AApiPrefix;
end;

function TDextNatsJetStreamFetcher.Fetch(const AStreamName,
  AConsumerName: string; ABatch, AExpiresMs: Integer): IList<TNatsJsMsg>;
var
  Batch, ExpiresMs, WaitMs: Integer;
  Inbox, NextSubject, RequestBody: string;
  Sid: Integer;
  Gate: INatsFetchGate;
  Messages: IList<TNatsJsMsg>;
  WaitResult: TWaitResult;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create('Fetch requires stream and consumer names');
  if not FClient.Connected then
    raise EDextNatsException.Create('Cannot Fetch: NATS client is not connected');

  Batch := ABatch;
  if Batch <= 0 then
    Batch := 1;
  ExpiresMs := AExpiresMs;
  if ExpiresMs < 0 then
    ExpiresMs := 0;
  WaitMs := ExpiresMs + 5000;
  if WaitMs < 1000 then
    WaitMs := 1000;

  Messages := TCollections.CreateList<TNatsJsMsg>;
  Result := Messages;
  Gate := TNatsFetchGate.Create(Messages, Batch);
  Sid := 0;
  try
    Inbox := FClient.NewInbox;
    // Fetch may itself be called from the sole application callback worker.
    // Its private inbox collector therefore completes inline on RecvLoop; only this
    // small internal collector is exempt from normal worker dispatch.
    Sid := TDextNatsClientFetchAccess(FClient).SubscribeInline(Inbox,
      procedure(const AMsg: TNatsMsg)
      begin
        Gate.Handle(AMsg);
      end);

    FClient.Unsubscribe(Sid, Batch + 5);
    NextSubject := Format('%sCONSUMER.MSG.NEXT.%s.%s',
      [FApiPrefix, AStreamName, AConsumerName]);
    RequestBody := NatsJsBuildFetchRequest(Batch, ExpiresMs);
    FClient.Publish(NextSubject, RequestBody, Inbox);

    WaitResult := Gate.WaitFor(Cardinal(WaitMs));
    case WaitResult of
      wrSignaled, wrTimeout: ;
    else
      raise EDextNatsException.Create(
        'Error waiting for JetStream Fetch response');
    end;
  finally
    // Stop under lock first so a late MSG cannot touch wait state after we
    // drop our Gate ref. Always UNSUB max_msgs=0 (remove interest) before
    // releasing Gate; MaxMsgs auto-unsub leaves the inbox SUB live on wrSignaled.
    Gate.Stop;
    if Sid <> 0 then
    try
      FClient.Unsubscribe(Sid, 0);
    except
    end;
  end;
end;

end.
