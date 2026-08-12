{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream ordered-consumer engine                               }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Ordered;

interface

uses
  System.SyncObjs,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Consumers,
  Dext.Net.Nats.JetStream.Push;

type
  TDextNatsOrderedConsumerEngine = class
  private
    FClient: TDextNatsClient;
    FConsumers: TDextNatsJetStreamConsumers;
    FPushService: TDextNatsJetStreamPush;
    FStreamName: string;
    FHandler: TNatsOrderedConsumerHandler;
    FOptions: TNatsOrderedConsumerOptions;
    FPush: TDextNatsJetStreamPushSubscription;
    FLock: TCriticalSection;
    FWake: TEvent;
    FMonitor: TThread;
    FConsumerName: string;
    FDeliverSubject: string;
    FSerial: Integer;
    FExpectedDseq: UInt64;
    FLastStreamSeq: UInt64;
    FLastConsumerSeq: UInt64;
    FIdleHeartbeatNs: Int64;
    FActive: Boolean;
    FStopping: Boolean;
    FResetPending: Boolean;
    FResetCount: Integer;
    procedure TouchActivity;
    procedure RequestReset;
    procedure FailTerminal(const AErrorMessage: string);
    function BuildConsumerConfig(ASerial: Integer; const ADeliver: string;
      ARecreate: Boolean; ALastStreamSeq: UInt64): TNatsConsumerConfig;
    procedure TeardownPushAndConsumer;
    procedure InstallDelivery(ASerial: Integer);
    function TryReset(AInitial: Boolean = False): Boolean;
    procedure HandleRawMsg(ASerial: Integer; const AMsg: TNatsMsg);
    procedure MonitorLoop;
    function GetActive: Boolean;
    function GetConsumerName: string;
    function GetLastStreamSequence: UInt64;
    function GetSerial: Integer;
    function GetResetCount: Integer;
  public
    constructor Create(AClient: TDextNatsClient;
      AConsumers: TDextNatsJetStreamConsumers;
      APushService: TDextNatsJetStreamPush;
      const AStreamName: string; AHandler: TNatsOrderedConsumerHandler;
      const AOptions: TNatsOrderedConsumerOptions);
    destructor Destroy; override;
    procedure Stop;
    property Active: Boolean read GetActive;
    property ConsumerName: string read GetConsumerName;
    property LastStreamSequence: UInt64 read GetLastStreamSequence;
    property Serial: Integer read GetSerial;
    property ResetCount: Integer read GetResetCount;
  end;

implementation

uses
  System.SysUtils,
  System.Classes,
  Dext.Net.Nats.JetStream.Fetch;

const
  NATS_JS_ORDERED_HB_NS = Int64(5) * 1000000000;
  NATS_JS_ORDERED_INACTIVE_NS = Int64(5) * 60 * 1000000000;
  NATS_JS_ORDERED_ACK_WAIT_NS = Int64(30) * 1000000000;
  NATS_JS_NS_PER_MS = 1000000;
  NATS_JS_ORDERED_HB_THRESH = 2;

function NewOrderedNuid: string;
var
  G: TGUID;
  S: string;
begin
  CreateGUID(G);
  S := GUIDToString(G);
  S := StringReplace(S, '{', '', [rfReplaceAll]);
  S := StringReplace(S, '}', '', [rfReplaceAll]);
  Result := LowerCase(StringReplace(S, '-', '', [rfReplaceAll]));
end;

constructor TDextNatsOrderedConsumerEngine.Create(AClient: TDextNatsClient;
  AConsumers: TDextNatsJetStreamConsumers; APushService: TDextNatsJetStreamPush;
  const AStreamName: string; AHandler: TNatsOrderedConsumerHandler;
  const AOptions: TNatsOrderedConsumerOptions);
var
  Prefix: string;
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('SubscribeOrdered requires a NATS client');
  if AConsumers = nil then
    raise EDextNatsException.Create('SubscribeOrdered requires consumer administration');
  if APushService = nil then
    raise EDextNatsException.Create('SubscribeOrdered requires push service');
  if AStreamName = '' then
    raise EDextNatsException.Create('SubscribeOrdered requires a stream name');
  if not Assigned(AHandler) then
    raise EDextNatsException.Create('SubscribeOrdered requires a message handler');
  if not AClient.Connected then
    raise EDextNatsException.Create('Cannot SubscribeOrdered: NATS client is not connected');

  FClient := AClient;
  FConsumers := AConsumers;
  FPushService := APushService;
  FStreamName := AStreamName;
  FHandler := AHandler;
  FOptions := AOptions;
  FLock := TCriticalSection.Create;
  FWake := TEvent.Create(nil, False, False, '');
  FExpectedDseq := 1;
  FIdleHeartbeatNs := FOptions.IdleHeartbeat;
  if FIdleHeartbeatNs <= 0 then
    FIdleHeartbeatNs := NATS_JS_ORDERED_HB_NS;
  Prefix := Trim(FOptions.NamePrefix);
  if Prefix = '' then
    Prefix := 'ord_' + NewOrderedNuid;
  FOptions.NamePrefix := Prefix;

  TouchActivity;
  if not TryReset(True) then
  begin
    FWake.Free;
    FLock.Free;
    raise EDextNatsException.Create(
      'SubscribeOrdered failed to create the initial consumer');
  end;

  FMonitor := TThread.CreateAnonymousThread(MonitorLoop);
  FMonitor.FreeOnTerminate := False;
  FMonitor.Start;
end;

destructor TDextNatsOrderedConsumerEngine.Destroy;
begin
  Stop;
  FWake.Free;
  FLock.Free;
  inherited;
end;

procedure TDextNatsOrderedConsumerEngine.TouchActivity;
begin
  FLock.Enter;
  try
    FLastActivityMs := TThread.GetTickCount64;
  finally
    FLock.Leave;
  end;
end;

procedure TDextNatsOrderedConsumerEngine.RequestReset;
begin
  FLock.Enter;
  try
    if FStopping or (not FActive) or FResetPending then
      Exit;
    FResetPending := True;
  finally
    FLock.Leave;
  end;
  FWake.SetEvent;
end;

function TDextNatsOrderedConsumerEngine.GetActive: Boolean;
begin
  FLock.Enter;
  try Result := FActive and (not FStopping); finally FLock.Leave; end;
end;

function TDextNatsOrderedConsumerEngine.GetConsumerName: string;
begin
  FLock.Enter;
  try Result := FConsumerName; finally FLock.Leave; end;
end;

function TDextNatsOrderedConsumerEngine.GetLastStreamSequence: UInt64;
begin
  FLock.Enter;
  try Result := FLastStreamSeq; finally FLock.Leave; end;
end;

function TDextNatsOrderedConsumerEngine.GetSerial: Integer;
begin
  FLock.Enter;
  try Result := FSerial; finally FLock.Leave; end;
end;

function TDextNatsOrderedConsumerEngine.GetResetCount: Integer;
begin
  FLock.Enter;
  try Result := FResetCount; finally FLock.Leave; end;
end;

procedure TDextNatsOrderedConsumerEngine.FailTerminal(const AErrorMessage: string);
var
  ErrorHandler: TNatsOrderedConsumerErrorHandler;
begin
  FLock.Enter;
  try
    FActive := False;
    ErrorHandler := FOptions.OnError;
  finally
    FLock.Leave;
  end;
  if Assigned(ErrorHandler) then
  try ErrorHandler(AErrorMessage); except end;
end;

function TDextNatsOrderedConsumerEngine.BuildConsumerConfig(ASerial: Integer;
  const ADeliver: string; ARecreate: Boolean;
  ALastStreamSeq: UInt64): TNatsConsumerConfig;
var
  Inactive: Int64;
  NextSeq: UInt64;
begin
  Result := TNatsConsumerConfig.CreateDefault;
  Result.Name := Format('%s_%d', [FOptions.NamePrefix, ASerial]);
  Result.FilterSubject := FOptions.FilterSubject;
  Result.DeliverSubject := ADeliver;
  Result.AckPolicy := apNone;
  Result.MaxAckPending := 0;
  Result.MaxDeliver := 1;
  Result.AckWait := NATS_JS_ORDERED_ACK_WAIT_NS;
  Result.FlowControl := True;
  Result.IdleHeartbeat := FIdleHeartbeatNs;
  Inactive := FOptions.InactiveThreshold;
  if Inactive <= 0 then Inactive := NATS_JS_ORDERED_INACTIVE_NS;
  Result.InactiveThreshold := Inactive;
  Result.MemoryStorage := True;
  Result.NumReplicas := 1;
  Result.HeadersOnly := FOptions.HeadersOnly;
  Result.ReplayPolicy := rpInstant;

  if ARecreate or (ALastStreamSeq > 0) then
  begin
    NextSeq := ALastStreamSeq + 1;
    if NextSeq = 0 then NextSeq := 1;
    Result.DeliverPolicy := dpByStartSequence;
    Result.OptStartSeq := NextSeq;
  end
  else
  begin
    Result.DeliverPolicy := FOptions.DeliverPolicy;
    if FOptions.DeliverPolicy = dpByStartSequence then
    begin
      if FOptions.OptStartSeq > 0 then Result.OptStartSeq := FOptions.OptStartSeq
      else Result.OptStartSeq := 1;
    end
    else if (FOptions.DeliverPolicy = dpLastPerSubject) and
      (Result.FilterSubject = '') then
      Result.FilterSubject := '>';
  end;
end;

procedure TDextNatsOrderedConsumerEngine.TeardownPushAndConsumer;
var
  Push: TDextNatsJetStreamPushSubscription;
  ConsumerName: string;
begin
  FLock.Enter;
  try
    Push := FPush;
    FPush := nil;
    ConsumerName := FConsumerName;
    FConsumerName := '';
    FDeliverSubject := '';
  finally
    FLock.Leave;
  end;
  if Push <> nil then
  try Push.Free; except end;
  if ConsumerName <> '' then
  try FConsumers.DeleteConsumer(FStreamName, ConsumerName); except end;
end;

procedure TDextNatsOrderedConsumerEngine.InstallDelivery(ASerial: Integer);
var
  Deliver: string;
  SelfRef: TDextNatsOrderedConsumerEngine;
begin
  Deliver := FClient.NewInbox;
  SelfRef := Self;
  FPush := FPushService.Subscribe(Deliver,
    procedure(const AMsg: TNatsJsMsg)
    begin
      { Push service already converted the wire message. Reconstruct a minimal
        NATS message path is intentionally avoided; ordered validation happens
        directly on the JetStream sequence fields. }
      SelfRef.FLock.Enter;
      try
        if SelfRef.FStopping or (ASerial <> SelfRef.FSerial) then Exit;
        SelfRef.FLastActivityMs := TThread.GetTickCount64;
        if AMsg.ConsumerSequence <> SelfRef.FExpectedDseq then
          SelfRef.FResetPending := True
        else
        begin
          SelfRef.FExpectedDseq := AMsg.ConsumerSequence + 1;
          SelfRef.FLastStreamSeq := AMsg.StreamSequence;
          SelfRef.FLastConsumerSeq := AMsg.ConsumerSequence;
        end;
      finally
        SelfRef.FLock.Leave;
      end;
      if SelfRef.FResetPending then SelfRef.FWake.SetEvent
      else if Assigned(SelfRef.FHandler) then SelfRef.FHandler(AMsg);
    end);
  FDeliverSubject := Deliver;
end;

function TDextNatsOrderedConsumerEngine.TryReset(AInitial: Boolean): Boolean;
var
  Serial: Integer;
  Config: TNatsConsumerConfig;
  Info: TNatsConsumerInfo;
  Recreate: Boolean;
  LastStreamSeq: UInt64;
  Attempts, MaxAttempts, DelayMs: Integer;
  LastError: string;
begin
  Result := False;
  FLock.Enter;
  try MaxAttempts := FOptions.MaxResetAttempts; finally FLock.Leave; end;
  if AInitial then MaxAttempts := 1 else if MaxAttempts = 0 then MaxAttempts := -1;
  Attempts := 0;
  DelayMs := 200;
  LastError := '';
  while not FStopping do
  begin
    TeardownPushAndConsumer;
    FLock.Enter;
    try
      Inc(FSerial);
      Serial := FSerial;
      FExpectedDseq := 1;
      FLastConsumerSeq := 0;
      LastStreamSeq := FLastStreamSeq;
      Recreate := LastStreamSeq > 0;
    finally FLock.Leave; end;
    try
      InstallDelivery(Serial);
      Config := BuildConsumerConfig(Serial, FDeliverSubject, Recreate, LastStreamSeq);
      Info := FConsumers.CreateConsumer(FStreamName, Config);
      FLock.Enter;
      try
        FConsumerName := Info.Name;
        if FConsumerName = '' then FConsumerName := Config.Name;
        FActive := True;
        FResetPending := False;
        if Recreate then Inc(FResetCount);
        FLastActivityMs := TThread.GetTickCount64;
      finally FLock.Leave; end;
      Exit(True);
    except
      on E: Exception do
      begin
        LastError := E.Message;
        Inc(Attempts);
        if (MaxAttempts > 0) and (Attempts >= MaxAttempts) then
        begin
          if not AInitial then
            FailTerminal(Format('Ordered consumer reset failed after %d attempts: %s',
              [Attempts, LastError]));
          Exit(False);
        end;
        Sleep(DelayMs);
        if DelayMs < 2000 then DelayMs := DelayMs * 2;
      end;
    end;
  end;
end;

procedure TDextNatsOrderedConsumerEngine.HandleRawMsg(ASerial: Integer;
  const AMsg: TNatsMsg);
begin
  { Retained as an explicit seam for the eventual facade cut-over. The V2 push
    service owns wire conversion; runtime parity tests will exercise this seam. }
  if NatsJsIsControlMessage(AMsg) then Exit;
end;

procedure TDextNatsOrderedConsumerEngine.MonitorLoop;
var
  WaitMs: Cardinal;
  HbMs, LastAct, NowMs: UInt64;
  DoReset, Stopping: Boolean;
begin
  HbMs := UInt64(FIdleHeartbeatNs div NATS_JS_NS_PER_MS);
  if HbMs < 1000 then HbMs := 1000;
  WaitMs := Cardinal(HbMs);
  if WaitMs > 5000 then WaitMs := 5000;
  while True do
  begin
    FWake.WaitFor(WaitMs);
    FLock.Enter;
    try
      Stopping := FStopping;
      DoReset := FResetPending and (not FStopping);
      LastAct := FLastActivityMs;
      NowMs := TThread.GetTickCount64;
      if (not DoReset) and FActive and (not FStopping) and
        ((NowMs - LastAct) >= (HbMs * NATS_JS_ORDERED_HB_THRESH)) then
      begin
        FResetPending := True;
        DoReset := True;
      end;
    finally FLock.Leave; end;
    if Stopping then Break;
    if DoReset then TryReset(False);
  end;
end;

procedure TDextNatsOrderedConsumerEngine.Stop;
var
  Monitor: TThread;
begin
  FLock.Enter;
  try
    if FStopping then Monitor := nil
    else
    begin
      FStopping := True;
      FActive := False;
      FResetPending := False;
      Monitor := FMonitor;
      FMonitor := nil;
    end;
  finally FLock.Leave; end;
  if FWake <> nil then FWake.SetEvent;
  if Monitor <> nil then
  begin
    Monitor.WaitFor;
    Monitor.Free;
  end;
  TeardownPushAndConsumer;
end;

end.
