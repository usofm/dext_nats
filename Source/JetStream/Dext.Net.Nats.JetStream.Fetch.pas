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
    function Fetch(const AStreamName, AConsumerName: string;
      ABatch: Integer = 1; AExpiresMs: Integer = 1000): IList<TNatsJsMsg>;
  end;

function NatsJsBuildFetchRequest(ABatch, AExpiresMs: Integer): string;
function NatsJsIsControlMessage(const AMsg: TNatsMsg): Boolean;

implementation

uses
  System.SysUtils,
  System.SyncObjs;

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
  { JetStream pull status/control frames are NATS status messages. Normal
    application messages have StatusCode = 0. This covers 100 heartbeat/FC,
    404 no messages, 408 request timeout and 409 consumer state conflicts. }
  Result := AMsg.StatusCode <> 0;
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
  Done: TEvent;
  Lock: TCriticalSection;
  Messages: IList<TNatsJsMsg>;
  ReceivedCount: Integer;
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
  ReceivedCount := 0;
  Done := TEvent.Create(nil, True, False, '');
  Lock := TCriticalSection.Create;
  try
    Inbox := FClient.NewInbox;
    Sid := FClient.Subscribe(Inbox,
      procedure(const AMsg: TNatsMsg)
      var
        JsMsg: TNatsJsMsg;
        IsControl, Signal: Boolean;
      begin
        IsControl := NatsJsIsControlMessage(AMsg);
        Signal := False;
        Lock.Enter;
        try
          if not IsControl then
          begin
            JsMsg := TNatsJsMsg.FromNatsMsg(AMsg);
            Messages.Add(JsMsg);
            Inc(ReceivedCount);
          end;
          Signal := IsControl or (ReceivedCount >= Batch);
        finally
          Lock.Leave;
        end;
        if Signal then
          Done.SetEvent;
      end);

    try
      FClient.Unsubscribe(Sid, Batch + 5);
      NextSubject := Format('%sCONSUMER.MSG.NEXT.%s.%s',
        [FApiPrefix, AStreamName, AConsumerName]);
      RequestBody := NatsJsBuildFetchRequest(Batch, ExpiresMs);
      FClient.Publish(NextSubject, RequestBody, Inbox);

      WaitResult := Done.WaitFor(Cardinal(WaitMs));
      case WaitResult of
        wrSignaled: ;
        wrTimeout:
          FClient.Unsubscribe(Sid, 0);
      else
        begin
          FClient.Unsubscribe(Sid, 0);
          raise EDextNatsException.Create(
            'Error waiting for JetStream Fetch response');
        end;
      end;
    except
      try
        FClient.Unsubscribe(Sid, 0);
      except
      end;
      raise;
    end;
  finally
    Lock.Free;
    Done.Free;
  end;
end;

end.
