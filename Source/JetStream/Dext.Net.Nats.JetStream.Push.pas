{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream push subscriptions                                    }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Push;

interface

uses
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Consumers;

type
  TDextNatsJetStreamPush = class
  private
    FClient: TDextNatsClient;
    FConsumers: TDextNatsJetStreamConsumers;
  public
    constructor Create(AClient: TDextNatsClient;
      AConsumers: TDextNatsJetStreamConsumers);

    function Subscribe(const ADeliverSubject: string;
      AHandler: TNatsJsMsgHandler;
      const AQueueGroup: string = ''): TDextNatsJetStreamPushSubscription; overload;
    function Subscribe(const AStreamName, AConsumerName: string;
      AHandler: TNatsJsMsgHandler): TDextNatsJetStreamPushSubscription; overload;
  end;

implementation

uses
  System.SysUtils,
  Dext.Net.Nats.JetStream.Fetch;

constructor TDextNatsJetStreamPush.Create(AClient: TDextNatsClient;
  AConsumers: TDextNatsJetStreamConsumers);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('JetStream push service requires a NATS client');
  if AConsumers = nil then
    raise EDextNatsException.Create('JetStream push service requires consumer administration');
  FClient := AClient;
  FConsumers := AConsumers;
end;

function TDextNatsJetStreamPush.Subscribe(const ADeliverSubject: string;
  AHandler: TNatsJsMsgHandler;
  const AQueueGroup: string): TDextNatsJetStreamPushSubscription;
var
  Sid: Integer;
  Handler: TNatsJsMsgHandler;
begin
  if ADeliverSubject = '' then
    raise EDextNatsException.Create('SubscribePush requires a deliver subject');
  if not Assigned(AHandler) then
    raise EDextNatsException.Create('SubscribePush requires a message handler');
  if not FClient.Connected then
    raise EDextNatsException.Create('Cannot SubscribePush: NATS client is not connected');

  Handler := AHandler;
  Sid := FClient.Subscribe(ADeliverSubject,
    procedure(const AMsg: TNatsMsg)
    var
      JsMsg: TNatsJsMsg;
    begin
      if NatsJsIsControlMessage(AMsg) then
        Exit;
      JsMsg := TNatsJsMsg.FromNatsMsg(AMsg);
      Handler(JsMsg);
    end,
    AQueueGroup);

  Result := TDextNatsJetStreamPushSubscription.Create(
    FClient, Sid, ADeliverSubject);
end;

function TDextNatsJetStreamPush.Subscribe(const AStreamName,
  AConsumerName: string;
  AHandler: TNatsJsMsgHandler): TDextNatsJetStreamPushSubscription;
var
  Info: TNatsConsumerInfo;
begin
  if (AStreamName = '') or (AConsumerName = '') then
    raise EDextNatsException.Create(
      'SubscribePush requires stream and consumer names');

  Info := FConsumers.GetConsumerInfo(AStreamName, AConsumerName);
  if Info.DeliverSubject = '' then
    raise EDextNatsException.CreateFmt(
      'Consumer "%s" on stream "%s" has no deliver_subject (not a push consumer)',
      [AConsumerName, AStreamName]);

  Result := Subscribe(Info.DeliverSubject, AHandler, Info.DeliverGroup);
end;

end.
