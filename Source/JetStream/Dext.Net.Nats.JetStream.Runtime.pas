unit Dext.Net.Nats.JetStream.Runtime;

interface

uses
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Transport,
  Dext.Net.Nats.JetStream.Streams,
  Dext.Net.Nats.JetStream.Consumers,
  Dext.Net.Nats.JetStream.Fetch,
  Dext.Net.Nats.JetStream.Push,
  Dext.Net.Nats.JetStream.Ordered;

type
  TDextNatsJetStreamRuntime = class
  private
    FTransport: INatsJetStreamApiTransport;
    FStreams: TDextNatsJetStreamStreams;
    FConsumers: TDextNatsJetStreamConsumers;
    FFetcher: TDextNatsJetStreamFetcher;
    FPush: TDextNatsJetStreamPush;
    FClient: TDextNatsClient;
  public
    constructor Create(AClient: TDextNatsClient;
      const AApiPrefix: string = '$JS.API.');
    destructor Destroy; override;

    function NewOrdered(const AStreamName: string;
      AHandler: TNatsOrderedConsumerHandler;
      const AOptions: TNatsOrderedConsumerOptions): TDextNatsOrderedConsumerEngine;

    property Streams: TDextNatsJetStreamStreams read FStreams;
    property Consumers: TDextNatsJetStreamConsumers read FConsumers;
    property Fetcher: TDextNatsJetStreamFetcher read FFetcher;
    property Push: TDextNatsJetStreamPush read FPush;
    property Client: TDextNatsClient read FClient;
  end;

implementation

constructor TDextNatsJetStreamRuntime.Create(AClient: TDextNatsClient;
  const AApiPrefix: string);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('JetStream runtime requires a NATS client');
  FClient := AClient;
  FTransport := TDextNatsJetStreamApiTransport.Create(AClient, AApiPrefix);
  FStreams := TDextNatsJetStreamStreams.Create(FTransport);
  FConsumers := TDextNatsJetStreamConsumers.Create(FTransport);
  FFetcher := TDextNatsJetStreamFetcher.Create(AClient, AApiPrefix);
  FPush := TDextNatsJetStreamPush.Create(AClient, FConsumers);
end;

destructor TDextNatsJetStreamRuntime.Destroy;
begin
  FPush.Free;
  FFetcher.Free;
  FConsumers.Free;
  FStreams.Free;
  FTransport := nil;
  inherited;
end;

function TDextNatsJetStreamRuntime.NewOrdered(const AStreamName: string;
  AHandler: TNatsOrderedConsumerHandler;
  const AOptions: TNatsOrderedConsumerOptions): TDextNatsOrderedConsumerEngine;
begin
  Result := TDextNatsOrderedConsumerEngine.Create(FClient, FConsumers, FPush,
    AStreamName, AHandler, AOptions);
end;

end.
