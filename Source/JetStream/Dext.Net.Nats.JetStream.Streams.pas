{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream stream administration                                 }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Streams;

interface

uses
  Dext.Collections,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.JetStream.Transport;

type
  /// <summary>
  /// Extracted stream administration implementation. The public
  /// TDextNatsJetStreamContext remains the compatibility facade.
  /// </summary>
  TDextNatsJetStreamStreams = class
  private
    FTransport: INatsJetStreamApiTransport;
  public
    constructor Create(const ATransport: INatsJetStreamApiTransport);

    function CreateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
    function UpdateStream(const AConfig: TNatsStreamConfig): TNatsStreamInfo;
    function GetStreamInfo(const AStreamName: string): TNatsStreamInfo;
    function StreamExists(const AStreamName: string): Boolean;
    function DeleteStream(const AStreamName: string): Boolean;
    function PurgeStream(const AStreamName: string;
      const ARequest: TNatsStreamPurgeRequest): Boolean; overload;
    function PurgeStream(const AStreamName: string): Boolean; overload;
    function ListStreamNames(const ASubjectFilter: string = ''): IList<string>;
    function GetLastMessage(const AStreamName, ASubject: string;
      ATimeoutMs: Integer = 0): TNatsStoredMsg;
    function GetMessage(const AStreamName: string; ASequence: UInt64;
      ATimeoutMs: Integer = 0): TNatsStoredMsg;
  end;

implementation

uses
  System.SysUtils,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream.Json,
  Dext.Net.Nats.JetStream.Codecs,
  Dext.Net.Nats.JetStream.Parsers,
  Dext.Net.Nats.JetStream.Paging;

constructor TDextNatsJetStreamStreams.Create(
  const ATransport: INatsJetStreamApiTransport);
begin
  inherited Create;
  if ATransport = nil then
    raise EDextNatsException.Create('JetStream streams service requires a transport');
  FTransport := ATransport;
end;

function TDextNatsJetStreamStreams.CreateStream(
  const AConfig: TNatsStreamConfig): TNatsStreamInfo;
begin
  if AConfig.Name = '' then
    raise EDextNatsException.Create('CreateStream requires a stream name');
  Result := NatsJsParseStreamInfo(FTransport.Request(
    'STREAM.CREATE.' + AConfig.Name,
    NatsJsEncodeStreamConfig(AConfig)));
end;

function TDextNatsJetStreamStreams.UpdateStream(
  const AConfig: TNatsStreamConfig): TNatsStreamInfo;
begin
  if AConfig.Name = '' then
    raise EDextNatsException.Create('UpdateStream requires a stream name');
  Result := NatsJsParseStreamInfo(FTransport.Request(
    'STREAM.UPDATE.' + AConfig.Name,
    NatsJsEncodeStreamConfig(AConfig)));
end;

function TDextNatsJetStreamStreams.GetStreamInfo(
  const AStreamName: string): TNatsStreamInfo;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('GetStreamInfo requires a stream name');
  Result := NatsJsParseStreamInfo(FTransport.Request(
    'STREAM.INFO.' + AStreamName, '{}'));
end;

function TDextNatsJetStreamStreams.StreamExists(
  const AStreamName: string): Boolean;
begin
  try
    GetStreamInfo(AStreamName);
    Result := True;
  except
    on E: EDextNatsJetStreamError do
    begin
      if (E.ErrCode = 10059) or (E.Code = 404) then
        Result := False
      else
        raise;
    end;
  end;
end;

function TDextNatsJetStreamStreams.DeleteStream(
  const AStreamName: string): Boolean;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('DeleteStream requires a stream name');
  Result := NatsJsParseSuccess(FTransport.Request(
    'STREAM.DELETE.' + AStreamName, '{}'));
end;

function TDextNatsJetStreamStreams.PurgeStream(const AStreamName: string;
  const ARequest: TNatsStreamPurgeRequest): Boolean;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('PurgeStream requires a stream name');
  Result := NatsJsParseSuccess(FTransport.Request(
    'STREAM.PURGE.' + AStreamName,
    NatsJsEncodePurgeRequest(ARequest)));
end;

function TDextNatsJetStreamStreams.PurgeStream(
  const AStreamName: string): Boolean;
begin
  Result := PurgeStream(AStreamName, TNatsStreamPurgeRequest.CreateDefault);
end;

function TDextNatsJetStreamStreams.ListStreamNames(
  const ASubjectFilter: string): IList<string>;
var
  Offset, I: Integer;
  Page: TNatsJsNamePage;
begin
  Result := TCollections.CreateList<string>;
  Offset := 0;
  repeat
    Page := NatsJsParseNamePage(FTransport.Request('STREAM.NAMES',
      NatsJsBuildPagedListRequest(Offset, ASubjectFilter)), 'streams');
    for I := 0 to High(Page.Items) do
      Result.Add(Page.Items[I]);
    if Length(Page.Items) = 0 then
      Break;
    Inc(Offset, Length(Page.Items));
  until Offset >= Page.Total;
end;

function TDextNatsJetStreamStreams.GetLastMessage(const AStreamName,
  ASubject: string; ATimeoutMs: Integer): TNatsStoredMsg;
begin
  if (AStreamName = '') or (ASubject = '') then
    raise EDextNatsException.Create(
      'GetLastMessage requires stream name and subject');
  Result := NatsJsParseStoredMsg(FTransport.Request(
    'STREAM.MSG.GET.' + AStreamName,
    NatsJsBuildGetLastMessageRequest(ASubject), ATimeoutMs));
end;

function TDextNatsJetStreamStreams.GetMessage(const AStreamName: string;
  ASequence: UInt64; ATimeoutMs: Integer): TNatsStoredMsg;
begin
  if AStreamName = '' then
    raise EDextNatsException.Create('GetMessage requires a stream name');
  if ASequence = 0 then
    raise EDextNatsException.Create('GetMessage requires a non-zero sequence');
  Result := NatsJsParseStoredMsg(FTransport.Request(
    'STREAM.MSG.GET.' + AStreamName,
    NatsJsBuildGetMessageRequest(ASequence), ATimeoutMs));
end;

end.
