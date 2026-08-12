unit Dext.Net.Nats.ObjectStore.Reader;

interface

uses
  System.Hash,
  Dext.Collections,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.ObjectStore;

type
  /// <summary>
  /// Forward-only lazy chunk reader engine extracted from TDextNatsObjectResult.
  /// The public stream class can delegate to this engine after compile validation.
  /// </summary>
  TDextNatsObjectReaderEngine = class
  private
    const CHUNK_FETCH_BATCH = 16;
  private
    FJs: TDextNatsJetStreamContext;
    FStreamName: string;
    FChunkSubject: string;
    FConsumerName: string;
    FInfo: TNatsObjectInfo;
    FHash: THashSHA2;
    FBatch: IList<TNatsJsMsg>;
    FBatchIndex: Integer;
    FChunkOffset: Integer;
    FChunksGot: Cardinal;
    FBytesRead: UInt64;
    FConsumerReady: Boolean;
    FClosed: Boolean;
    FEof: Boolean;
    FFailed: Boolean;
    procedure EnsureConsumer;
    procedure FetchMore;
    procedure CleanupConsumer;
    procedure FinalizeAtEof;
    procedure Fail(const AMessage: string);
  public
    constructor Create(AJs: TDextNatsJetStreamContext; const AStreamName,
      AChunkSubject: string; const AInfo: TNatsObjectInfo);
    destructor Destroy; override;
    function Read(var Buffer; Count: Longint): Longint;
    procedure Close;
    property Info: TNatsObjectInfo read FInfo;
    property Position: UInt64 read FBytesRead;
    property Eof: Boolean read FEof;
    property Failed: Boolean read FFailed;
  end;

implementation

uses
  System.SysUtils,
  Dext.Net.Nats.ObjectStore.Crypto;

constructor TDextNatsObjectReaderEngine.Create(AJs: TDextNatsJetStreamContext;
  const AStreamName, AChunkSubject: string; const AInfo: TNatsObjectInfo);
begin
  inherited Create;
  if AJs = nil then
    raise EDextNatsObjectStoreError.Create('Object Store reader requires JetStream');
  FJs := AJs;
  FStreamName := AStreamName;
  FChunkSubject := AChunkSubject;
  FInfo := AInfo;
  FHash := THashSHA2.Create(THashSHA2.TSHA2Version.SHA256);
end;

destructor TDextNatsObjectReaderEngine.Destroy;
begin
  Close;
  inherited;
end;

procedure TDextNatsObjectReaderEngine.Fail(const AMessage: string);
begin
  FFailed := True;
  FEof := True;
  CleanupConsumer;
  raise EDextNatsObjectStoreError.Create(AMessage);
end;

procedure TDextNatsObjectReaderEngine.CleanupConsumer;
begin
  if FConsumerReady and (FJs <> nil) and (FStreamName <> '') and
    (FConsumerName <> '') then
  try
    FJs.DeleteConsumer(FStreamName, FConsumerName);
  except
  end;
  FConsumerReady := False;
  FConsumerName := '';
  FBatch := nil;
  FBatchIndex := 0;
  FChunkOffset := 0;
end;

procedure TDextNatsObjectReaderEngine.Close;
begin
  if FClosed then Exit;
  FClosed := True;
  FEof := True;
  CleanupConsumer;
end;

procedure TDextNatsObjectReaderEngine.EnsureConsumer;
var
  Config: TNatsConsumerConfig;
  Info: TNatsConsumerInfo;
begin
  if FConsumerReady or (FInfo.Chunks = 0) then Exit;
  Config := TNatsConsumerConfig.CreateDefault;
  Config.Name := 'osget_' + NatsObjectNewNuid;
  Config.FilterSubject := FChunkSubject;
  Config.DeliverPolicy := dpAll;
  Config.AckPolicy := apNone;
  Config.MaxDeliver := 1;
  Info := FJs.CreateConsumer(FStreamName, Config);
  FConsumerName := Info.Name;
  FConsumerReady := True;
end;

procedure TDextNatsObjectReaderEngine.FetchMore;
var
  Remaining: Cardinal;
  Batch: Integer;
begin
  EnsureConsumer;
  if FChunksGot >= FInfo.Chunks then Exit;
  Remaining := FInfo.Chunks - FChunksGot;
  Batch := Integer(Remaining);
  if Batch > CHUNK_FETCH_BATCH then Batch := CHUNK_FETCH_BATCH;
  FBatch := FJs.Fetch(FStreamName, FConsumerName, Batch, 30000);
  FBatchIndex := 0;
  FChunkOffset := 0;
  if (FBatch = nil) or (FBatch.Count = 0) then
    Fail(Format('Object Store chunk fetch stalled for nuid %s: got %d of %d',
      [FInfo.Nuid, FChunksGot, FInfo.Chunks]));
  if FBatch.Count > Batch then
    Fail(Format('Object Store chunk batch overflow for nuid %s', [FInfo.Nuid]));
end;

procedure TDextNatsObjectReaderEngine.FinalizeAtEof;
var
  Digest: string;
begin
  if FEof then Exit;
  FEof := True;
  CleanupConsumer;
  Digest := NatsObjectDigestValue(FHash.HashAsBytes);
  if (FInfo.Digest <> '') and (Digest <> FInfo.Digest) then
    Fail(Format('Object Store digest mismatch for %s', [FInfo.Name]));
  if FBytesRead <> FInfo.Size then
    Fail(Format('Object Store size mismatch for %s: expected %d got %d',
      [FInfo.Name, FInfo.Size, FBytesRead]));
  if FChunksGot <> FInfo.Chunks then
    Fail(Format('Object Store chunk count mismatch for nuid %s: expected %d got %d',
      [FInfo.Nuid, FInfo.Chunks, FChunksGot]));
end;

function TDextNatsObjectReaderEngine.Read(var Buffer; Count: Longint): Longint;
var
  Dest: PByte;
  Avail, Take: Integer;
  Payload: TBytes;
begin
  if FClosed then Fail('Object Store reader is closed');
  if FFailed then Fail(Format('Object Store reader previously failed for %s', [FInfo.Name]));
  if Count <= 0 then Exit(0);
  if FEof then Exit(0);

  if FInfo.Chunks = 0 then
  begin
    FinalizeAtEof;
    Exit(0);
  end;

  Result := 0;
  Dest := @Buffer;
  while Result < Count do
  begin
    while (FBatch = nil) or (FBatchIndex >= FBatch.Count) do
    begin
      if FChunksGot >= FInfo.Chunks then
      begin
        FinalizeAtEof;
        Exit;
      end;
      FetchMore;
    end;

    Payload := FBatch[FBatchIndex].Payload;
    Avail := Length(Payload) - FChunkOffset;
    if Avail <= 0 then
    begin
      Inc(FChunksGot);
      Inc(FBatchIndex);
      FChunkOffset := 0;
      Continue;
    end;

    Take := Count - Result;
    if Take > Avail then Take := Avail;
    Move(Payload[FChunkOffset], Dest^, Take);
    FHash.Update(Payload[FChunkOffset], Cardinal(Take));
    Inc(FChunkOffset, Take);
    Inc(FBytesRead, UInt64(Take));
    Inc(Result, Take);
    Inc(Dest, Take);
    if FChunkOffset >= Length(Payload) then
    begin
      Inc(FChunksGot);
      Inc(FBatchIndex);
      FChunkOffset := 0;
    end;
  end;

  if (not FEof) and (FChunksGot >= FInfo.Chunks) then FinalizeAtEof;
end;

end.
