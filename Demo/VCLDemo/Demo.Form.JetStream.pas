{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License");}
{           you may not use this file except in compliance with the License.}
{           You may obtain a copy of the License at                         }
{                                                                           }
{               http://www.apache.org/licenses/LICENSE-2.0                  }
{                                                                           }
{***************************************************************************}
unit Demo.Form.JetStream;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  System.Generics.Collections, Vcl.Graphics, Vcl.Controls, Vcl.Forms,
  Vcl.Dialogs, Vcl.StdCtrls, Vcl.ComCtrls, Vcl.ExtCtrls,
  Dext.Collections,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream;

type
  TfrmJetStream = class(TForm)
    pcMain: TPageControl;
    tsStreams: TTabSheet;
    tsPublish: TTabSheet;
    tsConsumers: TTabSheet;
    tsConsume: TTabSheet;
    sbMain: TStatusBar;
    mmLog: TMemo;
    grpStreamCreate: TGroupBox;
    lblStreamNameCreate: TLabel;
    edtStreamNameCreate: TEdit;
    lblStreamSubjectsCreate: TLabel;
    edtStreamSubjectsCreate: TEdit;
    btnStreamCreate: TButton;
    grpStreamDetails: TGroupBox;
    mmStreamInfo: TMemo;
    btnStreamInfo: TButton;
    btnStreamDelete: TButton;
    edtStreamNameDetail: TEdit;
    lblStreamNameDetail: TLabel;
    btnStreamExists: TButton;
    grpPublish: TGroupBox;
    lblPublishSubject: TLabel;
    edtPublishSubject: TEdit;
    lblPublishMessage: TLabel;
    edtPublishMessage: TEdit;
    btnPublish: TButton;
    mmPubAck: TMemo;
    lblPublishMsgId: TLabel;
    edtPublishMsgId: TEdit;
    lblPublishExpectedStream: TLabel;
    edtPublishExpectedStream: TEdit;
    grpConsumerCreate: TGroupBox;
    lblConsumerStreamNameCreate: TLabel;
    edtConsumerStreamNameCreate: TEdit;
    lblConsumerNameCreate: TLabel;
    edtConsumerNameCreate: TEdit;
    lblConsumerFilter: TLabel;
    edtConsumerFilter: TEdit;
    btnConsumerCreate: TButton;
    grpConsumerDetails: TGroupBox;
    mmConsumerInfo: TMemo;
    btnConsumerInfo: TButton;
    btnConsumerDelete: TButton;
    edtConsumerNameDetail: TEdit;
    lblConsumerNameDetail: TLabel;
    edtConsumerStreamNameDetail: TEdit;
    lblConsumerStreamNameDetail: TLabel;
    grpConsume: TGroupBox;
    lblConsumeStream: TLabel;
    edtConsumeStream: TEdit;
    lblConsumeConsumer: TLabel;
    edtConsumeConsumer: TEdit;
    btnFetchMessages: TButton;
    mmMessages: TMemo;
    btnAckAll: TButton;
    lblConsumeBatchSize: TLabel;
    edtConsumeBatchSize: TEdit;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnStreamCreateClick(Sender: TObject);
    procedure btnStreamInfoClick(Sender: TObject);
    procedure btnStreamDeleteClick(Sender: TObject);
    procedure btnStreamExistsClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnConsumerCreateClick(Sender: TObject);
    procedure btnConsumerInfoClick(Sender: TObject);
    procedure btnConsumerDeleteClick(Sender: TObject);
    procedure btnFetchMessagesClick(Sender: TObject);
    procedure btnAckAllClick(Sender: TObject);
  private
    FClient: TDextNatsClient;
    FJS: TDextNatsJetStreamContext;
    FOwnsClient: Boolean;
    FConnectionLabel: string;
    FLastFetched: TList<TNatsJsMsg>;
    procedure Log(const AMessage: string);
    procedure EnsureJS;
    procedure ShowStreamInfo(const AInfo: TNatsStreamInfo);
    procedure ShowConsumerInfo(const AInfo: TNatsConsumerInfo);
    function SplitSubjects(const AText: string): TArray<string>;
  public
    procedure SetExternalClient(AClient: TDextNatsClient; const AConnectionLabel: string = '');
  end;

implementation

{$R *.dfm}

procedure TfrmJetStream.FormCreate(Sender: TObject);
begin
  FClient := nil;
  FJS := nil;
  FOwnsClient := False;
  FLastFetched := TList<TNatsJsMsg>.Create;
  edtStreamNameCreate.Text := 'DEMO_STREAM';
  edtStreamSubjectsCreate.Text := 'vcl.test.>';
  edtStreamNameDetail.Text := 'DEMO_STREAM';
  edtPublishSubject.Text := 'vcl.test.data';
  edtPublishExpectedStream.Text := 'DEMO_STREAM';
  edtPublishMessage.Text := 'hello from Dext.Nats VCL';
  edtConsumerStreamNameCreate.Text := 'DEMO_STREAM';
  edtConsumerNameCreate.Text := 'VCL_PULL_CONSUMER';
  edtConsumerFilter.Text := 'vcl.test.>';
  edtConsumerStreamNameDetail.Text := 'DEMO_STREAM';
  edtConsumerNameDetail.Text := 'VCL_PULL_CONSUMER';
  edtConsumeStream.Text := 'DEMO_STREAM';
  edtConsumeConsumer.Text := 'VCL_PULL_CONSUMER';
  edtConsumeBatchSize.Text := '5';
  Log('JetStream form ready. Use SetExternalClient from a connected tab, or this form expects an external client.');
end;

procedure TfrmJetStream.SetExternalClient(AClient: TDextNatsClient; const AConnectionLabel: string);
begin
  if FOwnsClient and Assigned(FClient) and (FClient <> AClient) then
    FreeAndNil(FClient);
  FreeAndNil(FJS);

  FClient := AClient;
  FOwnsClient := False;
  FConnectionLabel := AConnectionLabel;
  if Assigned(FClient) and FClient.Connected then
  begin
    FJS := TDextNatsJetStreamContext.Create(FClient);
    sbMain.SimpleText := 'Using connection: ' + AConnectionLabel;
    Log('Using external TDextNatsClient (' + AConnectionLabel + ').');
    if FClient.ServerInfo.Jetstream then
      Log('Server INFO reports jetstream=true.')
    else
      Log('WARNING: Server INFO reports jetstream=false. Start nats-server -js.');
    pcMain.ActivePage := tsStreams;
  end
  else
  begin
    sbMain.SimpleText := 'Client not connected';
    Log('External client is nil or not connected.');
  end;
end;

procedure TfrmJetStream.FormDestroy(Sender: TObject);
begin
  FreeAndNil(FJS);
  if FOwnsClient then
    FreeAndNil(FClient)
  else
    FClient := nil;
  FreeAndNil(FLastFetched);
end;

procedure TfrmJetStream.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  Action := caFree;
end;

procedure TfrmJetStream.Log(const AMessage: string);
begin
  TThread.Queue(nil,
    procedure
    begin
      mmLog.Lines.Add(FormatDateTime('[hh:nn:ss.zzz] ', Now) + AMessage);
      mmLog.SelStart := mmLog.GetTextLen;
      mmLog.SelLength := 0;
    end);
end;

procedure TfrmJetStream.EnsureJS;
begin
  if not Assigned(FClient) or not FClient.Connected then
    raise Exception.Create('NATS client is not connected');
  if not Assigned(FJS) then
    FJS := TDextNatsJetStreamContext.Create(FClient);
end;

function TfrmJetStream.SplitSubjects(const AText: string): TArray<string>;
var
  parts: TArray<string>;
  i, n: Integer;
  s: string;
begin
  parts := AText.Split([',', ';']);
  n := 0;
  SetLength(Result, Length(parts));
  for i := 0 to High(parts) do
  begin
    s := Trim(parts[i]);
    if s <> '' then
    begin
      Result[n] := s;
      Inc(n);
    end;
  end;
  SetLength(Result, n);
end;

procedure TfrmJetStream.ShowStreamInfo(const AInfo: TNatsStreamInfo);
begin
  mmStreamInfo.Clear;
  mmStreamInfo.Lines.Add('Name: ' + AInfo.Name);
  mmStreamInfo.Lines.Add('Messages: ' + AInfo.Messages.ToString);
  mmStreamInfo.Lines.Add('Bytes: ' + AInfo.Bytes.ToString);
  mmStreamInfo.Lines.Add('FirstSeq: ' + AInfo.FirstSeq.ToString);
  mmStreamInfo.Lines.Add('LastSeq: ' + AInfo.LastSeq.ToString);
  mmStreamInfo.Lines.Add('Consumers: ' + AInfo.ConsumerCount.ToString);
end;

procedure TfrmJetStream.ShowConsumerInfo(const AInfo: TNatsConsumerInfo);
begin
  mmConsumerInfo.Clear;
  mmConsumerInfo.Lines.Add('Stream: ' + AInfo.StreamName);
  mmConsumerInfo.Lines.Add('Name: ' + AInfo.Name);
  mmConsumerInfo.Lines.Add('Durable: ' + AInfo.DurableName);
  mmConsumerInfo.Lines.Add('Filter: ' + AInfo.FilterSubject);
  mmConsumerInfo.Lines.Add('DeliverSubject: ' + AInfo.DeliverSubject);
  mmConsumerInfo.Lines.Add('NumPending: ' + AInfo.NumPending.ToString);
  mmConsumerInfo.Lines.Add('NumAckPending: ' + AInfo.NumAckPending.ToString);
  mmConsumerInfo.Lines.Add('NumRedelivered: ' + AInfo.NumRedelivered.ToString);
end;

procedure TfrmJetStream.btnStreamCreateClick(Sender: TObject);
var
  cfg: TNatsStreamConfig;
  info: TNatsStreamInfo;
  subjects: TArray<string>;
begin
  try
    EnsureJS;
    subjects := SplitSubjects(edtStreamSubjectsCreate.Text);
    if (Trim(edtStreamNameCreate.Text) = '') or (Length(subjects) = 0) then
    begin
      Log('Stream name and subjects are required.');
      Exit;
    end;
    cfg := TNatsStreamConfig.CreateDefault(Trim(edtStreamNameCreate.Text), subjects);
    cfg.Storage := ssMemory;
    info := FJS.CreateStream(cfg);
    edtStreamNameDetail.Text := info.Name;
    ShowStreamInfo(info);
    Log('Created stream "' + info.Name + '".');
  except
    on E: Exception do
      Log('CreateStream failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnStreamInfoClick(Sender: TObject);
var
  info: TNatsStreamInfo;
  name: string;
begin
  try
    EnsureJS;
    name := Trim(edtStreamNameDetail.Text);
    if name = '' then
    begin
      Log('Enter a stream name.');
      Exit;
    end;
    info := FJS.GetStreamInfo(name);
    ShowStreamInfo(info);
    Log('GetStreamInfo OK for "' + name + '".');
  except
    on E: Exception do
      Log('GetStreamInfo failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnStreamExistsClick(Sender: TObject);
var
  name: string;
begin
  try
    EnsureJS;
    name := Trim(edtStreamNameDetail.Text);
    if name = '' then
    begin
      Log('Enter a stream name.');
      Exit;
    end;
    if FJS.StreamExists(name) then
      Log('Stream "' + name + '" exists.')
    else
      Log('Stream "' + name + '" does not exist.');
  except
    on E: Exception do
      Log('StreamExists failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnStreamDeleteClick(Sender: TObject);
var
  name: string;
begin
  try
    EnsureJS;
    name := Trim(edtStreamNameDetail.Text);
    if name = '' then
    begin
      Log('Enter a stream name.');
      Exit;
    end;
    if FJS.DeleteStream(name) then
    begin
      mmStreamInfo.Clear;
      Log('Deleted stream "' + name + '".');
    end
    else
      Log('DeleteStream returned False for "' + name + '".');
  except
    on E: Exception do
      Log('DeleteStream failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnPublishClick(Sender: TObject);
var
  opts: TNatsJetStreamPublishOptions;
  ack: TNatsPublishAck;
  subject, msgText: string;
begin
  try
    EnsureJS;
    subject := Trim(edtPublishSubject.Text);
    msgText := edtPublishMessage.Text;
    if subject = '' then
    begin
      Log('Publish subject is required.');
      Exit;
    end;
    opts := TNatsJetStreamPublishOptions.CreateDefault;
    opts.MsgId := Trim(edtPublishMsgId.Text);
    opts.ExpectedStream := Trim(edtPublishExpectedStream.Text);
    ack := FJS.Publish(subject, TEncoding.UTF8.GetBytes(msgText), opts);
    mmPubAck.Clear;
    mmPubAck.Lines.Add('Stream: ' + ack.Stream);
    mmPubAck.Lines.Add('Sequence: ' + ack.Sequence.ToString);
    mmPubAck.Lines.Add('Duplicate: ' + BoolToStr(ack.Duplicate, True));
    mmPubAck.Lines.Add('Domain: ' + ack.Domain);
    Log(Format('Published to %s seq=%s', [subject, ack.Sequence.ToString]));
  except
    on E: Exception do
      Log('JetStream Publish failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnConsumerCreateClick(Sender: TObject);
var
  cfg: TNatsConsumerConfig;
  info: TNatsConsumerInfo;
  streamName, durable, filter: string;
begin
  try
    EnsureJS;
    streamName := Trim(edtConsumerStreamNameCreate.Text);
    durable := Trim(edtConsumerNameCreate.Text);
    filter := Trim(edtConsumerFilter.Text);
    if (streamName = '') or (durable = '') then
    begin
      Log('Stream name and durable consumer name are required.');
      Exit;
    end;
    cfg := TNatsConsumerConfig.CreateDefault(durable, filter);
    info := FJS.CreateConsumer(streamName, cfg);
    edtConsumerStreamNameDetail.Text := info.StreamName;
    edtConsumerNameDetail.Text := info.Name;
    ShowConsumerInfo(info);
    Log('Created consumer "' + info.Name + '" on "' + info.StreamName + '".');
  except
    on E: Exception do
      Log('CreateConsumer failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnConsumerInfoClick(Sender: TObject);
var
  info: TNatsConsumerInfo;
  streamName, consumerName: string;
begin
  try
    EnsureJS;
    streamName := Trim(edtConsumerStreamNameDetail.Text);
    consumerName := Trim(edtConsumerNameDetail.Text);
    if (streamName = '') or (consumerName = '') then
    begin
      Log('Stream and consumer names are required.');
      Exit;
    end;
    info := FJS.GetConsumerInfo(streamName, consumerName);
    ShowConsumerInfo(info);
    Log('GetConsumerInfo OK.');
  except
    on E: Exception do
      Log('GetConsumerInfo failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnConsumerDeleteClick(Sender: TObject);
var
  streamName, consumerName: string;
begin
  try
    EnsureJS;
    streamName := Trim(edtConsumerStreamNameDetail.Text);
    consumerName := Trim(edtConsumerNameDetail.Text);
    if (streamName = '') or (consumerName = '') then
    begin
      Log('Stream and consumer names are required.');
      Exit;
    end;
    if FJS.DeleteConsumer(streamName, consumerName) then
    begin
      mmConsumerInfo.Clear;
      Log('Deleted consumer "' + consumerName + '".');
    end
    else
      Log('DeleteConsumer returned False.');
  except
    on E: Exception do
      Log('DeleteConsumer failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnFetchMessagesClick(Sender: TObject);
var
  msgs: IList<TNatsJsMsg>;
  streamName, consumerName: string;
  batch, i: Integer;
  msg: TNatsJsMsg;
begin
  try
    EnsureJS;
    streamName := Trim(edtConsumeStream.Text);
    consumerName := Trim(edtConsumeConsumer.Text);
    batch := StrToIntDef(Trim(edtConsumeBatchSize.Text), 1);
    if (streamName = '') or (consumerName = '') then
    begin
      Log('Stream and consumer names are required for Fetch.');
      Exit;
    end;
    msgs := FJS.Fetch(streamName, consumerName, batch, 3000);
    FLastFetched.Clear;
    mmMessages.Clear;
    for i := 0 to msgs.Count - 1 do
    begin
      msg := msgs[i];
      FLastFetched.Add(msg);
      mmMessages.Lines.Add(Format('[%d] %s seq=%s pending=%d: %s',
        [i, msg.Subject, msg.StreamSequence.ToString, msg.NumPending, msg.AsString]));
    end;
    Log(Format('Fetch returned %d message(s).', [msgs.Count]));
  except
    on E: Exception do
      Log('Fetch failed: ' + E.Message);
  end;
end;

procedure TfrmJetStream.btnAckAllClick(Sender: TObject);
var
  i: Integer;
begin
  try
    EnsureJS;
    if FLastFetched.Count = 0 then
    begin
      Log('No fetched messages to ack.');
      Exit;
    end;
    for i := 0 to FLastFetched.Count - 1 do
      FJS.Ack(FLastFetched[i]);
    FClient.Flush(2000);
    Log(Format('Acked %d message(s).', [FLastFetched.Count]));
    FLastFetched.Clear;
  except
    on E: Exception do
      Log('Ack failed: ' + E.Message);
  end;
end;

end.
