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
unit Demo.Form.KeyValue;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls,
  Vcl.ExtCtrls, Vcl.ComCtrls,
  Dext.Collections,
  Dext.Net.Nats,
  Dext.Net.Nats.JetStream,
  Dext.Net.Nats.KeyValue;

type
  TfrmKeyValue = class(TForm)
    sbMain: TStatusBar;
    mmLog: TMemo;
    grpBucket: TGroupBox;
    lblBucket: TLabel;
    edtBucket: TEdit;
    btnCreateBucket: TButton;
    btnOpenBucket: TButton;
    btnDeleteBucket: TButton;
    btnBucketExists: TButton;
    grpEntry: TGroupBox;
    lblKey: TLabel;
    edtKey: TEdit;
    lblValue: TLabel;
    edtValue: TEdit;
    lblRevision: TLabel;
    edtRevision: TEdit;
    btnPut: TButton;
    btnGet: TButton;
    btnDelete: TButton;
    btnCreateCas: TButton;
    btnUpdateCas: TButton;
    mmEntry: TMemo;
    grpKeys: TGroupBox;
    lstKeys: TListBox;
    btnRefreshKeys: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnCreateBucketClick(Sender: TObject);
    procedure btnOpenBucketClick(Sender: TObject);
    procedure btnDeleteBucketClick(Sender: TObject);
    procedure btnBucketExistsClick(Sender: TObject);
    procedure btnPutClick(Sender: TObject);
    procedure btnGetClick(Sender: TObject);
    procedure btnDeleteClick(Sender: TObject);
    procedure btnCreateCasClick(Sender: TObject);
    procedure btnUpdateCasClick(Sender: TObject);
    procedure btnRefreshKeysClick(Sender: TObject);
    procedure lstKeysClick(Sender: TObject);
  private
    FClient: TDextNatsClient;
    FJS: TDextNatsJetStreamContext;
    FKV: TDextNatsKeyValue;
    FConnectionLabel: string;
    procedure Log(const AMessage: string);
    procedure EnsureJS;
    procedure EnsureKV;
    procedure BindKV(AKV: TDextNatsKeyValue);
    procedure ShowEntry(const AEntry: TNatsKeyValueEntry);
    procedure RefreshKeys;
  public
    procedure SetExternalClient(AClient: TDextNatsClient; const AConnectionLabel: string = '');
  end;

implementation

{$R *.dfm}

procedure TfrmKeyValue.FormCreate(Sender: TObject);
begin
  FClient := nil;
  FJS := nil;
  FKV := nil;
  edtBucket.Text := 'vcl_kv';
  edtKey.Text := 'greeting';
  edtValue.Text := 'hello from Dext.Nats KV';
  edtRevision.Text := '0';
  Log('Key-Value form ready. Connect a tab, then open this form via Key-Value.');
end;

procedure TfrmKeyValue.SetExternalClient(AClient: TDextNatsClient; const AConnectionLabel: string);
begin
  FreeAndNil(FKV);
  FreeAndNil(FJS);
  FClient := AClient;
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
  end
  else
  begin
    sbMain.SimpleText := 'Client not connected';
    Log('External client is nil or not connected.');
  end;
end;

procedure TfrmKeyValue.FormDestroy(Sender: TObject);
begin
  FreeAndNil(FKV);
  FreeAndNil(FJS);
  FClient := nil;
end;

procedure TfrmKeyValue.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  Action := caFree;
end;

procedure TfrmKeyValue.Log(const AMessage: string);
begin
  TThread.Queue(nil,
    procedure
    begin
      mmLog.Lines.Add(FormatDateTime('[hh:nn:ss.zzz] ', Now) + AMessage);
      mmLog.SelStart := mmLog.GetTextLen;
      mmLog.SelLength := 0;
    end);
end;

procedure TfrmKeyValue.EnsureJS;
begin
  if not Assigned(FClient) or not FClient.Connected then
    raise Exception.Create('NATS client is not connected');
  if not Assigned(FJS) then
    FJS := TDextNatsJetStreamContext.Create(FClient);
end;

procedure TfrmKeyValue.EnsureKV;
begin
  EnsureJS;
  if not Assigned(FKV) then
    raise Exception.Create('Open or create a bucket first');
end;

procedure TfrmKeyValue.BindKV(AKV: TDextNatsKeyValue);
begin
  if (FKV <> nil) and (FKV <> AKV) then
    FreeAndNil(FKV);
  FKV := AKV;
  if Assigned(FKV) then
  begin
    edtBucket.Text := FKV.Bucket;
    sbMain.SimpleText := Format('Bucket %s via %s', [FKV.Bucket, FConnectionLabel]);
  end;
end;

procedure TfrmKeyValue.ShowEntry(const AEntry: TNatsKeyValueEntry);
var
  opName: string;
begin
  case AEntry.Operation of
    kvoPut: opName := 'PUT';
    kvoDelete: opName := 'DEL';
    kvoPurge: opName := 'PURGE';
  else
    opName := '?';
  end;
  mmEntry.Clear;
  mmEntry.Lines.Add('Bucket: ' + AEntry.Bucket);
  mmEntry.Lines.Add('Key: ' + AEntry.Key);
  mmEntry.Lines.Add('Revision: ' + AEntry.Revision.ToString);
  mmEntry.Lines.Add('Operation: ' + opName);
  mmEntry.Lines.Add('Value: ' + AEntry.AsString);
  edtRevision.Text := AEntry.Revision.ToString;
  edtValue.Text := AEntry.AsString;
end;

procedure TfrmKeyValue.RefreshKeys;
var
  keys: IList<string>;
  i: Integer;
begin
  EnsureKV;
  keys := FKV.Keys;
  lstKeys.Items.BeginUpdate;
  try
    lstKeys.Clear;
    for i := 0 to keys.Count - 1 do
      lstKeys.Items.Add(keys[i]);
  finally
    lstKeys.Items.EndUpdate;
  end;
  Log(Format('Keys: %d live key(s).', [keys.Count]));
end;

procedure TfrmKeyValue.btnCreateBucketClick(Sender: TObject);
var
  cfg: TNatsKeyValueConfig;
  bucket: string;
begin
  try
    EnsureJS;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    cfg := TNatsKeyValueConfig.CreateDefault(bucket);
    cfg.History := 5;
    BindKV(TDextNatsKeyValue.CreateBucket(FJS, cfg));
    Log('Created bucket "' + FKV.Bucket + '" (stream ' + FKV.StreamName + ').');
    RefreshKeys;
  except
    on E: Exception do
      Log('CreateBucket failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnOpenBucketClick(Sender: TObject);
var
  bucket: string;
begin
  try
    EnsureJS;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    BindKV(TDextNatsKeyValue.Open(FJS, bucket));
    Log('Opened bucket "' + FKV.Bucket + '".');
    RefreshKeys;
  except
    on E: Exception do
      Log('Open failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnDeleteBucketClick(Sender: TObject);
var
  bucket: string;
begin
  try
    EnsureJS;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    FreeAndNil(FKV);
    if TDextNatsKeyValue.DeleteBucket(FJS, bucket) then
    begin
      lstKeys.Clear;
      mmEntry.Clear;
      Log('Deleted bucket "' + bucket + '".');
    end
    else
      Log('DeleteBucket returned False for "' + bucket + '".');
  except
    on E: Exception do
      Log('DeleteBucket failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnBucketExistsClick(Sender: TObject);
var
  bucket: string;
begin
  try
    EnsureJS;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    if TDextNatsKeyValue.BucketExists(FJS, bucket) then
      Log('Bucket "' + bucket + '" exists.')
    else
      Log('Bucket "' + bucket + '" does not exist.');
  except
    on E: Exception do
      Log('BucketExists failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnPutClick(Sender: TObject);
var
  key: string;
  rev: UInt64;
begin
  try
    EnsureKV;
    key := Trim(edtKey.Text);
    if key = '' then
    begin
      Log('Key is required.');
      Exit;
    end;
    rev := FKV.Put(key, edtValue.Text);
    edtRevision.Text := rev.ToString;
    Log(Format('Put "%s" revision=%s', [key, rev.ToString]));
    RefreshKeys;
  except
    on E: Exception do
      Log('Put failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnGetClick(Sender: TObject);
var
  key: string;
  entry: TNatsKeyValueEntry;
begin
  try
    EnsureKV;
    key := Trim(edtKey.Text);
    if key = '' then
    begin
      Log('Key is required.');
      Exit;
    end;
    entry := FKV.Get(key);
    ShowEntry(entry);
    Log(Format('Get "%s" revision=%s', [key, entry.Revision.ToString]));
  except
    on E: Exception do
      Log('Get failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnDeleteClick(Sender: TObject);
var
  key: string;
begin
  try
    EnsureKV;
    key := Trim(edtKey.Text);
    if key = '' then
    begin
      Log('Key is required.');
      Exit;
    end;
    FKV.Delete(key);
    mmEntry.Clear;
    Log('Deleted key "' + key + '".');
    RefreshKeys;
  except
    on E: Exception do
      Log('Delete failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnCreateCasClick(Sender: TObject);
var
  key: string;
  rev: UInt64;
begin
  try
    EnsureKV;
    key := Trim(edtKey.Text);
    if key = '' then
    begin
      Log('Key is required.');
      Exit;
    end;
    rev := FKV.Create(key, edtValue.Text);
    edtRevision.Text := rev.ToString;
    Log(Format('Create (CAS) "%s" revision=%s', [key, rev.ToString]));
    RefreshKeys;
  except
    on E: Exception do
      Log('Create (CAS) failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnUpdateCasClick(Sender: TObject);
var
  key: string;
  expected, rev: UInt64;
begin
  try
    EnsureKV;
    key := Trim(edtKey.Text);
    if key = '' then
    begin
      Log('Key is required.');
      Exit;
    end;
    expected := StrToUInt64Def(Trim(edtRevision.Text), 0);
    if expected = 0 then
    begin
      Log('Revision is required for Update (use Get first).');
      Exit;
    end;
    rev := FKV.Update(key, edtValue.Text, expected);
    edtRevision.Text := rev.ToString;
    Log(Format('Update (CAS) "%s" %s -> %s', [key, expected.ToString, rev.ToString]));
    RefreshKeys;
  except
    on E: Exception do
      Log('Update (CAS) failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.btnRefreshKeysClick(Sender: TObject);
begin
  try
    RefreshKeys;
  except
    on E: Exception do
      Log('Keys failed: ' + E.Message);
  end;
end;

procedure TfrmKeyValue.lstKeysClick(Sender: TObject);
begin
  if lstKeys.ItemIndex >= 0 then
    edtKey.Text := lstKeys.Items[lstKeys.ItemIndex];
end;

end.
