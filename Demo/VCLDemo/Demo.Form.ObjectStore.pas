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
unit Demo.Form.ObjectStore;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls,
  Vcl.ExtCtrls, Vcl.ComCtrls,
  Dext.Collections,
  Dext.Net.Nats,
  Dext.Net.Nats.ObjectStore;

type
  TfrmObjectStore = class(TForm)
    sbMain: TStatusBar;
    mmLog: TMemo;
    grpStore: TGroupBox;
    lblBucket: TLabel;
    edtBucket: TEdit;
    btnCreateStore: TButton;
    btnOpenStore: TButton;
    btnDeleteStore: TButton;
    btnSeal: TButton;
    btnIsSealed: TButton;
    grpObject: TGroupBox;
    lblName: TLabel;
    edtName: TEdit;
    lblData: TLabel;
    mmData: TMemo;
    btnPut: TButton;
    btnGet: TButton;
    btnDelete: TButton;
    mmInfo: TMemo;
    grpList: TGroupBox;
    lstNames: TListBox;
    btnList: TButton;
    btnKeys: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnCreateStoreClick(Sender: TObject);
    procedure btnOpenStoreClick(Sender: TObject);
    procedure btnDeleteStoreClick(Sender: TObject);
    procedure btnSealClick(Sender: TObject);
    procedure btnIsSealedClick(Sender: TObject);
    procedure btnPutClick(Sender: TObject);
    procedure btnGetClick(Sender: TObject);
    procedure btnDeleteClick(Sender: TObject);
    procedure btnListClick(Sender: TObject);
    procedure btnKeysClick(Sender: TObject);
    procedure lstNamesClick(Sender: TObject);
  private
    FClient: TDextNatsClient;
    FObjCtx: TDextNatsObjectStoreContext;
    FStore: TDextNatsObjectStore;
    FConnectionLabel: string;
    procedure Log(const AMessage: string);
    procedure EnsureContext;
    procedure EnsureStore;
    procedure BindStore(AStore: TDextNatsObjectStore);
    procedure ShowInfo(const AInfo: TNatsObjectInfo);
    procedure FillNamesFromList;
    procedure FillNamesFromKeys;
  public
    procedure SetExternalClient(AClient: TDextNatsClient; const AConnectionLabel: string = '');
  end;

implementation

{$R *.dfm}

procedure TfrmObjectStore.FormCreate(Sender: TObject);
begin
  FClient := nil;
  FObjCtx := nil;
  FStore := nil;
  edtBucket.Text := 'vcl_obj';
  edtName.Text := 'readme.txt';
  mmData.Lines.Text := 'hello from Dext.Nats Object Store';
  Log('Object Store form ready. Connect a tab, then open this form via Object Store.');
end;

procedure TfrmObjectStore.SetExternalClient(AClient: TDextNatsClient; const AConnectionLabel: string);
begin
  FreeAndNil(FStore);
  FreeAndNil(FObjCtx);
  FClient := AClient;
  FConnectionLabel := AConnectionLabel;
  if Assigned(FClient) and FClient.Connected then
  begin
    FObjCtx := TDextNatsObjectStoreContext.Create(FClient);
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

procedure TfrmObjectStore.FormDestroy(Sender: TObject);
begin
  FreeAndNil(FStore);
  FreeAndNil(FObjCtx);
  FClient := nil;
end;

procedure TfrmObjectStore.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  Action := caFree;
end;

procedure TfrmObjectStore.Log(const AMessage: string);
begin
  TThread.Queue(nil,
    procedure
    begin
      mmLog.Lines.Add(FormatDateTime('[hh:nn:ss.zzz] ', Now) + AMessage);
      mmLog.SelStart := mmLog.GetTextLen;
      mmLog.SelLength := 0;
    end);
end;

procedure TfrmObjectStore.EnsureContext;
begin
  if not Assigned(FClient) or not FClient.Connected then
    raise Exception.Create('NATS client is not connected');
  if not Assigned(FObjCtx) then
    FObjCtx := TDextNatsObjectStoreContext.Create(FClient);
end;

procedure TfrmObjectStore.EnsureStore;
begin
  EnsureContext;
  if not Assigned(FStore) then
    raise Exception.Create('Open or create a store first');
end;

procedure TfrmObjectStore.BindStore(AStore: TDextNatsObjectStore);
begin
  if (FStore <> nil) and (FStore <> AStore) then
    FreeAndNil(FStore);
  FStore := AStore;
  if Assigned(FStore) then
  begin
    edtBucket.Text := FStore.Bucket;
    sbMain.SimpleText := Format('Store %s via %s', [FStore.Bucket, FConnectionLabel]);
  end;
end;

procedure TfrmObjectStore.ShowInfo(const AInfo: TNatsObjectInfo);
begin
  mmInfo.Clear;
  mmInfo.Lines.Add('Name: ' + AInfo.Name);
  mmInfo.Lines.Add('Bucket: ' + AInfo.Bucket);
  mmInfo.Lines.Add('Size: ' + AInfo.Size.ToString);
  mmInfo.Lines.Add('Chunks: ' + AInfo.Chunks.ToString);
  mmInfo.Lines.Add('Digest: ' + AInfo.Digest);
  mmInfo.Lines.Add('Deleted: ' + BoolToStr(AInfo.Deleted, True));
  if AInfo.Description <> '' then
    mmInfo.Lines.Add('Description: ' + AInfo.Description);
end;

procedure TfrmObjectStore.FillNamesFromList;
var
  items: IList<TNatsObjectInfo>;
  i: Integer;
begin
  EnsureStore;
  items := FStore.List;
  lstNames.Items.BeginUpdate;
  try
    lstNames.Clear;
    for i := 0 to items.Count - 1 do
      lstNames.Items.Add(items[i].Name);
  finally
    lstNames.Items.EndUpdate;
  end;
  Log(Format('List: %d object(s).', [items.Count]));
end;

procedure TfrmObjectStore.FillNamesFromKeys;
var
  keys: IList<string>;
  i: Integer;
begin
  EnsureStore;
  keys := FStore.Keys;
  lstNames.Items.BeginUpdate;
  try
    lstNames.Clear;
    for i := 0 to keys.Count - 1 do
      lstNames.Items.Add(keys[i]);
  finally
    lstNames.Items.EndUpdate;
  end;
  Log(Format('Keys: %d name(s).', [keys.Count]));
end;

procedure TfrmObjectStore.btnCreateStoreClick(Sender: TObject);
var
  cfg: TNatsObjectStoreConfig;
  bucket: string;
begin
  try
    EnsureContext;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
    BindStore(FObjCtx.CreateStore(cfg));
    Log('Created store "' + FStore.Bucket + '" (stream ' + FStore.StreamName + ').');
    FillNamesFromKeys;
  except
    on E: Exception do
      Log('CreateStore failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnOpenStoreClick(Sender: TObject);
var
  bucket: string;
begin
  try
    EnsureContext;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    BindStore(FObjCtx.OpenStore(bucket));
    Log('Opened store "' + FStore.Bucket + '".');
    FillNamesFromKeys;
  except
    on E: Exception do
      Log('OpenStore failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnDeleteStoreClick(Sender: TObject);
var
  bucket: string;
begin
  try
    EnsureContext;
    bucket := Trim(edtBucket.Text);
    if bucket = '' then
    begin
      Log('Bucket name is required.');
      Exit;
    end;
    FreeAndNil(FStore);
    FObjCtx.DeleteStore(bucket);
    lstNames.Clear;
    mmInfo.Clear;
    Log('Deleted store "' + bucket + '".');
  except
    on E: Exception do
      Log('DeleteStore failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnSealClick(Sender: TObject);
begin
  try
    EnsureStore;
    FStore.Seal;
    Log('Sealed store "' + FStore.Bucket + '".');
  except
    on E: Exception do
      Log('Seal failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnIsSealedClick(Sender: TObject);
begin
  try
    EnsureStore;
    if FStore.IsSealed then
      Log('Store "' + FStore.Bucket + '" is sealed.')
    else
      Log('Store "' + FStore.Bucket + '" is not sealed.');
  except
    on E: Exception do
      Log('IsSealed failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnPutClick(Sender: TObject);
var
  name: string;
  info: TNatsObjectInfo;
begin
  try
    EnsureStore;
    name := Trim(edtName.Text);
    if name = '' then
    begin
      Log('Object name is required.');
      Exit;
    end;
    info := FStore.Put(name, TEncoding.UTF8.GetBytes(mmData.Text));
    ShowInfo(info);
    Log(Format('Put "%s" size=%s chunks=%d', [info.Name, info.Size.ToString, info.Chunks]));
    FillNamesFromKeys;
  except
    on E: Exception do
      Log('Put failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnGetClick(Sender: TObject);
var
  name: string;
  info: TNatsObjectInfo;
  data: TBytes;
begin
  try
    EnsureStore;
    name := Trim(edtName.Text);
    if name = '' then
    begin
      Log('Object name is required.');
      Exit;
    end;
    data := FStore.Get(name, info);
    ShowInfo(info);
    mmData.Text := TEncoding.UTF8.GetString(data);
    Log(Format('Get "%s" size=%s', [info.Name, info.Size.ToString]));
  except
    on E: Exception do
      Log('Get failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnDeleteClick(Sender: TObject);
var
  name: string;
begin
  try
    EnsureStore;
    name := Trim(edtName.Text);
    if name = '' then
    begin
      Log('Object name is required.');
      Exit;
    end;
    FStore.Delete(name);
    mmInfo.Clear;
    Log('Deleted object "' + name + '".');
    FillNamesFromKeys;
  except
    on E: Exception do
      Log('Delete failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnListClick(Sender: TObject);
begin
  try
    FillNamesFromList;
  except
    on E: Exception do
      Log('List failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnKeysClick(Sender: TObject);
begin
  try
    FillNamesFromKeys;
  except
    on E: Exception do
      Log('Keys failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.lstNamesClick(Sender: TObject);
begin
  if lstNames.ItemIndex >= 0 then
    edtName.Text := lstNames.Items[lstNames.ItemIndex];
end;

end.
