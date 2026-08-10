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
    btnGetInfo: TButton;
    btnDelete: TButton;
    btnPutFile: TButton;
    btnGetFile: TButton;
    mmInfo: TMemo;
    grpList: TGroupBox;
    lstNames: TListBox;
    btnList: TButton;
    btnKeys: TButton;
    grpLink: TGroupBox;
    lblLinkName: TLabel;
    edtLinkName: TEdit;
    lblTargetObject: TLabel;
    edtTargetObject: TEdit;
    lblTargetBucket: TLabel;
    edtTargetBucket: TEdit;
    btnAddLink: TButton;
    btnAddBucketLink: TButton;
    dlgOpenFile: TOpenDialog;
    dlgSaveFile: TSaveDialog;
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
    procedure btnGetInfoClick(Sender: TObject);
    procedure btnDeleteClick(Sender: TObject);
    procedure btnPutFileClick(Sender: TObject);
    procedure btnGetFileClick(Sender: TObject);
    procedure btnListClick(Sender: TObject);
    procedure btnKeysClick(Sender: TObject);
    procedure btnAddLinkClick(Sender: TObject);
    procedure btnAddBucketLinkClick(Sender: TObject);
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
    procedure ShowInfo(const AInfo: TNatsObjectInfo; const ACaption: string = '';
      AAppend: Boolean = False);
    function FormatListName(const AInfo: TNatsObjectInfo): string;
    function ListItemObjectName(const ADisplay: string): string;
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
  edtLinkName.Text := 'alias.txt';
  edtTargetObject.Text := 'readme.txt';
  edtTargetBucket.Text := '';
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

procedure TfrmObjectStore.ShowInfo(const AInfo: TNatsObjectInfo; const ACaption: string;
  AAppend: Boolean);
begin
  if not AAppend then
    mmInfo.Clear;
  if ACaption <> '' then
    mmInfo.Lines.Add(ACaption);
  mmInfo.Lines.Add('Name: ' + AInfo.Name);
  mmInfo.Lines.Add('Bucket: ' + AInfo.Bucket);
  mmInfo.Lines.Add('Size: ' + AInfo.Size.ToString);
  mmInfo.Lines.Add('Chunks: ' + AInfo.Chunks.ToString);
  mmInfo.Lines.Add('Digest: ' + AInfo.Digest);
  mmInfo.Lines.Add('Deleted: ' + BoolToStr(AInfo.Deleted, True));
  mmInfo.Lines.Add('IsLink: ' + BoolToStr(AInfo.IsLink, True));
  if AInfo.IsLink then
  begin
    mmInfo.Lines.Add('IsBucketLink: ' + BoolToStr(AInfo.IsBucketLink, True));
    mmInfo.Lines.Add('Link.Bucket: ' + AInfo.Link.Bucket);
    mmInfo.Lines.Add('Link.Name: ' + AInfo.Link.Name);
  end;
  if AInfo.Description <> '' then
    mmInfo.Lines.Add('Description: ' + AInfo.Description);
end;

function TfrmObjectStore.FormatListName(const AInfo: TNatsObjectInfo): string;
begin
  if AInfo.IsBucketLink then
    Result := Format('%s  [bucket-link -> %s]', [AInfo.Name, AInfo.Link.Bucket])
  else if AInfo.IsLink then
    Result := Format('%s  [link -> %s/%s]', [AInfo.Name, AInfo.Link.Bucket, AInfo.Link.Name])
  else
    Result := AInfo.Name;
end;

function TfrmObjectStore.ListItemObjectName(const ADisplay: string): string;
var
  p: Integer;
begin
  p := Pos('  [', ADisplay);
  if p > 0 then
    Result := Copy(ADisplay, 1, p - 1)
  else
    Result := ADisplay;
end;

procedure TfrmObjectStore.FillNamesFromList;
var
  items: IList<TNatsObjectInfo>;
  i: Integer;
  linkCount: Integer;
begin
  EnsureStore;
  items := FStore.List;
  linkCount := 0;
  lstNames.Items.BeginUpdate;
  try
    lstNames.Clear;
    for i := 0 to items.Count - 1 do
    begin
      if items[i].IsLink then
        Inc(linkCount);
      lstNames.Items.Add(FormatListName(items[i]));
    end;
  finally
    lstNames.Items.EndUpdate;
  end;
  Log(Format('List: %d object(s), %d link(s).', [items.Count, linkCount]));
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
    Log(Format('Put "%s" size=%s chunks=%d IsLink=%s',
      [info.Name, info.Size.ToString, info.Chunks, BoolToStr(info.IsLink, True)]));
    FillNamesFromKeys;
  except
    on E: Exception do
      Log('Put failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnGetClick(Sender: TObject);
var
  name: string;
  linkInfo, resolved: TNatsObjectInfo;
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
    { Get follows object links; show link meta first when the name is a link. }
    linkInfo := FStore.GetInfo(name);
    data := FStore.Get(name, resolved);
    if linkInfo.IsLink then
    begin
      ShowInfo(linkInfo, 'Link meta (before follow):');
      mmInfo.Lines.Add('');
      ShowInfo(resolved, 'Resolved target (Get):', True);
      Log(Format('Get "%s" IsLink=True -> %s/%s; resolved "%s" size=%s',
        [name, linkInfo.Link.Bucket, linkInfo.Link.Name, resolved.Name, resolved.Size.ToString]));
    end
    else
    begin
      ShowInfo(resolved);
      Log(Format('Get "%s" size=%s IsLink=%s',
        [resolved.Name, resolved.Size.ToString, BoolToStr(resolved.IsLink, True)]));
    end;
    mmData.Text := TEncoding.UTF8.GetString(data);
  except
    on E: Exception do
      Log('Get failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnGetInfoClick(Sender: TObject);
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
    info := FStore.GetInfo(name);
    ShowInfo(info);
    if info.IsLink then
      Log(Format('GetInfo "%s" IsLink=True Link=%s/%s IsBucketLink=%s',
        [info.Name, info.Link.Bucket, info.Link.Name, BoolToStr(info.IsBucketLink, True)]))
    else
      Log(Format('GetInfo "%s" IsLink=False size=%s', [info.Name, info.Size.ToString]));
  except
    on E: Exception do
      Log('GetInfo failed: ' + E.Message);
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

procedure TfrmObjectStore.btnPutFileClick(Sender: TObject);
var
  name, fileName: string;
  info, streamInfo: TNatsObjectInfo;
  ms: TMemoryStream;
begin
  try
    EnsureStore;
    if not dlgOpenFile.Execute then
      Exit;
    fileName := dlgOpenFile.FileName;
    name := Trim(edtName.Text);
    if name = '' then
    begin
      info := FStore.PutFile(fileName);
      edtName.Text := info.Name;
    end
    else
      info := FStore.PutFile(name, fileName);
    ShowInfo(info);
    Log(Format('PutFile "%s" <- %s size=%s chunks=%d',
      [info.Name, fileName, info.Size.ToString, info.Chunks]));

    { Optional stream round-trip: Get(name, TStream) without loading full TBytes. }
    ms := TMemoryStream.Create;
    try
      streamInfo := FStore.Get(info.Name, ms);
      Log(Format('Stream Get("%s", TMemoryStream) wrote %d byte(s); digest=%s match=%s',
        [streamInfo.Name, ms.Size, streamInfo.Digest,
         BoolToStr(SameText(streamInfo.Digest, info.Digest), True)]));
    finally
      ms.Free;
    end;

    FillNamesFromKeys;
  except
    on E: Exception do
      Log('PutFile failed: ' + E.Message);
  end;
end;

procedure TfrmObjectStore.btnGetFileClick(Sender: TObject);
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
    dlgSaveFile.FileName := ExtractFileName(name);
    if not dlgSaveFile.Execute then
      Exit;
    info := FStore.GetFile(name, dlgSaveFile.FileName);
    ShowInfo(info);
    Log(Format('GetFile "%s" -> %s size=%s chunks=%d (streamed via Get to TFileStream)',
      [name, dlgSaveFile.FileName, info.Size.ToString, info.Chunks]));
  except
    on E: Exception do
      Log('GetFile failed: ' + E.Message);
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

procedure TfrmObjectStore.btnAddLinkClick(Sender: TObject);
var
  linkName, targetName, targetBucket: string;
  targetStore: TDextNatsObjectStore;
  target, info: TNatsObjectInfo;
begin
  targetStore := nil;
  try
    EnsureStore;
    linkName := Trim(edtLinkName.Text);
    targetName := Trim(edtTargetObject.Text);
    targetBucket := Trim(edtTargetBucket.Text);
    if linkName = '' then
    begin
      Log('Link name is required.');
      Exit;
    end;
    if targetName = '' then
    begin
      Log('Target object name is required for AddLink.');
      Exit;
    end;
    if (targetBucket = '') or SameText(targetBucket, FStore.Bucket) then
      target := FStore.GetInfo(targetName)
    else
    begin
      EnsureContext;
      targetStore := FObjCtx.OpenStore(targetBucket);
      target := targetStore.GetInfo(targetName);
    end;
    info := FStore.AddLink(linkName, target);
    ShowInfo(info);
    Log(Format('AddLink "%s" -> %s/%s IsLink=%s',
      [info.Name, info.Link.Bucket, info.Link.Name, BoolToStr(info.IsLink, True)]));
    edtName.Text := linkName;
    FillNamesFromList;
  except
    on E: Exception do
      Log('AddLink failed: ' + E.Message);
  end;
  targetStore.Free;
end;

procedure TfrmObjectStore.btnAddBucketLinkClick(Sender: TObject);
var
  linkName, targetBucket: string;
  targetStore: TDextNatsObjectStore;
  info: TNatsObjectInfo;
begin
  targetStore := nil;
  try
    EnsureStore;
    linkName := Trim(edtLinkName.Text);
    targetBucket := Trim(edtTargetBucket.Text);
    if linkName = '' then
    begin
      Log('Link name is required.');
      Exit;
    end;
    if targetBucket = '' then
    begin
      Log('Target bucket is required for AddBucketLink.');
      Exit;
    end;
    if SameText(targetBucket, FStore.Bucket) then
    begin
      Log('AddBucketLink target should be another store bucket.');
      Exit;
    end;
    EnsureContext;
    targetStore := FObjCtx.OpenStore(targetBucket);
    info := FStore.AddBucketLink(linkName, targetStore);
    ShowInfo(info);
    Log(Format('AddBucketLink "%s" -> bucket %s IsBucketLink=%s',
      [info.Name, info.Link.Bucket, BoolToStr(info.IsBucketLink, True)]));
    edtName.Text := linkName;
    FillNamesFromList;
  except
    on E: Exception do
      Log('AddBucketLink failed: ' + E.Message);
  end;
  targetStore.Free;
end;

procedure TfrmObjectStore.lstNamesClick(Sender: TObject);
var
  display, name: string;
begin
  if lstNames.ItemIndex < 0 then
    Exit;
  display := lstNames.Items[lstNames.ItemIndex];
  name := ListItemObjectName(display);
  edtName.Text := name;
  edtLinkName.Text := name;
end;

end.
