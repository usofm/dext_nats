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
unit Demo.Form.Connection;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  System.Generics.Collections, Vcl.Graphics, Vcl.Controls, Vcl.Forms,
  Vcl.Dialogs, Vcl.StdCtrls, Vcl.Grids, Vcl.ValEdit, Vcl.WinXCtrls,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats;

type
  TfrmConnection = class(TForm)
    grpServer: TGroupBox;
    switchConnection: TToggleSwitch;
    lstServerInfo: TValueListEditor;
    lblServerInfo: TLabel;
    edtHost: TEdit;
    edtPort: TEdit;
    lblServerSettings: TLabel;
    grpCommands: TGroupBox;
    lstCommandList: TListBox;
    lblCommandList: TLabel;
    lstCommandParams: TValueListEditor;
    lblCommandParams: TLabel;
    btnSend: TButton;
    lstSubscriptions: TListBox;
    lblSubscriptionList: TLabel;
    grpConnection: TGroupBox;
    btnSimplePublish: TButton;
    btnSimpleSubscribe: TButton;
    btnSimpleRequest: TButton;
    btnSimpleUnsubscribe: TButton;
    btnJetStream: TButton;
    procedure btnJetStreamClick(Sender: TObject);
    procedure btnSimplePublishClick(Sender: TObject);
    procedure btnSendClick(Sender: TObject);
    procedure btnSimpleRequestClick(Sender: TObject);
    procedure btnSimpleSubscribeClick(Sender: TObject);
    procedure btnSimpleUnsubscribeClick(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure lstCommandListClick(Sender: TObject);
    procedure switchConnectionClick(Sender: TObject);
  private
    FLog: TStrings;
    FClient: TDextNatsClient;
    FConnectionName: string;
    FSubscriptions: TDictionary<Integer, string>;
    FUpdatingSwitch: Boolean;
    procedure Log(const AMessage: string);
    procedure LogFmt(const AMessage: string; const Args: array of const);
    procedure SetParams(const AParams: TArray<string>);
    procedure RefreshSubscriptionList;
    procedure ShowServerInfo(const AInfo: TNatsServerInfo);
    procedure HandleConnected(const AInfo: TNatsServerInfo; AIsReconnect: Boolean);
    procedure HandleDisconnected;
    procedure HandleError(const AErrorMessage: string);
    procedure EnsureClient;
    procedure SafeSetSwitch(AState: TToggleSwitchState);
    function MakeMsgHandler: TNatsMsgHandler;
  public
    class function CreateAndShow(const AName: string; AParent: TWinControl;
      ALog: TStrings): TfrmConnection;
    property Client: TDextNatsClient read FClient;
    property ConnectionName: string read FConnectionName;
  end;

implementation

{$R *.dfm}

uses
  Demo.Form.JetStream;

procedure TfrmConnection.FormCreate(Sender: TObject);
begin
  FSubscriptions := TDictionary<Integer, string>.Create;
  FUpdatingSwitch := False;
  SetParams(['Subject', 'Message']);
end;

procedure TfrmConnection.FormDestroy(Sender: TObject);
begin
  if Assigned(FClient) then
  begin
    try
      if FClient.Connected then
        FClient.Disconnect;
    except
      { ignore teardown errors }
    end;
    FreeAndNil(FClient);
  end;
  FreeAndNil(FSubscriptions);
end;

class function TfrmConnection.CreateAndShow(const AName: string; AParent: TWinControl;
  ALog: TStrings): TfrmConnection;
begin
  Result := TfrmConnection.Create(AParent);
  try
    Result.FConnectionName := AName;
    Result.FLog := ALog;
    Result.Top := 0;
    Result.Left := 0;
    Result.BorderStyle := bsNone;
    Result.Parent := AParent;
    Result.Align := alClient;
    Result.Show;
  except
    Result.Free;
    raise;
  end;
end;

procedure TfrmConnection.EnsureClient;
var
  opts: TDextNatsOptions;
begin
  if Assigned(FClient) then
    Exit;
  opts := TDextNatsOptions.CreateDefault;
  opts.Name := FConnectionName;
  opts.Host := Trim(edtHost.Text);
  opts.Port := Word(StrToIntDef(Trim(edtPort.Text), NATS_DEFAULT_PORT));
  FClient := TDextNatsClient.Create(opts);
  FClient.OnConnected := HandleConnected;
  FClient.OnDisconnected := HandleDisconnected;
  FClient.OnError := HandleError;
end;

procedure TfrmConnection.SafeSetSwitch(AState: TToggleSwitchState);
begin
  FUpdatingSwitch := True;
  try
    switchConnection.State := AState;
  finally
    FUpdatingSwitch := False;
  end;
end;

procedure TfrmConnection.Log(const AMessage: string);
var
  line: string;
begin
  if FLog = nil then
    Exit;
  line := Format('%s [%s]: %s', [
    FormatDateTime('hh:nn:ss.zzz', Now),
    FConnectionName,
    AMessage]);
  if TThread.CurrentThread.ThreadID = MainThreadID then
    FLog.Add(line)
  else
  begin
    TThread.Queue(nil,
      procedure
      begin
        if FLog <> nil then
          FLog.Add(line);
      end);
  end;
end;

procedure TfrmConnection.LogFmt(const AMessage: string; const Args: array of const);
begin
  Log(Format(AMessage, Args));
end;

procedure TfrmConnection.ShowServerInfo(const AInfo: TNatsServerInfo);
begin
  lstServerInfo.Strings.Values['Server ID'] := AInfo.ServerId;
  lstServerInfo.Strings.Values['Server Name'] := AInfo.ServerName;
  lstServerInfo.Strings.Values['Server Version'] := AInfo.Version;
  lstServerInfo.Strings.Values['Protocol'] := AInfo.Proto.ToString;
  lstServerInfo.Strings.Values['Host'] := AInfo.Host;
  lstServerInfo.Strings.Values['Port'] := AInfo.Port.ToString;
  lstServerInfo.Strings.Values['Client ID'] := AInfo.ClientId.ToString;
  lstServerInfo.Strings.Values['Client IP'] := AInfo.ClientIp;
  lstServerInfo.Strings.Values['JetStream'] := BoolToStr(AInfo.Jetstream, True);
  lstServerInfo.Strings.Values['Max Payload'] := AInfo.MaxPayload.ToString;
end;

procedure TfrmConnection.HandleConnected(const AInfo: TNatsServerInfo; AIsReconnect: Boolean);
var
  info: TNatsServerInfo;
  isReconnect: Boolean;
begin
  info := AInfo;
  isReconnect := AIsReconnect;
  TThread.Queue(nil,
    procedure
    begin
      if isReconnect then
        LogFmt('Reconnected to server %s (%s)', [info.ServerName, info.Version])
      else
        LogFmt('Connected to server %s (%s)', [info.ServerName, info.Version]);
      ShowServerInfo(info);
      SafeSetSwitch(tssOn);
    end);
end;

procedure TfrmConnection.HandleDisconnected;
begin
  TThread.Queue(nil,
    procedure
    begin
      Log('Disconnected');
      SafeSetSwitch(tssOff);
    end);
end;

procedure TfrmConnection.HandleError(const AErrorMessage: string);
var
  msg: string;
begin
  msg := AErrorMessage;
  TThread.Queue(nil,
    procedure
    begin
      Log('ERROR: ' + msg);
    end);
end;

procedure TfrmConnection.RefreshSubscriptionList;
var
  pair: TPair<Integer, string>;
begin
  lstSubscriptions.Clear;
  for pair in FSubscriptions do
    lstSubscriptions.Items.Add(Format('%s (%d)', [pair.Value, pair.Key]));
end;

procedure TfrmConnection.SetParams(const AParams: TArray<string>);
var
  LParam: string;
begin
  lstCommandParams.Strings.Clear;
  for LParam in AParams do
    lstCommandParams.Strings.AddPair(LParam, '');
end;

function TfrmConnection.MakeMsgHandler: TNatsMsgHandler;
begin
  Result :=
    procedure(const AMsg: TNatsMsg)
    var
      subject, payload, replyTo: string;
      client: TDextNatsClient;
    begin
      subject := AMsg.Subject;
      payload := AMsg.AsString;
      replyTo := AMsg.ReplyTo;
      client := FClient;
      if (replyTo <> '') and Assigned(client) and client.Connected then
        client.Publish(replyTo, 'Yes, I can help!');
      LogFmt('MSG <- %s: %s', [subject, payload]);
    end;
end;

procedure TfrmConnection.lstCommandListClick(Sender: TObject);
begin
  case lstCommandList.ItemIndex of
    0: SetParams(['Subject', 'Message']);
    1: SetParams(['Subject', 'Message*']);
    2: SetParams(['Subject', 'Queue*']);
    3: SetParams(['Id', 'Max*']);
  end;
end;

procedure TfrmConnection.btnSendClick(Sender: TObject);
var
  subject, messageText, queue, maxText: string;
  sid, maxMsgs: Integer;
  payload: TBytes;
  onReply: TNatsMsgHandler;
  onTimeout: TNatsRequestTimeoutHandler;
begin
  if not Assigned(FClient) or not FClient.Connected then
  begin
    Log('Not connected');
    Exit;
  end;

  case lstCommandList.ItemIndex of
    0: // Publish
      begin
        subject := Trim(lstCommandParams.Values['Subject']);
        messageText := lstCommandParams.Values['Message'];
        if subject = '' then
        begin
          Log('Subject is required');
          Exit;
        end;
        FClient.Publish(subject, messageText);
        LogFmt('PUB -> %s: %s', [subject, messageText]);
      end;
    1: // Request
      begin
        subject := Trim(lstCommandParams.Values['Subject']);
        messageText := lstCommandParams.Values['Message*'];
        if subject = '' then
        begin
          Log('Subject is required');
          Exit;
        end;
        payload := TEncoding.UTF8.GetBytes(messageText);
        LogFmt('REQ -> %s: %s', [subject, messageText]);
        onReply :=
          procedure(const AMsg: TNatsMsg)
          begin
            if AMsg.IsNoResponders then
              Log('RES <- no responders (503)')
            else
              LogFmt('RES <- (%s) %s: %s', [AMsg.ReplyTo, AMsg.Subject, AMsg.AsString]);
          end;
        onTimeout :=
          procedure
          begin
            Log('RES <- timeout');
          end;
        FClient.RequestAsync(subject, payload, onReply, onTimeout);
      end;
    2: // Subscribe
      begin
        subject := Trim(lstCommandParams.Values['Subject']);
        queue := Trim(lstCommandParams.Values['Queue*']);
        if subject = '' then
        begin
          Log('Subject is required');
          Exit;
        end;
        sid := FClient.Subscribe(subject, MakeMsgHandler(), queue);
        FSubscriptions.AddOrSetValue(sid, subject);
        if queue = '' then
          LogFmt('SUB %s sid=%d', [subject, sid])
        else
          LogFmt('SUB %s queue=%s sid=%d', [subject, queue, sid]);
      end;
    3: // Unsubscribe
      begin
        sid := StrToIntDef(Trim(lstCommandParams.Values['Id']), 0);
        maxText := Trim(lstCommandParams.Values['Max*']);
        if sid <= 0 then
        begin
          Log('Subscription Id is required');
          Exit;
        end;
        if maxText = '' then
        begin
          FClient.Unsubscribe(sid);
          FSubscriptions.Remove(sid);
          LogFmt('UNSUB sid=%d', [sid]);
        end
        else
        begin
          maxMsgs := StrToIntDef(maxText, 0);
          FClient.Unsubscribe(sid, maxMsgs);
          LogFmt('UNSUB sid=%d max=%d', [sid, maxMsgs]);
        end;
      end;
  else
    Log('Select a command first');
  end;
  RefreshSubscriptionList;
end;

procedure TfrmConnection.btnSimplePublishClick(Sender: TObject);
begin
  if not Assigned(FClient) or not FClient.Connected then
  begin
    Log('Not connected');
    Exit;
  end;
  FClient.Publish('mysubject', 'My Message');
  Log('PUB -> mysubject: My Message');
end;

procedure TfrmConnection.btnSimpleSubscribeClick(Sender: TObject);
var
  sid: Integer;
begin
  if not Assigned(FClient) or not FClient.Connected then
  begin
    Log('Not connected');
    Exit;
  end;
  sid := FClient.Subscribe('mysubject', MakeMsgHandler());
  FSubscriptions.AddOrSetValue(sid, 'mysubject');
  RefreshSubscriptionList;
  LogFmt('SUB mysubject sid=%d', [sid]);
end;

procedure TfrmConnection.btnSimpleRequestClick(Sender: TObject);
var
  onReply: TNatsMsgHandler;
  onTimeout: TNatsRequestTimeoutHandler;
begin
  if not Assigned(FClient) or not FClient.Connected then
  begin
    Log('Not connected');
    Exit;
  end;
  Log('REQ -> mysubject');
  onReply :=
    procedure(const AMsg: TNatsMsg)
    begin
      if AMsg.IsNoResponders then
        Log('RES <- no responders (503)')
      else
        LogFmt('RES <- %s', [AMsg.AsString]);
    end;
  onTimeout :=
    procedure
    begin
      Log('RES <- timeout');
    end;
  FClient.RequestAsync('mysubject', TEncoding.UTF8.GetBytes(''), onReply, onTimeout);
end;

procedure TfrmConnection.btnSimpleUnsubscribeClick(Sender: TObject);
var
  pair: TPair<Integer, string>;
  toRemove: TArray<Integer>;
  i, n: Integer;
begin
  if not Assigned(FClient) then
  begin
    Log('Not connected');
    Exit;
  end;
  n := 0;
  SetLength(toRemove, FSubscriptions.Count);
  for pair in FSubscriptions do
    if SameText(pair.Value, 'mysubject') then
    begin
      toRemove[n] := pair.Key;
      Inc(n);
    end;
  SetLength(toRemove, n);
  for i := 0 to High(toRemove) do
  begin
    FClient.Unsubscribe(toRemove[i]);
    FSubscriptions.Remove(toRemove[i]);
    LogFmt('UNSUB sid=%d (mysubject)', [toRemove[i]]);
  end;
  if n = 0 then
    FClient.UnsubscribeSubject('mysubject');
  RefreshSubscriptionList;
end;

procedure TfrmConnection.btnJetStreamClick(Sender: TObject);
var
  JF: TfrmJetStream;
begin
  if not Assigned(FClient) or not FClient.Connected then
  begin
    Log('Connect before opening JetStream');
    Exit;
  end;
  JF := TfrmJetStream.Create(Application);
  JF.SetExternalClient(FClient, FConnectionName);
  JF.Show;
end;

procedure TfrmConnection.switchConnectionClick(Sender: TObject);
begin
  if FUpdatingSwitch then
    Exit;

  if switchConnection.State = tssOn then
  begin
    try
      EnsureClient;
      var opts := FClient.Options;
      opts.Name := FConnectionName;
      FClient.Options := opts;
      FClient.Connect(Trim(edtHost.Text), Word(StrToIntDef(Trim(edtPort.Text), NATS_DEFAULT_PORT)));
      ShowServerInfo(FClient.ServerInfo);
      LogFmt('Connected to %s:%d', [FClient.Host, FClient.Port]);
    except
      on E: Exception do
      begin
        Log('Connect failed: ' + E.Message);
        SafeSetSwitch(tssOff);
      end;
    end;
  end
  else
  begin
    if Assigned(FClient) and FClient.Connected then
    begin
      try
        FClient.Disconnect;
        Log('Disconnected by user');
      except
        on E: Exception do
          Log('Disconnect failed: ' + E.Message);
      end;
    end;
    FSubscriptions.Clear;
    RefreshSubscriptionList;
  end;
end;

end.
