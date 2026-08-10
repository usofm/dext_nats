object frmConnection: TfrmConnection
  Left = 0
  Top = 0
  Caption = 'frmConnection'
  ClientHeight = 400
  ClientWidth = 900
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  TextHeight = 15
  object grpServer: TGroupBox
    Left = 0
    Top = 0
    Width = 250
    Height = 400
    Align = alLeft
    Caption = ' Server '
    TabOrder = 0
    object lblServerInfo: TLabel
      Left = 12
      Top = 96
      Width = 58
      Height = 15
      Caption = 'Server Info'
    end
    object lblServerSettings: TLabel
      Left = 12
      Top = 48
      Width = 64
      Height = 15
      Caption = 'Host && Port'
    end
    object switchConnection: TToggleSwitch
      Left = 12
      Top = 20
      Width = 72
      Height = 20
      TabOrder = 0
      OnClick = switchConnectionClick
    end
    object lstServerInfo: TValueListEditor
      Left = 12
      Top = 116
      Width = 226
      Height = 268
      Anchors = [akLeft, akTop, akRight, akBottom]
      DefaultColWidth = 100
      Options = [goFixedVertLine, goFixedHorzLine, goVertLine, goHorzLine, goColSizing, goThumbTracking]
      Strings.Strings = (
        'Server ID='
        'Server Name='
        'Server Version='
        'Protocol='
        'Host='
        'Port='
        'Client ID='
        'Client IP='
        'JetStream='
        'Max Payload=')
      TabOrder = 1
      TitleCaptions.Strings = (
        'Prop'
        'Value')
      ColWidths = (
        100
        120)
    end
    object edtHost: TEdit
      Left = 12
      Top = 68
      Width = 150
      Height = 23
      TabOrder = 2
      Text = '127.0.0.1'
    end
    object edtPort: TEdit
      Left = 168
      Top = 68
      Width = 70
      Height = 23
      TabOrder = 3
      Text = '4222'
    end
  end
  object grpCommands: TGroupBox
    Left = 250
    Top = 0
    Width = 420
    Height = 400
    Align = alLeft
    Caption = ' Commands '
    TabOrder = 1
    object lblCommandList: TLabel
      Left = 12
      Top = 24
      Width = 81
      Height = 15
      Caption = 'Command List'
    end
    object lblCommandParams: TLabel
      Left = 12
      Top = 200
      Width = 120
      Height = 15
      Caption = 'Command Parameters'
    end
    object lblSubscriptionList: TLabel
      Left = 220
      Top = 24
      Width = 88
      Height = 15
      Caption = 'Subscription List'
    end
    object lstCommandList: TListBox
      Left = 12
      Top = 44
      Width = 190
      Height = 140
      ItemHeight = 15
      Items.Strings = (
        'Publish'
        'Request'
        'Subscribe'
        'Unsubscribe')
      TabOrder = 0
      OnClick = lstCommandListClick
    end
    object lstCommandParams: TValueListEditor
      Left = 12
      Top = 220
      Width = 396
      Height = 120
      Anchors = [akLeft, akTop, akRight]
      DefaultColWidth = 100
      Strings.Strings = (
        'Subject='
        'Message=')
      TabOrder = 1
      TitleCaptions.Strings = (
        'Prop'
        'Value')
      ColWidths = (
        100
        290)
    end
    object btnSend: TButton
      Left = 12
      Top = 356
      Width = 396
      Height = 28
      Anchors = [akLeft, akRight, akBottom]
      Caption = 'Send Command'
      TabOrder = 2
      OnClick = btnSendClick
    end
    object lstSubscriptions: TListBox
      Left = 220
      Top = 44
      Width = 188
      Height = 140
      ItemHeight = 15
      TabOrder = 3
    end
  end
  object grpConnection: TGroupBox
    Left = 670
    Top = 0
    Width = 230
    Height = 400
    Align = alClient
    Caption = ' Misc '
    TabOrder = 2
    object btnSimplePublish: TButton
      Left = 12
      Top = 32
      Width = 200
      Height = 28
      Caption = 'Simple Publish Code'
      TabOrder = 0
      OnClick = btnSimplePublishClick
    end
    object btnSimpleSubscribe: TButton
      Left = 12
      Top = 68
      Width = 200
      Height = 28
      Caption = 'Simple Subscribe Code'
      TabOrder = 1
      OnClick = btnSimpleSubscribeClick
    end
    object btnSimpleRequest: TButton
      Left = 12
      Top = 104
      Width = 200
      Height = 28
      Caption = 'Simple Request Code'
      TabOrder = 2
      OnClick = btnSimpleRequestClick
    end
    object btnSimpleUnsubscribe: TButton
      Left = 12
      Top = 140
      Width = 200
      Height = 28
      Caption = 'Simple Unsubscribe Code'
      TabOrder = 3
      OnClick = btnSimpleUnsubscribeClick
    end
    object btnJetStream: TButton
      Left = 12
      Top = 284
      Width = 200
      Height = 28
      Anchors = [akLeft, akRight, akBottom]
      Caption = 'JetStream'
      TabOrder = 4
      OnClick = btnJetStreamClick
    end
    object btnKeyValue: TButton
      Left = 12
      Top = 320
      Width = 200
      Height = 28
      Anchors = [akLeft, akRight, akBottom]
      Caption = 'Key-Value'
      TabOrder = 5
      OnClick = btnKeyValueClick
    end
    object btnObjectStore: TButton
      Left = 12
      Top = 356
      Width = 200
      Height = 28
      Anchors = [akLeft, akRight, akBottom]
      Caption = 'Object Store'
      TabOrder = 6
      OnClick = btnObjectStoreClick
    end
  end
end.
