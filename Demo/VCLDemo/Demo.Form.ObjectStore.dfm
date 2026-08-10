object frmObjectStore: TfrmObjectStore
  Left = 0
  Top = 0
  Caption = 'Dext.Nats Object Store'
  ClientHeight = 640
  ClientWidth = 820
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnClose = FormClose
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  TextHeight = 15
  object grpStore: TGroupBox
    Left = 12
    Top = 12
    Width = 796
    Height = 88
    Caption = ' Store '
    TabOrder = 0
    object lblBucket: TLabel
      Left = 12
      Top = 24
      Width = 38
      Height = 15
      Caption = 'Bucket'
    end
    object edtBucket: TEdit
      Left = 12
      Top = 44
      Width = 180
      Height = 23
      TabOrder = 0
    end
    object btnCreateStore: TButton
      Left = 204
      Top = 42
      Width = 90
      Height = 28
      Caption = 'Create'
      TabOrder = 1
      OnClick = btnCreateStoreClick
    end
    object btnOpenStore: TButton
      Left = 302
      Top = 42
      Width = 90
      Height = 28
      Caption = 'Open'
      TabOrder = 2
      OnClick = btnOpenStoreClick
    end
    object btnDeleteStore: TButton
      Left = 400
      Top = 42
      Width = 110
      Height = 28
      Caption = 'Delete Store'
      TabOrder = 3
      OnClick = btnDeleteStoreClick
    end
    object btnSeal: TButton
      Left = 522
      Top = 42
      Width = 90
      Height = 28
      Caption = 'Seal'
      TabOrder = 4
      OnClick = btnSealClick
    end
    object btnIsSealed: TButton
      Left = 620
      Top = 42
      Width = 90
      Height = 28
      Caption = 'Is Sealed?'
      TabOrder = 5
      OnClick = btnIsSealedClick
    end
  end
  object grpObject: TGroupBox
    Left = 12
    Top = 112
    Width = 520
    Height = 284
    Caption = ' Object '
    TabOrder = 1
    object lblName: TLabel
      Left = 12
      Top = 24
      Width = 33
      Height = 15
      Caption = 'Name'
    end
    object lblData: TLabel
      Left = 12
      Top = 72
      Width = 26
      Height = 15
      Caption = 'Data'
    end
    object edtName: TEdit
      Left = 12
      Top = 44
      Width = 360
      Height = 23
      TabOrder = 0
    end
    object mmData: TMemo
      Left = 12
      Top = 92
      Width = 490
      Height = 48
      ScrollBars = ssVertical
      TabOrder = 1
    end
    object btnPut: TButton
      Left = 12
      Top = 148
      Width = 80
      Height = 28
      Caption = 'Put'
      TabOrder = 2
      OnClick = btnPutClick
    end
    object btnGet: TButton
      Left = 100
      Top = 148
      Width = 80
      Height = 28
      Caption = 'Get'
      TabOrder = 3
      OnClick = btnGetClick
    end
    object btnGetInfo: TButton
      Left = 188
      Top = 148
      Width = 80
      Height = 28
      Caption = 'GetInfo'
      TabOrder = 4
      OnClick = btnGetInfoClick
    end
    object btnDelete: TButton
      Left = 276
      Top = 148
      Width = 80
      Height = 28
      Caption = 'Delete'
      TabOrder = 5
      OnClick = btnDeleteClick
    end
    object btnPutFile: TButton
      Left = 12
      Top = 184
      Width = 90
      Height = 28
      Caption = 'Put File'
      TabOrder = 6
      OnClick = btnPutFileClick
    end
    object btnGetFile: TButton
      Left = 110
      Top = 184
      Width = 90
      Height = 28
      Caption = 'Get File'
      TabOrder = 7
      OnClick = btnGetFileClick
    end
    object mmInfo: TMemo
      Left = 12
      Top = 220
      Width = 490
      Height = 52
      ReadOnly = True
      ScrollBars = ssVertical
      TabOrder = 8
    end
  end
  object grpList: TGroupBox
    Left = 548
    Top = 112
    Width = 260
    Height = 284
    Caption = ' Names '
    TabOrder = 2
    object lstNames: TListBox
      Left = 12
      Top = 24
      Width = 236
      Height = 212
      ItemHeight = 15
      TabOrder = 0
      OnClick = lstNamesClick
    end
    object btnList: TButton
      Left = 12
      Top = 244
      Width = 112
      Height = 28
      Caption = 'List'
      TabOrder = 1
      OnClick = btnListClick
    end
    object btnKeys: TButton
      Left = 136
      Top = 244
      Width = 112
      Height = 28
      Caption = 'Keys'
      TabOrder = 2
      OnClick = btnKeysClick
    end
  end
  object grpLink: TGroupBox
    Left = 12
    Top = 404
    Width = 796
    Height = 88
    Caption = ' Links '
    TabOrder = 3
    object lblLinkName: TLabel
      Left = 12
      Top = 20
      Width = 55
      Height = 15
      Caption = 'Link name'
    end
    object lblTargetObject: TLabel
      Left = 200
      Top = 20
      Width = 108
      Height = 15
      Caption = 'Target object name'
    end
    object lblTargetBucket: TLabel
      Left = 420
      Top = 20
      Width = 119
      Height = 15
      Caption = 'Target bucket (link)'
    end
    object edtLinkName: TEdit
      Left = 12
      Top = 40
      Width = 170
      Height = 23
      TabOrder = 0
      TextHint = 'alias.png'
    end
    object edtTargetObject: TEdit
      Left = 200
      Top = 40
      Width = 200
      Height = 23
      TabOrder = 1
      TextHint = 'readme.txt'
    end
    object edtTargetBucket: TEdit
      Left = 420
      Top = 40
      Width = 160
      Height = 23
      TabOrder = 2
      TextHint = 'other_bucket'
    end
    object btnAddLink: TButton
      Left = 596
      Top = 38
      Width = 90
      Height = 28
      Caption = 'AddLink'
      TabOrder = 3
      OnClick = btnAddLinkClick
    end
    object btnAddBucketLink: TButton
      Left = 694
      Top = 38
      Width = 90
      Height = 28
      Caption = 'AddBucket'
      TabOrder = 4
      OnClick = btnAddBucketLinkClick
    end
  end
  object mmLog: TMemo
    Left = 0
    Top = 504
    Width = 820
    Height = 116
    Align = alBottom
    Font.Charset = ANSI_CHARSET
    Font.Color = clWindowText
    Font.Height = -11
    Font.Name = 'Consolas'
    Font.Style = []
    ParentFont = False
    ReadOnly = True
    ScrollBars = ssBoth
    TabOrder = 4
    WordWrap = False
  end
  object sbMain: TStatusBar
    Left = 0
    Top = 620
    Width = 820
    Height = 20
    Panels = <>
    SimplePanel = True
  end
  object dlgOpenFile: TOpenDialog
    Filter = 'All files (*.*)|*.*'
    Options = [ofHideReadOnly, ofFileMustExist, ofEnableSizing]
    Title = 'Put File into Object Store'
    Left = 760
    Top = 160
  end
  object dlgSaveFile: TSaveDialog
    Filter = 'All files (*.*)|*.*'
    Options = [ofOverwritePrompt, ofHideReadOnly, ofEnableSizing]
    Title = 'Get File from Object Store'
    Left = 760
    Top = 208
  end
end.
