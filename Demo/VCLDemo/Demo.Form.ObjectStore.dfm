object frmObjectStore: TfrmObjectStore
  Left = 0
  Top = 0
  Caption = 'Dext.Nats Object Store'
  ClientHeight = 560
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
    Height = 248
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
    object btnDelete: TButton
      Left = 188
      Top = 148
      Width = 80
      Height = 28
      Caption = 'Delete'
      TabOrder = 4
      OnClick = btnDeleteClick
    end
    object mmInfo: TMemo
      Left = 12
      Top = 184
      Width = 490
      Height = 52
      ReadOnly = True
      ScrollBars = ssVertical
      TabOrder = 5
    end
  end
  object grpList: TGroupBox
    Left = 548
    Top = 112
    Width = 260
    Height = 248
    Caption = ' Names '
    TabOrder = 2
    object lstNames: TListBox
      Left = 12
      Top = 24
      Width = 236
      Height = 176
      ItemHeight = 15
      TabOrder = 0
      OnClick = lstNamesClick
    end
    object btnList: TButton
      Left = 12
      Top = 208
      Width = 112
      Height = 28
      Caption = 'List'
      TabOrder = 1
      OnClick = btnListClick
    end
    object btnKeys: TButton
      Left = 136
      Top = 208
      Width = 112
      Height = 28
      Caption = 'Keys'
      TabOrder = 2
      OnClick = btnKeysClick
    end
  end
  object mmLog: TMemo
    Left = 0
    Top = 372
    Width = 820
    Height = 168
    Align = alBottom
    Font.Charset = ANSI_CHARSET
    Font.Color = clWindowText
    Font.Height = -11
    Font.Name = 'Consolas'
    Font.Style = []
    ParentFont = False
    ReadOnly = True
    ScrollBars = ssBoth
    TabOrder = 3
    WordWrap = False
  end
  object sbMain: TStatusBar
    Left = 0
    Top = 540
    Width = 820
    Height = 20
    Panels = <>
    SimplePanel = True
  end
end.
