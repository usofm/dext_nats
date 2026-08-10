object frmKeyValue: TfrmKeyValue
  Left = 0
  Top = 0
  Caption = 'Dext.Nats Key-Value'
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
  object grpBucket: TGroupBox
    Left = 12
    Top = 12
    Width = 796
    Height = 88
    Caption = ' Bucket '
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
      Width = 200
      Height = 23
      TabOrder = 0
    end
    object btnCreateBucket: TButton
      Left = 228
      Top = 42
      Width = 100
      Height = 28
      Caption = 'Create'
      TabOrder = 1
      OnClick = btnCreateBucketClick
    end
    object btnOpenBucket: TButton
      Left = 336
      Top = 42
      Width = 100
      Height = 28
      Caption = 'Open'
      TabOrder = 2
      OnClick = btnOpenBucketClick
    end
    object btnBucketExists: TButton
      Left = 444
      Top = 42
      Width = 100
      Height = 28
      Caption = 'Exists?'
      TabOrder = 3
      OnClick = btnBucketExistsClick
    end
    object btnDeleteBucket: TButton
      Left = 552
      Top = 42
      Width = 120
      Height = 28
      Caption = 'Delete Bucket'
      TabOrder = 4
      OnClick = btnDeleteBucketClick
    end
  end
  object grpEntry: TGroupBox
    Left = 12
    Top = 112
    Width = 520
    Height = 248
    Caption = ' Entry '
    TabOrder = 1
    object lblKey: TLabel
      Left = 12
      Top = 24
      Width = 19
      Height = 15
      Caption = 'Key'
    end
    object lblValue: TLabel
      Left = 12
      Top = 72
      Width = 29
      Height = 15
      Caption = 'Value'
    end
    object lblRevision: TLabel
      Left = 280
      Top = 24
      Width = 45
      Height = 15
      Caption = 'Revision'
    end
    object edtKey: TEdit
      Left = 12
      Top = 44
      Width = 250
      Height = 23
      TabOrder = 0
    end
    object edtRevision: TEdit
      Left = 280
      Top = 44
      Width = 100
      Height = 23
      TabOrder = 1
    end
    object edtValue: TEdit
      Left = 12
      Top = 92
      Width = 490
      Height = 23
      TabOrder = 2
    end
    object btnPut: TButton
      Left = 12
      Top = 128
      Width = 80
      Height = 28
      Caption = 'Put'
      TabOrder = 3
      OnClick = btnPutClick
    end
    object btnGet: TButton
      Left = 100
      Top = 128
      Width = 80
      Height = 28
      Caption = 'Get'
      TabOrder = 4
      OnClick = btnGetClick
    end
    object btnDelete: TButton
      Left = 188
      Top = 128
      Width = 80
      Height = 28
      Caption = 'Delete'
      TabOrder = 5
      OnClick = btnDeleteClick
    end
    object btnCreateCas: TButton
      Left = 276
      Top = 128
      Width = 100
      Height = 28
      Caption = 'Create (CAS)'
      TabOrder = 6
      OnClick = btnCreateCasClick
    end
    object btnUpdateCas: TButton
      Left = 384
      Top = 128
      Width = 100
      Height = 28
      Caption = 'Update (CAS)'
      TabOrder = 7
      OnClick = btnUpdateCasClick
    end
    object mmEntry: TMemo
      Left = 12
      Top = 168
      Width = 490
      Height = 64
      ReadOnly = True
      ScrollBars = ssVertical
      TabOrder = 8
    end
  end
  object grpKeys: TGroupBox
    Left = 548
    Top = 112
    Width = 260
    Height = 248
    Caption = ' Keys '
    TabOrder = 2
    object lstKeys: TListBox
      Left = 12
      Top = 24
      Width = 236
      Height = 176
      ItemHeight = 15
      TabOrder = 0
      OnClick = lstKeysClick
    end
    object btnRefreshKeys: TButton
      Left = 12
      Top = 208
      Width = 236
      Height = 28
      Caption = 'Refresh Keys'
      TabOrder = 1
      OnClick = btnRefreshKeysClick
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
