object frmMain: TfrmMain
  Left = 0
  Top = 0
  Caption = 'Dext.Nats for Delphi'
  ClientHeight = 640
  ClientWidth = 1100
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnCreate = FormCreate
  TextHeight = 15
  object pnlClient: TPanel
    Left = 0
    Top = 0
    Width = 1100
    Height = 640
    Align = alClient
    BevelOuter = bvNone
    TabOrder = 0
    object Splitter1: TSplitter
      Left = 0
      Top = 420
      Width = 1100
      Height = 4
      Cursor = crVSplit
      Align = alBottom
      MinSize = 60
    end
    object memoLog: TMemo
      Left = 0
      Top = 424
      Width = 1100
      Height = 216
      Align = alBottom
      Font.Charset = ANSI_CHARSET
      Font.Color = clWindowText
      Font.Height = -12
      Font.Name = 'Consolas'
      Font.Style = []
      ParentFont = False
      ReadOnly = True
      ScrollBars = ssBoth
      TabOrder = 0
      WordWrap = False
    end
    object pnlNetwork: TPanel
      Left = 0
      Top = 0
      Width = 180
      Height = 420
      Align = alLeft
      BevelOuter = bvNone
      TabOrder = 1
      object lstNetwork: TListBox
        Left = 0
        Top = 48
        Width = 180
        Height = 372
        Align = alClient
        ItemHeight = 15
        TabOrder = 0
      end
      object btnNewConnection: TButton
        Left = 8
        Top = 12
        Width = 164
        Height = 28
        Caption = 'New Connection'
        TabOrder = 1
        OnClick = btnNewConnectionClick
      end
    end
    object pgcConnections: TPageControl
      Left = 180
      Top = 0
      Width = 920
      Height = 420
      ActivePage = tsAbout
      Align = alClient
      TabOrder = 2
      object tsAbout: TTabSheet
        Caption = 'About Dext.Nats'
        object lblAboutTitle: TLabel
          Left = 32
          Top = 40
          Width = 280
          Height = 29
          Caption = 'Dext.Nats VCL Demo'
          Font.Charset = DEFAULT_CHARSET
          Font.Color = clWindowText
          Font.Height = -24
          Font.Name = 'Segoe UI'
          Font.Style = [fsBold]
          ParentFont = False
        end
        object lblAboutBody: TLabel
          Left = 32
          Top = 88
          Width = 700
          Height = 160
          AutoSize = False
          Caption = 
            'Interactive desktop demo for TDextNatsClient.'#13#10#13#10'1. Click New Conn' +
            'ection'#13#10'2. Toggle the connection switch (default 127.0.0.1:4222)'#13#10 +
            '3. Use Publish / Subscribe / Request / Unsubscribe'#13#10'4. Open JetSt' +
            'ream for stream/consumer admin (needs nats-server -js)'#13#10#13#10'Messag' +
            'e handlers run on the receive thread; UI updates use TThread.Queue.'
          WordWrap = True
        end
      end
    end
  end
end.
