object frmJetStream: TfrmJetStream
  Left = 0
  Top = 0
  Caption = 'Dext.Nats JetStream'
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
  object pcMain: TPageControl
    Left = 0
    Top = 0
    Width = 820
    Height = 360
    ActivePage = tsStreams
    Align = alClient
    TabOrder = 0
    object tsStreams: TTabSheet
      Caption = 'Streams'
      object grpStreamCreate: TGroupBox
        Left = 12
        Top = 12
        Width = 380
        Height = 140
        Caption = ' Create Stream '
        TabOrder = 0
        object lblStreamNameCreate: TLabel
          Left = 12
          Top = 24
          Width = 35
          Height = 15
          Caption = 'Name'
        end
        object lblStreamSubjectsCreate: TLabel
          Left = 12
          Top = 72
          Width = 120
          Height = 15
          Caption = 'Subjects (comma-sep)'
        end
        object edtStreamNameCreate: TEdit
          Left = 12
          Top = 44
          Width = 240
          Height = 23
          TabOrder = 0
        end
        object edtStreamSubjectsCreate: TEdit
          Left = 12
          Top = 92
          Width = 240
          Height = 23
          TabOrder = 1
        end
        object btnStreamCreate: TButton
          Left = 268
          Top = 92
          Width = 96
          Height = 28
          Caption = 'Create'
          TabOrder = 2
          OnClick = btnStreamCreateClick
        end
      end
      object grpStreamList: TGroupBox
        Left = 12
        Top = 164
        Width = 380
        Height = 148
        Caption = ' List Streams '
        TabOrder = 2
        object btnListStreams: TButton
          Left = 12
          Top = 24
          Width = 140
          Height = 28
          Caption = 'List Streams'
          TabOrder = 0
          OnClick = btnListStreamsClick
        end
        object lbStreams: TListBox
          Left = 12
          Top = 60
          Width = 352
          Height = 72
          ItemHeight = 15
          TabOrder = 1
          OnDblClick = lbStreamsDblClick
        end
      end
      object grpStreamDetails: TGroupBox
        Left = 408
        Top = 12
        Width = 380
        Height = 300
        Caption = ' Stream Details '
        TabOrder = 1
        object lblStreamNameDetail: TLabel
          Left = 12
          Top = 24
          Width = 35
          Height = 15
          Caption = 'Name'
        end
        object edtStreamNameDetail: TEdit
          Left = 12
          Top = 44
          Width = 200
          Height = 23
          TabOrder = 0
        end
        object btnStreamInfo: TButton
          Left = 224
          Top = 42
          Width = 70
          Height = 28
          Caption = 'Info'
          TabOrder = 1
          OnClick = btnStreamInfoClick
        end
        object btnStreamExists: TButton
          Left = 300
          Top = 42
          Width = 64
          Height = 28
          Caption = 'Exists?'
          TabOrder = 2
          OnClick = btnStreamExistsClick
        end
        object btnStreamDelete: TButton
          Left = 224
          Top = 76
          Width = 140
          Height = 28
          Caption = 'Delete Stream'
          TabOrder = 3
          OnClick = btnStreamDeleteClick
        end
        object mmStreamInfo: TMemo
          Left = 12
          Top = 116
          Width = 352
          Height = 168
          ReadOnly = True
          ScrollBars = ssVertical
          TabOrder = 4
        end
      end
    end
    object tsPublish: TTabSheet
      Caption = 'Publish'
      object grpPublish: TGroupBox
        Left = 12
        Top = 12
        Width = 776
        Height = 300
        Caption = ' JetStream Publish '
        TabOrder = 0
        object lblPublishSubject: TLabel
          Left = 12
          Top = 24
          Width = 39
          Height = 15
          Caption = 'Subject'
        end
        object lblPublishMessage: TLabel
          Left = 12
          Top = 72
          Width = 49
          Height = 15
          Caption = 'Message'
        end
        object lblPublishMsgId: TLabel
          Left = 12
          Top = 120
          Width = 100
          Height = 15
          Caption = 'Nats-Msg-Id (opt)'
        end
        object lblPublishExpectedStream: TLabel
          Left = 280
          Top = 120
          Width = 110
          Height = 15
          Caption = 'Expected Stream'
        end
        object edtPublishSubject: TEdit
          Left = 12
          Top = 44
          Width = 500
          Height = 23
          TabOrder = 0
        end
        object edtPublishMessage: TEdit
          Left = 12
          Top = 92
          Width = 500
          Height = 23
          TabOrder = 1
        end
        object edtPublishMsgId: TEdit
          Left = 12
          Top = 140
          Width = 240
          Height = 23
          TabOrder = 2
        end
        object edtPublishExpectedStream: TEdit
          Left = 280
          Top = 140
          Width = 232
          Height = 23
          TabOrder = 3
        end
        object btnPublish: TButton
          Left = 12
          Top = 180
          Width = 140
          Height = 28
          Caption = 'Publish'
          TabOrder = 4
          OnClick = btnPublishClick
        end
        object mmPubAck: TMemo
          Left = 12
          Top = 220
          Width = 748
          Height = 64
          ReadOnly = True
          ScrollBars = ssVertical
          TabOrder = 5
        end
      end
    end
    object tsConsumers: TTabSheet
      Caption = 'Consumers'
      object grpConsumerCreate: TGroupBox
        Left = 12
        Top = 12
        Width = 380
        Height = 200
        Caption = ' Create Pull Consumer '
        TabOrder = 0
        object lblConsumerStreamNameCreate: TLabel
          Left = 12
          Top = 24
          Width = 74
          Height = 15
          Caption = 'Stream Name'
        end
        object lblConsumerNameCreate: TLabel
          Left = 12
          Top = 72
          Width = 79
          Height = 15
          Caption = 'Durable Name'
        end
        object lblConsumerFilter: TLabel
          Left = 12
          Top = 120
          Width = 74
          Height = 15
          Caption = 'Filter Subject'
        end
        object edtConsumerStreamNameCreate: TEdit
          Left = 12
          Top = 44
          Width = 240
          Height = 23
          TabOrder = 0
        end
        object edtConsumerNameCreate: TEdit
          Left = 12
          Top = 92
          Width = 240
          Height = 23
          TabOrder = 1
        end
        object edtConsumerFilter: TEdit
          Left = 12
          Top = 140
          Width = 240
          Height = 23
          TabOrder = 2
        end
        object btnConsumerCreate: TButton
          Left = 268
          Top = 140
          Width = 96
          Height = 28
          Caption = 'Create'
          TabOrder = 3
          OnClick = btnConsumerCreateClick
        end
      end
      object grpConsumerDetails: TGroupBox
        Left = 408
        Top = 12
        Width = 380
        Height = 300
        Caption = ' Consumer Details '
        TabOrder = 1
        object lblConsumerStreamNameDetail: TLabel
          Left = 12
          Top = 24
          Width = 74
          Height = 15
          Caption = 'Stream Name'
        end
        object lblConsumerNameDetail: TLabel
          Left = 12
          Top = 72
          Width = 93
          Height = 15
          Caption = 'Consumer Name'
        end
        object edtConsumerStreamNameDetail: TEdit
          Left = 12
          Top = 44
          Width = 200
          Height = 23
          TabOrder = 0
        end
        object edtConsumerNameDetail: TEdit
          Left = 12
          Top = 92
          Width = 200
          Height = 23
          TabOrder = 1
        end
        object btnConsumerInfo: TButton
          Left = 224
          Top = 42
          Width = 68
          Height = 28
          Caption = 'Info'
          TabOrder = 2
          OnClick = btnConsumerInfoClick
        end
        object btnListConsumers: TButton
          Left = 298
          Top = 42
          Width = 68
          Height = 28
          Caption = 'List'
          TabOrder = 5
          OnClick = btnListConsumersClick
        end
        object btnConsumerDelete: TButton
          Left = 224
          Top = 90
          Width = 140
          Height = 28
          Caption = 'Delete Consumer'
          TabOrder = 3
          OnClick = btnConsumerDeleteClick
        end
        object lbConsumers: TListBox
          Left = 12
          Top = 124
          Width = 352
          Height = 56
          ItemHeight = 15
          TabOrder = 6
          OnDblClick = lbConsumersDblClick
        end
        object mmConsumerInfo: TMemo
          Left = 12
          Top = 188
          Width = 352
          Height = 96
          ReadOnly = True
          ScrollBars = ssVertical
          TabOrder = 4
        end
      end
    end
    object tsConsume: TTabSheet
      Caption = 'Consume'
      object grpConsume: TGroupBox
        Left = 12
        Top = 12
        Width = 776
        Height = 300
        Caption = ' Fetch / Ack '
        TabOrder = 0
        object lblConsumeStream: TLabel
          Left = 12
          Top = 24
          Width = 74
          Height = 15
          Caption = 'Stream Name'
        end
        object lblConsumeConsumer: TLabel
          Left = 220
          Top = 24
          Width = 93
          Height = 15
          Caption = 'Consumer Name'
        end
        object lblConsumeBatchSize: TLabel
          Left = 428
          Top = 24
          Width = 55
          Height = 15
          Caption = 'Batch Size'
        end
        object edtConsumeStream: TEdit
          Left = 12
          Top = 44
          Width = 190
          Height = 23
          TabOrder = 0
        end
        object edtConsumeConsumer: TEdit
          Left = 220
          Top = 44
          Width = 190
          Height = 23
          TabOrder = 1
        end
        object edtConsumeBatchSize: TEdit
          Left = 428
          Top = 44
          Width = 60
          Height = 23
          TabOrder = 2
        end
        object btnFetchMessages: TButton
          Left = 508
          Top = 42
          Width = 100
          Height = 28
          Caption = 'Fetch'
          TabOrder = 3
          OnClick = btnFetchMessagesClick
        end
        object btnAckAll: TButton
          Left = 620
          Top = 42
          Width = 100
          Height = 28
          Caption = 'Ack All'
          TabOrder = 4
          OnClick = btnAckAllClick
        end
        object mmMessages: TMemo
          Left = 12
          Top = 88
          Width = 748
          Height = 192
          ReadOnly = True
          ScrollBars = ssBoth
          TabOrder = 5
          WordWrap = False
        end
      end
    end
  end
  object mmLog: TMemo
    Left = 0
    Top = 360
    Width = 820
    Height = 180
    Align = alBottom
    Font.Charset = ANSI_CHARSET
    Font.Color = clWindowText
    Font.Height = -11
    Font.Name = 'Consolas'
    Font.Style = []
    ParentFont = False
    ReadOnly = True
    ScrollBars = ssBoth
    TabOrder = 1
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
