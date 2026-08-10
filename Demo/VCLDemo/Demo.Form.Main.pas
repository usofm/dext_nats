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
unit Demo.Form.Main;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls,
  Vcl.ExtCtrls, Vcl.ComCtrls,
  Demo.Form.Connection;

type
  TfrmMain = class(TForm)
    pnlClient: TPanel;
    Splitter1: TSplitter;
    memoLog: TMemo;
    pnlNetwork: TPanel;
    lstNetwork: TListBox;
    btnNewConnection: TButton;
    pgcConnections: TPageControl;
    tsAbout: TTabSheet;
    lblAboutTitle: TLabel;
    lblAboutBody: TLabel;
    procedure FormCreate(Sender: TObject);
    procedure btnNewConnectionClick(Sender: TObject);
  private
  public
  end;

var
  frmMain: TfrmMain;

implementation

{$R *.dfm}

procedure TfrmMain.FormCreate(Sender: TObject);
begin
  Randomize;
  Color := RGB(Random(200) + 40, Random(200) + 40, Random(200) + 40);
end;

procedure TfrmMain.btnNewConnectionClick(Sender: TObject);
var
  LConn: TfrmConnection;
  LTabSheet: TTabSheet;
  LCaption: string;
begin
  LCaption := 'Connection ' + pgcConnections.PageCount.ToString;
  LTabSheet := TTabSheet.Create(pgcConnections);
  LTabSheet.Caption := LCaption;
  LTabSheet.PageControl := pgcConnections;

  LConn := TfrmConnection.CreateAndShow(LCaption, LTabSheet, memoLog.Lines);
  lstNetwork.AddItem(LCaption, LConn);
  pgcConnections.ActivePage := LTabSheet;
end;

end.
