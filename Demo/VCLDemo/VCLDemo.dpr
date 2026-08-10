{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           A native NATS client library for the Dext Framework            }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License");}
{           you may not use this file except in compliance with the License.}
{           You may obtain a copy of the License at                         }
{                                                                           }
{               http://www.apache.org/licenses/LICENSE-2.0                  }
{                                                                           }
{           Unless required by applicable law or agreed to in writing,      }
{           software distributed under the License is distributed on an     }
{           "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,    }
{           either express or implied. See the License for the specific     }
{           language governing permissions and limitations under the        }
{                                                                           }
{***************************************************************************}
{                                                                           }
{  VCL desktop demo for TDextNatsClient: multi-connection tabs,            }
{  publish / subscribe / request-reply, shared log, JetStream helper,      }
{  Key-Value and Object Store forms.                                       }
{                                                                           }
{  Modeled after nats.delphi.marmot Demos/VCLDemo.                         }
{                                                                           }
{  REQUIRES a running NATS server, e.g.:                                   }
{                                                                           }
{      nats-server                                                         }
{      nats-server -js   (JetStream / KV / Object Store forms)             }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                        }
{                                                                           }
{      msbuild Demo\VCLDemo\VCLDemo.dproj /p:Config=Debug /p:Platform=Win32}
{                                                                           }
{  Run:                                                                    }
{                                                                           }
{      Output\Win32\Debug\VCLDemo.exe                                      }
{                                                                           }
{***************************************************************************}
program VCLDemo;

uses
  Vcl.Forms,
  Demo.Form.Main in 'Demo.Form.Main.pas' {frmMain},
  Demo.Form.Connection in 'Demo.Form.Connection.pas' {frmConnection},
  Demo.Form.JetStream in 'Demo.Form.JetStream.pas' {frmJetStream},
  Demo.Form.KeyValue in 'Demo.Form.KeyValue.pas' {frmKeyValue},
  Demo.Form.ObjectStore in 'Demo.Form.ObjectStore.pas' {frmObjectStore};

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.Title := 'Dext.Nats VCL Demo';
  Application.CreateForm(TfrmMain, frmMain);
  Application.Run;
end.
