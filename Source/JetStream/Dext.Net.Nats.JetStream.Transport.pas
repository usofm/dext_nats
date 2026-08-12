{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream API transport boundary                                }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Transport;

interface

uses
  Dext.Net.Nats;

type
  /// <summary>
  /// Internal request/reply boundary used by extracted JetStream services.
  /// It keeps stream/consumer administration testable without opening the
  /// private fields of TDextNatsJetStreamContext.
  /// </summary>
  INatsJetStreamApiTransport = interface
    ['{17FC6DD7-62A3-44CE-9393-F5988DA70430}']
    function Request(const ASubjectSuffix, ABody: string;
      ATimeoutMs: Integer = 0): string;
  end;

  /// <summary>Production transport backed by an already-connected NATS client.</summary>
  TDextNatsJetStreamApiTransport = class(TInterfacedObject, INatsJetStreamApiTransport)
  private
    FClient: TDextNatsClient;
    FApiPrefix: string;
  public
    constructor Create(AClient: TDextNatsClient;
      const AApiPrefix: string = '$JS.API.');
    function Request(const ASubjectSuffix, ABody: string;
      ATimeoutMs: Integer = 0): string;
    property Client: TDextNatsClient read FClient;
    property ApiPrefix: string read FApiPrefix;
  end;

implementation

uses
  System.SysUtils;

constructor TDextNatsJetStreamApiTransport.Create(AClient: TDextNatsClient;
  const AApiPrefix: string);
begin
  inherited Create;
  if AClient = nil then
    raise EDextNatsException.Create('JetStream transport requires a NATS client');
  FClient := AClient;
  FApiPrefix := AApiPrefix;
end;

function TDextNatsJetStreamApiTransport.Request(const ASubjectSuffix,
  ABody: string; ATimeoutMs: Integer): string;
begin
  Result := FClient.Request(FApiPrefix + ASubjectSuffix,
    TEncoding.UTF8.GetBytes(ABody), ATimeoutMs).AsString;
end;

end.
