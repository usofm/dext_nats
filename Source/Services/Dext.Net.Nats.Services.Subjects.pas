unit Dext.Net.Nats.Services.Subjects;

interface

uses
  Dext.Net.Nats.Services;

function NatsServiceVerbName(AVerb: TNatsServiceVerb): string;
function NatsServiceDiscoverySubject(AVerb: TNatsServiceVerb;
  const AServiceName: string = ''; const AInstanceId: string = ''): string;
function NatsServiceJoinSubject(const APrefix, ASubject: string): string;
function NatsServiceIsValidName(const AValue: string): Boolean;

implementation

uses
  System.SysUtils;

function NatsServiceVerbName(AVerb: TNatsServiceVerb): string;
begin
  case AVerb of
    svPing: Result := 'PING';
    svStats: Result := 'STATS';
    svInfo: Result := 'INFO';
  else
    Result := 'PING';
  end;
end;

function NatsServiceDiscoverySubject(AVerb: TNatsServiceVerb;
  const AServiceName, AInstanceId: string): string;
begin
  Result := NATS_SRV_PREFIX + '.' + NatsServiceVerbName(AVerb);
  if AServiceName <> '' then
    Result := Result + '.' + AServiceName;
  if AInstanceId <> '' then
  begin
    if AServiceName = '' then
      raise EDextNatsServiceError.Create(
        'Service instance discovery requires a service name');
    Result := Result + '.' + AInstanceId;
  end;
end;

function NatsServiceJoinSubject(const APrefix, ASubject: string): string;
begin
  if APrefix = '' then Exit(ASubject);
  if ASubject = '' then Exit(APrefix);
  Result := APrefix + '.' + ASubject;
end;

function NatsServiceIsValidName(const AValue: string): Boolean;
var
  I: Integer;
  C: Char;
begin
  Result := AValue <> '';
  if not Result then Exit;
  for I := 1 to Length(AValue) do
  begin
    C := AValue[I];
    if not (((C >= 'A') and (C <= 'Z')) or ((C >= 'a') and (C <= 'z')) or
      ((C >= '0') and (C <= '9')) or (C = '-') or (C = '_')) then
      Exit(False);
  end;
end;

end.
