unit Dext.Net.Nats.Services.Routing;

interface

procedure NatsServiceResolveQueue(const ACustomQueue: string;
  ACustomDisabled: Boolean; const AParentQueue: string;
  AParentDisabled: Boolean; out AQueue: string; out ADisabled: Boolean);
function NatsServiceResolveEndpointSubject(const AGroupPrefix,
  AEndpointSubject, AEndpointName: string): string;
function NatsServiceResolveNestedPrefix(const AParentPrefix,
  AChildPrefix: string): string;

implementation

uses
  Dext.Net.Nats.Services.Subjects;

procedure NatsServiceResolveQueue(const ACustomQueue: string;
  ACustomDisabled: Boolean; const AParentQueue: string;
  AParentDisabled: Boolean; out AQueue: string; out ADisabled: Boolean);
begin
  if ACustomDisabled then
  begin
    AQueue := '';
    ADisabled := True;
  end
  else if ACustomQueue <> '' then
  begin
    AQueue := ACustomQueue;
    ADisabled := False;
  end
  else
  begin
    AQueue := AParentQueue;
    ADisabled := AParentDisabled;
    if ADisabled then AQueue := '';
  end;
end;

function NatsServiceResolveEndpointSubject(const AGroupPrefix,
  AEndpointSubject, AEndpointName: string): string;
var
  Subject: string;
begin
  Subject := AEndpointSubject;
  if Subject = '' then Subject := AEndpointName;
  Result := NatsServiceJoinSubject(AGroupPrefix, Subject);
end;

function NatsServiceResolveNestedPrefix(const AParentPrefix,
  AChildPrefix: string): string;
begin
  Result := NatsServiceJoinSubject(AParentPrefix, AChildPrefix);
end;

end.
