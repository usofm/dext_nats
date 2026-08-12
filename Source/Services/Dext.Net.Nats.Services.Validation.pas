unit Dext.Net.Nats.Services.Validation;

interface

function NatsServiceValidateName(const AValue: string): Boolean;
function NatsServiceValidateSemVer(const AVersion: string): Boolean;
function NatsServiceValidateSubject(const ASubject: string): Boolean;

implementation

uses
  System.SysUtils,
  System.RegularExpressions;

const
  CSemVerPattern =
    '^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)' +
    '(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?' +
    '(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$';

function NatsServiceValidateName(const AValue: string): Boolean;
var
  I: Integer;
  C: Char;
begin
  if AValue = '' then Exit(False);
  for I := 1 to Length(AValue) do
  begin
    C := AValue[I];
    if not (((C >= 'A') and (C <= 'Z')) or ((C >= 'a') and (C <= 'z')) or
      ((C >= '0') and (C <= '9')) or (C = '-') or (C = '_')) then
      Exit(False);
  end;
  Result := True;
end;

function NatsServiceValidateSemVer(const AVersion: string): Boolean;
begin
  Result := (AVersion <> '') and TRegEx.IsMatch(AVersion, CSemVerPattern);
end;

function NatsServiceValidateSubject(const ASubject: string): Boolean;
var
  I: Integer;
  C: Char;
begin
  if ASubject = '' then Exit(False);
  for I := 1 to Length(ASubject) do
  begin
    C := ASubject[I];
    if C = ' ' then Exit(False);
    if (C = '>') and (I <> Length(ASubject)) then Exit(False);
  end;
  Result := True;
end;

end.
