unit Dext.Net.Nats.Protocol.Control;

interface

uses
  System.SysUtils;

function NatsControlPing: TBytes;
function NatsControlPong: TBytes;
function NatsControlSub(const ASubject, AQueue: string; ASid: Integer): TBytes;
function NatsControlUnsub(ASid: Integer; AMaxMsgs: Integer = 0): TBytes;

implementation

uses
  Dext.Net.Nats.Protocol;

function AsciiBytes(const S: string): TBytes;
begin
  Result := TEncoding.ASCII.GetBytes(S);
end;

function NatsControlPing: TBytes;
begin
  Result := AsciiBytes('PING' + NATS_CRLF);
end;

function NatsControlPong: TBytes;
begin
  Result := AsciiBytes('PONG' + NATS_CRLF);
end;

function NatsControlSub(const ASubject, AQueue: string;
  ASid: Integer): TBytes;
var
  Line: string;
begin
  if AQueue <> '' then
    Line := Format('SUB %s %s %d%s', [ASubject, AQueue, ASid, NATS_CRLF])
  else
    Line := Format('SUB %s %d%s', [ASubject, ASid, NATS_CRLF]);
  Result := AsciiBytes(Line);
end;

function NatsControlUnsub(ASid, AMaxMsgs: Integer): TBytes;
begin
  if AMaxMsgs > 0 then
    Result := AsciiBytes(Format('UNSUB %d %d%s', [ASid, AMaxMsgs, NATS_CRLF]))
  else
    Result := AsciiBytes(Format('UNSUB %d%s', [ASid, NATS_CRLF]));
end;

end.
