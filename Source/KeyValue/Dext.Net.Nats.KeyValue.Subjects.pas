unit Dext.Net.Nats.KeyValue.Subjects;

interface

uses
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.KeyValue;

function NatsKvStreamName(const ABucket: string): string;
function NatsKvSubjectForKey(const ABucket, AKey: string): string;
function NatsKvKeyFromSubject(const ABucket, ASubject: string): string;
function NatsKvOperationFromHeaders(const AHeaders: TNatsHeaders): TNatsKvOperation;
procedure NatsKvValidateBucket(const ABucket: string);
procedure NatsKvValidateKey(const AKey: string);
procedure NatsKvValidateSearchKey(const AKey: string);
procedure NatsKvValidateTtlNanos(ATTLNanos: Int64; const ALabel: string);

implementation

uses
  System.SysUtils;

function IsBucketChar(C: Char): Boolean;
begin
  Result := ((C >= 'a') and (C <= 'z')) or ((C >= 'A') and (C <= 'Z')) or
    ((C >= '0') and (C <= '9')) or (C = '_') or (C = '-');
end;

function IsKeyChar(C: Char): Boolean;
begin
  Result := IsBucketChar(C) or (C = '.') or (C = '/') or (C = '=');
end;

function IsSearchKeyChar(C: Char): Boolean;
begin
  Result := IsKeyChar(C) or (C = '*');
end;

procedure NatsKvValidateBucket(const ABucket: string);
var
  I: Integer;
begin
  if ABucket = '' then
    raise EDextNatsKeyValueError.Create('KV bucket name must not be empty');
  for I := 1 to Length(ABucket) do
    if not IsBucketChar(ABucket[I]) then
      raise EDextNatsKeyValueError.CreateFmt('Invalid KV bucket name: %s', [ABucket]);
end;

procedure NatsKvValidateKey(const AKey: string);
var
  I: Integer;
begin
  if AKey = '' then
    raise EDextNatsKeyValueError.Create('KV key must not be empty');
  if (AKey[1] = '.') or (AKey[Length(AKey)] = '.') or (Pos('..', AKey) > 0) then
    raise EDextNatsKeyValueError.CreateFmt('Invalid KV key: %s', [AKey]);
  for I := 1 to Length(AKey) do
    if not IsKeyChar(AKey[I]) then
      raise EDextNatsKeyValueError.CreateFmt('Invalid KV key: %s', [AKey]);
end;

procedure NatsKvValidateSearchKey(const AKey: string);
var
  I, N: Integer;
begin
  if AKey = '' then
    raise EDextNatsKeyValueError.Create('KV key filter must not be empty');
  N := Length(AKey);
  if (AKey[1] = '.') or (AKey[N] = '.') or (Pos('..', AKey) > 0) then
    raise EDextNatsKeyValueError.CreateFmt('Invalid KV key filter: %s', [AKey]);
  for I := 1 to N do
  begin
    if AKey[I] = '>' then
    begin
      if I <> N then
        raise EDextNatsKeyValueError.CreateFmt('Invalid KV key filter: %s', [AKey]);
    end
    else if not IsSearchKeyChar(AKey[I]) then
      raise EDextNatsKeyValueError.CreateFmt('Invalid KV key filter: %s', [AKey]);
  end;
end;

procedure NatsKvValidateTtlNanos(ATTLNanos: Int64; const ALabel: string);
begin
  if ATTLNanos <= 0 then Exit;
  if ATTLNanos < NATS_KV_MIN_TTL_NANOS then
    raise EDextNatsKeyValueError.CreateFmt(
      'KV %s must be at least 1 second (%d ns)', [ALabel, NATS_KV_MIN_TTL_NANOS]);
end;

function NatsKvStreamName(const ABucket: string): string;
begin
  Result := NATS_KV_STREAM_PREFIX + ABucket;
end;

function NatsKvSubjectForKey(const ABucket, AKey: string): string;
begin
  Result := Format(NATS_KV_SUBJECT_PREFIX, [ABucket]) + AKey;
end;

function NatsKvKeyFromSubject(const ABucket, ASubject: string): string;
var
  Prefix: string;
begin
  Prefix := Format(NATS_KV_SUBJECT_PREFIX, [ABucket]);
  if ASubject.StartsWith(Prefix) and (Length(ASubject) > Length(Prefix)) then
    Result := Copy(ASubject, Length(Prefix) + 1, MaxInt)
  else
    Result := '';
end;

function NatsKvOperationFromHeaders(const AHeaders: TNatsHeaders): TNatsKvOperation;
var
  Op, Reason: string;
begin
  Result := kvoPut;
  Op := AHeaders.GetValue(NATS_KV_OP_HEADER);
  if SameText(Op, NATS_KV_OP_DEL) then Exit(kvoDelete);
  if SameText(Op, NATS_KV_OP_PURGE) then Exit(kvoPurge);
  Reason := AHeaders.GetValue(NATS_KV_MARKER_REASON_HEADER);
  if SameText(Reason, 'MaxAge') or SameText(Reason, 'Purge') then
    Result := kvoPurge
  else if SameText(Reason, 'Remove') then
    Result := kvoDelete;
end;

end.
