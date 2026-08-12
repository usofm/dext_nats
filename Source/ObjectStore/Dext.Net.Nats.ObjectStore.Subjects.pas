unit Dext.Net.Nats.ObjectStore.Subjects;

interface

function NatsObjectStreamName(const ABucket: string): string;
function NatsObjectEncodeName(const AName: string): string;
function NatsObjectDecodeName(const AEncoded: string): string;
function NatsObjectMetaSubject(const ABucket, AObjectName: string): string;
function NatsObjectMetaWildcard(const ABucket: string): string;
function NatsObjectChunkSubject(const ABucket, ANuid: string): string;
function NatsObjectChunkWildcard(const ABucket: string): string;
function NatsObjectIsValidBucketChar(C: Char): Boolean;

implementation

uses
  System.SysUtils,
  System.NetEncoding;

function NatsObjectStreamName(const ABucket: string): string;
begin
  Result := 'OBJ_' + ABucket;
end;

function NatsObjectEncodeName(const AName: string): string;
begin
  Result := TNetEncoding.Base64URL.EncodeBytesToString(
    TEncoding.UTF8.GetBytes(AName));
end;

function NatsObjectDecodeName(const AEncoded: string): string;
var
  Bytes: TBytes;
begin
  Result := '';
  if AEncoded = '' then Exit;
  try
    Bytes := TNetEncoding.Base64URL.DecodeStringToBytes(AEncoded);
    if Length(Bytes) > 0 then Result := TEncoding.UTF8.GetString(Bytes);
  except
    Result := '';
  end;
end;

function NatsObjectMetaSubject(const ABucket, AObjectName: string): string;
begin
  Result := Format('$O.%s.M.%s', [ABucket, NatsObjectEncodeName(AObjectName)]);
end;

function NatsObjectMetaWildcard(const ABucket: string): string;
begin
  Result := Format('$O.%s.M.>', [ABucket]);
end;

function NatsObjectChunkSubject(const ABucket, ANuid: string): string;
begin
  Result := Format('$O.%s.C.%s', [ABucket, ANuid]);
end;

function NatsObjectChunkWildcard(const ABucket: string): string;
begin
  Result := Format('$O.%s.C.>', [ABucket]);
end;

function NatsObjectIsValidBucketChar(C: Char): Boolean;
begin
  Result := ((C >= 'A') and (C <= 'Z')) or
            ((C >= 'a') and (C <= 'z')) or
            ((C >= '0') and (C <= '9')) or (C = '_') or (C = '-');
end;

end.
