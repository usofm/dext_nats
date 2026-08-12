unit Dext.Net.Nats.ObjectStore.Crypto;

interface

uses
  System.SysUtils;

function NatsObjectNewNuid: string;
function NatsObjectDigestValue(const AHashBytes: TBytes): string;

implementation

uses
  System.NetEncoding;

function NatsObjectNewNuid: string;
var
  G: TGUID;
  S: string;
begin
  CreateGUID(G);
  S := GUIDToString(G);
  S := StringReplace(S, '{', '', [rfReplaceAll]);
  S := StringReplace(S, '}', '', [rfReplaceAll]);
  S := StringReplace(S, '-', '', [rfReplaceAll]);
  Result := Copy(S, 1, 22);
end;

function NatsObjectDigestValue(const AHashBytes: TBytes): string;
begin
  Result := 'SHA-256=' +
    TNetEncoding.Base64URL.EncodeBytesToString(AHashBytes);
end;

end.
