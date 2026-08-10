{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                     }
{                                                                           }
{           A native NATS client library for the Dext Framework            }
{                                                                           }
{           Licensed under the Apache License, Version 2.0 (the "License"); }
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
{           License.                                                        }
{                                                                           }
{***************************************************************************}
{                                                                           }
{  NATS NKey / JWT helpers: seed decode (Base32 + CRC16-XMODEM), Ed25519    }
{  nonce signing via OpenSSL libcrypto (same DLL as Dext TLS), .creds       }
{  parsing, and CONNECT jwt/nkey/sig helpers. No socket I/O.                }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.NKeys;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.IOUtils,
  Dext.Net.Nats.Protocol;

type
  /// <summary>Raised when NKey/JWT credentials are missing, malformed, or cannot be used to sign.</summary>
  EDextNatsAuthError = class(EDextNatsException);

  /// <summary>User JWT + NKey seed extracted from a NATS <c>.creds</c> file (or set directly).</summary>
  TNatsCredentials = record
    JWT: string;
    Seed: string;
    /// <summary>Parses a credentials document (JWT + USER NKEY SEED blocks, or a bare seed line).</summary>
    class function Parse(const AText: string): TNatsCredentials; static;
    /// <summary>Loads and parses a credentials file from disk.</summary>
    class function FromFile(const APath: string): TNatsCredentials; static;
    /// <summary>True when a seed is present (JWT may still be empty for bare NKey auth).</summary>
    function HasSeed: Boolean;
    /// <summary>True when a user JWT is present.</summary>
    function HasJWT: Boolean;
  end;

/// <summary>Decodes an NKey seed string (e.g. <c>SU...</c>) to the 32-byte Ed25519 seed and role prefix.</summary>
procedure NatsDecodeSeed(const ASeed: string; out ARawSeed: TBytes; out ARolePrefix: Byte);
/// <summary>Derives the printable public NKey (e.g. <c>U...</c>) from a seed string.</summary>
function NatsPublicKeyFromSeed(const ASeed: string): string;
/// <summary>Signs ANonce with the seed and returns a base64url (no padding) signature for CONNECT <c>sig</c>.</summary>
function NatsSignNonce(const ASeed, ANonce: string): string;
/// <summary>Signs ANonce bytes; returns the raw 64-byte Ed25519 signature.</summary>
function NatsSignNonceRaw(const ASeed: string; const ANonce: TBytes): TBytes;
/// <summary>True when OpenSSL libcrypto was loaded and Ed25519 signing is available.</summary>
function NatsNKeyCryptoAvailable: Boolean;
/// <summary>
///   Fills CONNECT jwt/nkey/sig from credentials. When ASeed is set and ANonce is non-empty,
///   signs the nonce. JWT mode sets <c>jwt</c>+<c>sig</c>; bare NKey mode sets <c>nkey</c>+<c>sig</c>.
/// </summary>
procedure NatsApplyCredentialsToConnect(var AOptions: TNatsConnectOptions;
  const AJWT, ASeed, ANonce: string);

implementation

uses
  System.NetEncoding,
  Winapi.Windows;

const
  NATS_PREFIX_SEED = $90; // 18 shl 3
  NATS_PREFIX_USER = $A0; // 20 shl 3
  EVP_PKEY_ED25519 = 1087;
  LIBCRYPTO_DLL = 'libcrypto-3.dll';

  // CRC16-CCITT / XMODEM table (same as nats-io/nkeys).
  CRC16_TAB: array[0..255] of Word = (
    $0000, $1021, $2042, $3063, $4084, $50A5, $60C6, $70E7,
    $8108, $9129, $A14A, $B16B, $C18C, $D1AD, $E1CE, $F1EF,
    $1231, $0210, $3273, $2252, $52B5, $4294, $72F7, $62D6,
    $9339, $8318, $B37B, $A35A, $D3BD, $C39C, $F3FF, $E3DE,
    $2462, $3443, $0420, $1401, $64E6, $74C7, $44A4, $5485,
    $A56A, $B54B, $8528, $9509, $E5EE, $F5CF, $C5AC, $D58D,
    $3653, $2672, $1611, $0630, $76D7, $66F6, $5695, $46B4,
    $B75B, $A77A, $9719, $8738, $F7DF, $E7FE, $D79D, $C7BC,
    $48C4, $58E5, $6886, $78A7, $0840, $1861, $2802, $3823,
    $C9CC, $D9ED, $E98E, $F9AF, $8948, $9969, $A90A, $B92B,
    $5AF5, $4AD4, $7AB7, $6A96, $1A71, $0A50, $3A33, $2A12,
    $DBFD, $CBDC, $FBBF, $EB9E, $9B79, $8B58, $BB3B, $AB1A,
    $6CA6, $7C87, $4CE4, $5CC5, $2C22, $3C03, $0C60, $1C41,
    $EDAE, $FD8F, $CDEC, $DDCD, $AD2A, $BD0B, $8D68, $9D49,
    $7E97, $6EB6, $5ED5, $4EF4, $3E13, $2E32, $1E51, $0E70,
    $FF9F, $EFBE, $DFDD, $CFFC, $BF1B, $AF3A, $9F59, $8F78,
    $9188, $81A9, $B1CA, $A1EB, $D10C, $C12D, $F14E, $E16F,
    $1080, $00A1, $30C2, $20E3, $5004, $4025, $7046, $6067,
    $83B9, $9398, $A3FB, $B3DA, $C33D, $D31C, $E37F, $F35E,
    $02B1, $1290, $22F3, $32D2, $4235, $5214, $6277, $7256,
    $B5EA, $A5CB, $95A8, $8589, $F56E, $E54F, $D52C, $C50D,
    $34E2, $24C3, $14A0, $0481, $7466, $6447, $5424, $4405,
    $A7DB, $B7FA, $8799, $97B8, $E75F, $F77E, $C71D, $D73C,
    $26D3, $36F2, $0691, $16B0, $6657, $7676, $4615, $5634,
    $D94C, $C96D, $F90E, $E92F, $99C8, $89E9, $B98A, $A9AB,
    $5844, $4865, $7806, $6827, $18C0, $08E1, $3882, $28A3,
    $CB7D, $DB5C, $EB3F, $FB1E, $8BF9, $9BD8, $ABBB, $BB9A,
    $4A75, $5A54, $6A37, $7A16, $0AF1, $1AD0, $2AB3, $3A92,
    $FD2E, $ED0F, $DD6C, $CD4D, $BDAA, $AD8B, $9DE8, $8DC9,
    $7C26, $6C07, $5C64, $4C45, $3CA2, $2C83, $1CE0, $0CC1,
    $EF1F, $FF3E, $CF5D, $DF7C, $AF9B, $BFBA, $8FD9, $9FF8,
    $6E17, $7E36, $4E55, $5E74, $2E93, $3EB2, $0ED1, $1EF0
  );

type
  TEVP_PKEY_new_raw_private_key = function(AType: Integer; AEngine: Pointer;
    const AKey: PByte; AKeyLen: NativeUInt): Pointer; cdecl;
  TEVP_PKEY_get_raw_public_key = function(APkey: Pointer; APub: PByte;
    var ALen: NativeUInt): Integer; cdecl;
  TEVP_PKEY_free = procedure(APkey: Pointer); cdecl;
  TEVP_MD_CTX_new = function: Pointer; cdecl;
  TEVP_MD_CTX_free = procedure(ACtx: Pointer); cdecl;
  TEVP_DigestSignInit = function(ACtx: Pointer; APCtx: PPointer; AType: Pointer;
    AEngine: Pointer; APkey: Pointer): Integer; cdecl;
  TEVP_DigestSign = function(ACtx: Pointer; ASigRet: PByte; var ASigLen: NativeUInt;
    const ATbs: PByte; ATbsLen: NativeUInt): Integer; cdecl;

var
  GCryptoLock: TCriticalSection;
  GCryptoLib: HMODULE;
  GCryptoTried: Boolean;
  GCryptoReady: Boolean;
  GEVP_PKEY_new_raw_private_key: TEVP_PKEY_new_raw_private_key;
  GEVP_PKEY_get_raw_public_key: TEVP_PKEY_get_raw_public_key;
  GEVP_PKEY_free: TEVP_PKEY_free;
  GEVP_MD_CTX_new: TEVP_MD_CTX_new;
  GEVP_MD_CTX_free: TEVP_MD_CTX_free;
  GEVP_DigestSignInit: TEVP_DigestSignInit;
  GEVP_DigestSign: TEVP_DigestSign;

function NatsCrc16(const AData: TBytes): Word;
var
  I: Integer;
  Crc: Word;
begin
  Crc := 0;
  for I := 0 to Length(AData) - 1 do
    Crc := Word((Crc shl 8) and $FFFF) xor CRC16_TAB[((Crc shr 8) xor AData[I]) and $FF];
  Result := Crc;
end;

const
  BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

function NatsBase32Decode(const AText: string): TBytes;
var
  Clean: string;
  I, BitCount, OutLen: Integer;
  BitBuf: Cardinal;
  Ch: Char;
  Val: Integer;
  DecodeMap: array[0..255] of SmallInt;
begin
  // Standard Base32 alphabet A-Z2-7, no padding (NATS / Stellar-style).
  FillChar(DecodeMap, SizeOf(DecodeMap), $FF);
  for I := 1 to Length(BASE32_ALPHABET) do
    DecodeMap[Byte(BASE32_ALPHABET[I])] := I - 1;

  Clean := '';
  for I := 1 to Length(AText) do
  begin
    Ch := UpCase(AText[I]);
    if CharInSet(Ch, ['A'..'Z', '2'..'7']) then
      Clean := Clean + Ch;
  end;
  if Clean = '' then
    raise EDextNatsAuthError.Create('NKey string is empty or not Base32');

  SetLength(Result, (Length(Clean) * 5) div 8);
  BitBuf := 0;
  BitCount := 0;
  OutLen := 0;
  for I := 1 to Length(Clean) do
  begin
    Val := DecodeMap[Byte(Clean[I])];
    if Val < 0 then
      raise EDextNatsAuthError.Create('Invalid NKey Base32 character');
    BitBuf := (BitBuf shl 5) or Cardinal(Val);
    Inc(BitCount, 5);
    if BitCount >= 8 then
    begin
      Dec(BitCount, 8);
      Result[OutLen] := Byte((BitBuf shr BitCount) and $FF);
      Inc(OutLen);
    end;
  end;
  if OutLen <> Length(Result) then
    SetLength(Result, OutLen);
end;

function NatsBase32Encode(const AData: TBytes): string;
var
  I, BitCount: Integer;
  BitBuf: Cardinal;
  Sb: TStringBuilder;
begin
  Sb := TStringBuilder.Create;
  try
    BitBuf := 0;
    BitCount := 0;
    for I := 0 to Length(AData) - 1 do
    begin
      BitBuf := (BitBuf shl 8) or AData[I];
      Inc(BitCount, 8);
      while BitCount >= 5 do
      begin
        Dec(BitCount, 5);
        Sb.Append(BASE32_ALPHABET[((BitBuf shr BitCount) and $1F) + 1]);
      end;
    end;
    if BitCount > 0 then
      Sb.Append(BASE32_ALPHABET[((BitBuf shl (5 - BitCount)) and $1F) + 1]);
    Result := Sb.ToString;
  finally
    Sb.Free;
  end;
end;

function NatsBase64UrlEncode(const AData: TBytes): string;
var
  S: string;
begin
  // Raw URL-safe Base64 without padding (matches Go base64.RawURLEncoding).
  S := TNetEncoding.Base64URL.EncodeBytesToString(AData);
  while (S <> '') and (S[Length(S)] = '=') do
    Delete(S, Length(S), 1);
  Result := S;
end;

procedure WipeBytes(var AData: TBytes);
begin
  if Length(AData) > 0 then
    FillChar(AData[0], Length(AData), 0);
  SetLength(AData, 0);
end;

function EnsureCryptoLoaded: Boolean;
begin
  GCryptoLock.Enter;
  try
    if GCryptoTried then
      Exit(GCryptoReady);
    GCryptoTried := True;
    GCryptoLib := LoadLibrary(PChar(LIBCRYPTO_DLL));
    if GCryptoLib = 0 then
      Exit(False);

    @GEVP_PKEY_new_raw_private_key := GetProcAddress(GCryptoLib, 'EVP_PKEY_new_raw_private_key');
    @GEVP_PKEY_get_raw_public_key := GetProcAddress(GCryptoLib, 'EVP_PKEY_get_raw_public_key');
    @GEVP_PKEY_free := GetProcAddress(GCryptoLib, 'EVP_PKEY_free');
    @GEVP_MD_CTX_new := GetProcAddress(GCryptoLib, 'EVP_MD_CTX_new');
    @GEVP_MD_CTX_free := GetProcAddress(GCryptoLib, 'EVP_MD_CTX_free');
    @GEVP_DigestSignInit := GetProcAddress(GCryptoLib, 'EVP_DigestSignInit');
    @GEVP_DigestSign := GetProcAddress(GCryptoLib, 'EVP_DigestSign');

    GCryptoReady := Assigned(GEVP_PKEY_new_raw_private_key)
      and Assigned(GEVP_PKEY_get_raw_public_key)
      and Assigned(GEVP_PKEY_free)
      and Assigned(GEVP_MD_CTX_new)
      and Assigned(GEVP_MD_CTX_free)
      and Assigned(GEVP_DigestSignInit)
      and Assigned(GEVP_DigestSign);
    Result := GCryptoReady;
  finally
    GCryptoLock.Leave;
  end;
end;

procedure RequireCrypto;
begin
  if not EnsureCryptoLoaded then
    raise EDextNatsAuthError.Create(
      'OpenSSL libcrypto-3.dll is required for NKey Ed25519 signing (same DLL as Dext TLS)');
end;

function ExtractPemBlock(const AText, ABegin, AEnd: string): string;
var
  StartPos, EndPos, ContentStart: Integer;
  Block: string;
begin
  Result := '';
  StartPos := Pos(ABegin, AText);
  if StartPos = 0 then
    Exit;
  ContentStart := StartPos + Length(ABegin);
  EndPos := Pos(AEnd, AText);
  if (EndPos = 0) or (EndPos <= ContentStart) then
    Exit;
  Block := Copy(AText, ContentStart, EndPos - ContentStart);
  Result := Trim(Block);
  // Collapse internal newlines that wrap long JWTs.
  Result := StringReplace(Result, #13#10, '', [rfReplaceAll]);
  Result := StringReplace(Result, #10, '', [rfReplaceAll]);
  Result := StringReplace(Result, #13, '', [rfReplaceAll]);
  Result := Trim(Result);
end;

{ TNatsCredentials }

class function TNatsCredentials.Parse(const AText: string): TNatsCredentials;
var
  Text, Line, Candidate: string;
  Lines: TArray<string>;
  I: Integer;
begin
  Result.JWT := '';
  Result.Seed := '';
  Text := Trim(AText);
  if Text = '' then
    raise EDextNatsAuthError.Create('Credentials text is empty');

  Result.JWT := ExtractPemBlock(Text, '-----BEGIN NATS USER JWT-----', '------END NATS USER JWT------');
  Result.Seed := ExtractPemBlock(Text, '-----BEGIN USER NKEY SEED-----', '------END USER NKEY SEED------');

  if Result.Seed = '' then
  begin
    // Bare seed file: first non-comment, non-empty line starting with S.
    Lines := Text.Split([#13#10, #10, #13]);
    for I := 0 to High(Lines) do
    begin
      Line := Trim(Lines[I]);
      if (Line = '') or Line.StartsWith('#') then
        Continue;
      Candidate := Line;
      if Candidate.StartsWith('SU', True) or Candidate.StartsWith('SA', True)
        or Candidate.StartsWith('SO', True) or Candidate.StartsWith('SC', True)
        or Candidate.StartsWith('SN', True) then
      begin
        Result.Seed := Candidate;
        Break;
      end;
    end;
  end;

  if Result.Seed = '' then
    raise EDextNatsAuthError.Create('Credentials contain no NKey seed');
end;

class function TNatsCredentials.FromFile(const APath: string): TNatsCredentials;
var
  Text: string;
begin
  if not FileExists(APath) then
    raise EDextNatsAuthError.CreateFmt('Credentials file not found: %s', [APath]);
  Text := TFile.ReadAllText(APath, TEncoding.UTF8);
  Result := Parse(Text);
end;

function TNatsCredentials.HasSeed: Boolean;
begin
  Result := Seed <> '';
end;

function TNatsCredentials.HasJWT: Boolean;
begin
  Result := JWT <> '';
end;

procedure NatsDecodeSeed(const ASeed: string; out ARawSeed: TBytes; out ARolePrefix: Byte);
var
  Raw, Payload: TBytes;
  Expected, Actual: Word;
  B1, B2: Byte;
begin
  SetLength(ARawSeed, 0);
  ARolePrefix := 0;
  Raw := NatsBase32Decode(Trim(ASeed));
  if Length(Raw) < 5 then
    raise EDextNatsAuthError.Create('NKey seed is too short');

  Expected := Word(Raw[Length(Raw) - 2]) or (Word(Raw[Length(Raw) - 1]) shl 8);
  SetLength(Payload, Length(Raw) - 2);
  Move(Raw[0], Payload[0], Length(Payload));
  Actual := NatsCrc16(Payload);
  if Actual <> Expected then
    raise EDextNatsAuthError.Create('NKey seed CRC16 checksum mismatch');

  // Seed encoding packs PrefixByteSeed | role into two bytes (see nats-io/nkeys EncodeSeed).
  B1 := Payload[0] and $F8;
  B2 := Byte(((Payload[0] and $07) shl 5) or ((Payload[1] and $F8) shr 3));
  if B1 <> NATS_PREFIX_SEED then
    raise EDextNatsAuthError.Create('NKey string is not a seed (expected prefix S…)');
  ARolePrefix := B2;
  if Length(Payload) <> 34 then
    raise EDextNatsAuthError.CreateFmt('NKey seed payload length %d is invalid (want 34)', [Length(Payload)]);
  SetLength(ARawSeed, 32);
  Move(Payload[2], ARawSeed[0], 32);
end;

function NatsEncodePublicKey(ARolePrefix: Byte; const APubRaw: TBytes): string;
var
  Raw: TBytes;
  Crc: Word;
begin
  if Length(APubRaw) <> 32 then
    raise EDextNatsAuthError.Create('Ed25519 public key must be 32 bytes');
  SetLength(Raw, 35);
  Raw[0] := ARolePrefix;
  Move(APubRaw[0], Raw[1], 32);
  Crc := NatsCrc16(Copy(Raw, 0, 33));
  Raw[33] := Byte(Crc and $FF);
  Raw[34] := Byte((Crc shr 8) and $FF);
  Result := NatsBase32Encode(Raw);
end;

function Ed25519PublicFromSeed(const ARawSeed: TBytes): TBytes;
var
  PKey: Pointer;
  Len: NativeUInt;
begin
  RequireCrypto;
  if Length(ARawSeed) <> 32 then
    raise EDextNatsAuthError.Create('Ed25519 seed must be 32 bytes');
  PKey := GEVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nil, @ARawSeed[0], 32);
  if PKey = nil then
    raise EDextNatsAuthError.Create('OpenSSL rejected the Ed25519 private seed');
  try
    Len := 32;
    SetLength(Result, 32);
    if GEVP_PKEY_get_raw_public_key(PKey, @Result[0], Len) <> 1 then
      raise EDextNatsAuthError.Create('OpenSSL failed to export the Ed25519 public key');
    if Len <> 32 then
      raise EDextNatsAuthError.Create('Unexpected Ed25519 public key length');
  finally
    GEVP_PKEY_free(PKey);
  end;
end;

function Ed25519Sign(const ARawSeed, AMessage: TBytes): TBytes;
var
  PKey, Ctx: Pointer;
  SigLen: NativeUInt;
  MsgPtr: PByte;
begin
  RequireCrypto;
  if Length(ARawSeed) <> 32 then
    raise EDextNatsAuthError.Create('Ed25519 seed must be 32 bytes');
  PKey := GEVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nil, @ARawSeed[0], 32);
  if PKey = nil then
    raise EDextNatsAuthError.Create('OpenSSL rejected the Ed25519 private seed');
  Ctx := nil;
  try
    Ctx := GEVP_MD_CTX_new;
    if Ctx = nil then
      raise EDextNatsAuthError.Create('OpenSSL EVP_MD_CTX_new failed');
    if GEVP_DigestSignInit(Ctx, nil, nil, nil, PKey) <> 1 then
      raise EDextNatsAuthError.Create('OpenSSL EVP_DigestSignInit failed for Ed25519');

    if Length(AMessage) > 0 then
      MsgPtr := @AMessage[0]
    else
      MsgPtr := nil;

    SigLen := 64;
    SetLength(Result, 64);
    if GEVP_DigestSign(Ctx, @Result[0], SigLen, MsgPtr, NativeUInt(Length(AMessage))) <> 1 then
      raise EDextNatsAuthError.Create('OpenSSL Ed25519 signing failed');
    if SigLen <> 64 then
      SetLength(Result, Integer(SigLen));
  finally
    if Ctx <> nil then
      GEVP_MD_CTX_free(Ctx);
    GEVP_PKEY_free(PKey);
  end;
end;

function NatsPublicKeyFromSeed(const ASeed: string): string;
var
  RawSeed, PubRaw: TBytes;
  Role: Byte;
begin
  NatsDecodeSeed(ASeed, RawSeed, Role);
  try
    PubRaw := Ed25519PublicFromSeed(RawSeed);
    try
      Result := NatsEncodePublicKey(Role, PubRaw);
    finally
      WipeBytes(PubRaw);
    end;
  finally
    WipeBytes(RawSeed);
  end;
end;

function NatsSignNonceRaw(const ASeed: string; const ANonce: TBytes): TBytes;
var
  RawSeed: TBytes;
  Role: Byte;
begin
  NatsDecodeSeed(ASeed, RawSeed, Role);
  try
    Result := Ed25519Sign(RawSeed, ANonce);
  finally
    WipeBytes(RawSeed);
  end;
end;

function NatsSignNonce(const ASeed, ANonce: string): string;
var
  NonceBytes, Sig: TBytes;
begin
  NonceBytes := TEncoding.UTF8.GetBytes(ANonce);
  try
    Sig := NatsSignNonceRaw(ASeed, NonceBytes);
    try
      Result := NatsBase64UrlEncode(Sig);
    finally
      WipeBytes(Sig);
    end;
  finally
    WipeBytes(NonceBytes);
  end;
end;

function NatsNKeyCryptoAvailable: Boolean;
begin
  Result := EnsureCryptoLoaded;
end;

procedure NatsApplyCredentialsToConnect(var AOptions: TNatsConnectOptions;
  const AJWT, ASeed, ANonce: string);
var
  Seed, JWT: string;
begin
  Seed := Trim(ASeed);
  JWT := Trim(AJWT);
  if Seed = '' then
  begin
    if JWT <> '' then
      AOptions.JWT := JWT;
    Exit;
  end;

  if JWT <> '' then
  begin
    AOptions.JWT := JWT;
    AOptions.Nkey := '';
  end
  else
  begin
    AOptions.Nkey := NatsPublicKeyFromSeed(Seed);
    AOptions.JWT := '';
  end;

  // Server INFO nonce is required for challenge-response auth (JWT or bare NKey).
  if ANonce = '' then
    raise EDextNatsAuthError.Create(
      'NATS server did not provide a nonce; cannot sign with NKey credentials');
  AOptions.Sig := NatsSignNonce(Seed, ANonce);
end;

initialization
  GCryptoLock := TCriticalSection.Create;
  GCryptoLib := 0;
  GCryptoTried := False;
  GCryptoReady := False;

finalization
  if GCryptoLib <> 0 then
    FreeLibrary(GCryptoLib);
  GCryptoLock.Free;

end.
