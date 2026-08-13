{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
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
{  Manual end-to-end smoke test for JetStream Object Store                  }
{  (Dext.Net.Nats.ObjectStore). Interactive console against a local         }
{  nats-server with JetStream enabled. Covers PutFile/GetFile via temp      }
{  files under the system temp directory.                                   }
{                                                                           }
{  REQUIRES the target nats-server to be started with JetStream enabled,   }
{  e.g.:                                                                    }
{                                                                           }
{      nats-server -js                                                     }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\ObjectStoreE2E\ObjectStoreE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\ObjectStoreE2E.exe                                }
{      Output\Win32\Debug\ObjectStoreE2E.exe 127.0.0.1 4222                 }
{      Output\Win32\Debug\ObjectStoreE2E.exe -no-wait                       }
{                                                                           }
{  Optional: DEXT_NATS_HOST / DEXT_NATS_PORT. Pass -no-wait to skip pause.  }
{  Step 5: PutFile/GetFile round-trip under the system temp dir, then delete  }
{  the object so Keys/List still expect the original three names.            }
{                                                                           }
{***************************************************************************}
program ObjectStoreE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  System.IOUtils,
  Dext.Collections,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas',
  Dext.Net.Nats.JetStream in '..\..\Source\Dext.Net.Nats.JetStream.pas',
  Dext.Net.Nats.JetStream.Fetch in '..\..\Source\JetStream\Dext.Net.Nats.JetStream.Fetch.pas',
  Dext.Net.Nats.ObjectStore in '..\..\Source\Dext.Net.Nats.ObjectStore.pas';

const
  { Override: ObjectStoreE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4222;

  OBJ_INVOICE = 'invoice.pdf';
  OBJ_README = 'readme.txt';
  OBJ_SCRATCH = 'scratch.bin';
  OBJ_FILEBLOB = 'fileblob.bin';
  OBJ_LINK = 'invoice-link';

  WATCH_TIMEOUT_MS = 5000;

var
  GFailureCount: Integer = 0;

procedure PrintPass(const AMessage: string);
begin
  Writeln('[PASS] ' + AMessage);
end;

procedure PrintFail(const AMessage: string);
begin
  Inc(GFailureCount);
  Writeln('[FAIL] ' + AMessage);
end;

procedure PrintInfo(const AMessage: string);
begin
  Writeln('       ' + AMessage);
end;

{ Positional args only — skip switches such as ConsolePause's -no-wait. }
function PositionalArg(AIndex: Integer): string;
var
  i, n: Integer;
  arg: string;
begin
  Result := '';
  n := 0;
  for i := 1 to ParamCount do
  begin
    arg := ParamStr(i);
    if (arg <> '') and (arg[1] = '-') then
      Continue;
    Inc(n);
    if n = AIndex then
      Exit(arg);
  end;
end;

function ResolveHost: string;
begin
  Result := PositionalArg(1);
  if Result = '' then
    Result := GetEnvironmentVariable('DEXT_NATS_HOST');
  if Result = '' then
    Result := DEFAULT_HOST;
end;

function ResolvePort: Word;
var
  text: string;
  value: Integer;
begin
  text := PositionalArg(2);
  if text = '' then
    text := GetEnvironmentVariable('DEXT_NATS_PORT');
  if text = '' then
    Exit(DEFAULT_PORT);
  value := StrToIntDef(text, -1);
  if (value < 1) or (value > 65535) then
    raise EArgumentException.CreateFmt('Invalid port "%s" (expected 1..65535).', [text]);
  Result := Word(value);
end;

function UniqueBucketName: string;
begin
  { Bucket names allow only [A-Za-z0-9_-] }
  Result := 'OSE2E_' + IntToHex(Random(MaxInt), 8);
end;

function Utf8Bytes(const AText: string): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(AText);
end;

function BytesEqual(const A, B: TBytes): Boolean;
var
  i: Integer;
begin
  Result := Length(A) = Length(B);
  if not Result then
    Exit;
  for i := 0 to High(A) do
    if A[i] <> B[i] then
      Exit(False);
end;

function KeysContain(const AKeys: IList<string>; const AKey: string): Boolean;
var
  i: Integer;
begin
  Result := False;
  if AKeys = nil then
    Exit;
  for i := 0 to AKeys.Count - 1 do
    if AKeys[i] = AKey then
      Exit(True);
end;

function ListContainsName(const AList: IList<TNatsObjectInfo>; const AName: string): Boolean;
var
  i: Integer;
begin
  Result := False;
  if AList = nil then
    Exit;
  for i := 0 to AList.Count - 1 do
    if AList[i].Name = AName then
      Exit(True);
end;

procedure RunObjectStoreE2E;
var
  host: string;
  port: Word;
  client: TDextNatsClient;
  osCtx: TDextNatsObjectStoreContext;
  store: TDextNatsObjectStore;
  cfg: TNatsObjectStoreConfig;
  bucket: string;
  invoiceBytes, readmeBytes, scratchBytes, fileBlobBytes, got: TBytes;
  info, targetInfo, linkInfo: TNatsObjectInfo;
  meta: TNatsObjectMeta;
  keys: IList<string>;
  listed: IList<TNatsObjectInfo>;
  getRaised, putRaised: Boolean;
  watcher: TDextNatsObjectStoreWatcher;
  watchLock: TCriticalSection;
  watchNames: IList<string>;
  watchReady: TEvent;
  watchCount: Integer;
  srcPath, destPath: string;
begin
  host := ResolveHost;
  port := ResolvePort;
  bucket := UniqueBucketName;
  invoiceBytes := Utf8Bytes('%PDF-1.4 dext-nats ObjectStoreE2E invoice');
  readmeBytes := Utf8Bytes('hello object store');
  scratchBytes := Utf8Bytes('temp-blob');
  fileBlobBytes := Utf8Bytes('ObjectStoreE2E PutFile/GetFile payload');
  srcPath := '';
  destPath := '';

  Writeln('=== Dext.Nats JetStream Object Store E2E ===');
  Writeln('NOTE: this requires nats-server WITH JetStream enabled (nats-server -js).');
  Writeln(Format('Target: %s:%d  Bucket: %s', [host, port, bucket]));
  Writeln;

  client := TDextNatsClient.Create;
  osCtx := nil;
  store := nil;
  try
    { Step 1: connect. }
    try
      Writeln(Format('Connecting to %s:%d ...', [host, port]));
      client.Connect(host, port);
      PrintPass('Connected to NATS server.');
      PrintInfo(Format('ServerId=%s Version=%s MaxPayload=%d Jetstream=%s',
        [client.ServerInfo.ServerId, client.ServerInfo.Version, client.ServerInfo.MaxPayload,
         BoolToStr(client.ServerInfo.Jetstream, True)]));
    except
      on E: Exception do
      begin
        PrintFail('Could not connect: ' + E.Message);
        Exit;
      end;
    end;

    if not client.ServerInfo.Jetstream then
    begin
      PrintFail('Server INFO reports jetstream=false. Restart with: nats-server -js');
      try
        client.Disconnect;
      except
      end;
      Exit;
    end;
    PrintPass('Server INFO reports jetstream=true.');

    osCtx := TDextNatsObjectStoreContext.Create(client);
    try
      { Step 2: CreateStore (unique bucket; memory storage for a cheap smoke run). }
      try
        cfg := TNatsObjectStoreConfig.CreateDefault(bucket);
        cfg.Description := 'Dext.Nats ObjectStoreE2E';
        cfg.Storage := ssMemory;
        store := osCtx.CreateStore(cfg);
        PrintPass(Format('Created store "%s" (stream %s, memory).',
          [bucket, store.StreamName]));
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('CreateStore failed: %s (Code=%d, ErrCode=%d)',
            [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('CreateStore failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 3: Put. }
      try
        info := store.Put(OBJ_INVOICE, invoiceBytes);
        if (info.Name = OBJ_INVOICE) and (info.Size = UInt64(Length(invoiceBytes))) and
           (info.Chunks >= 1) and (not info.Deleted) then
          PrintPass(Format('Put("%s") size=%d chunks=%d digest=%s.',
            [OBJ_INVOICE, Int64(info.Size), info.Chunks, info.Digest]))
        else
          PrintFail(Format('Put("%s"): unexpected meta name=%s size=%d chunks=%d deleted=%s.',
            [OBJ_INVOICE, info.Name, Int64(info.Size), info.Chunks,
             BoolToStr(info.Deleted, True)]));

        info := store.Put(OBJ_README, readmeBytes);
        store.Put(OBJ_SCRATCH, scratchBytes);
        PrintPass(Format('Put "%s" / "%s" (readme size=%d).',
          [OBJ_README, OBJ_SCRATCH, Int64(info.Size)]));
      except
        on E: Exception do
        begin
          PrintFail('Put failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 4: Get + assert. }
      try
        got := store.Get(OBJ_INVOICE, info);
        if BytesEqual(got, invoiceBytes) and (info.Name = OBJ_INVOICE) and
           (info.Size = UInt64(Length(invoiceBytes))) then
          PrintPass(Format('Get("%s") round-trip OK (%d bytes).',
            [OBJ_INVOICE, Length(got)]))
        else
          PrintFail(Format('Get("%s"): payload/meta mismatch (got %d bytes, expected %d).',
            [OBJ_INVOICE, Length(got), Length(invoiceBytes)]));

        got := store.Get(OBJ_README);
        if BytesEqual(got, readmeBytes) then
          PrintPass(Format('Get("%s") = "%s".',
            [OBJ_README, TEncoding.UTF8.GetString(got)]))
        else
          PrintFail(Format('Get("%s"): expected "%s".',
            [OBJ_README, TEncoding.UTF8.GetString(readmeBytes)]));
      except
        on E: Exception do
        begin
          PrintFail('Get failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 5: PutFile / GetFile round-trip via system temp files. }
      try
        try
          srcPath := TPath.Combine(TPath.GetTempPath,
            'dext_os_e2e_src_' + IntToHex(Random(MaxInt), 8) + '.bin');
          destPath := TPath.Combine(TPath.GetTempPath,
            'dext_os_e2e_dst_' + IntToHex(Random(MaxInt), 8) + '.bin');
          TFile.WriteAllBytes(srcPath, fileBlobBytes);

          info := store.PutFile(OBJ_FILEBLOB, srcPath);
          if (info.Name = OBJ_FILEBLOB) and (info.Size = UInt64(Length(fileBlobBytes))) and
             (info.Chunks >= 1) and (not info.Deleted) then
            PrintPass(Format('PutFile("%s", temp) size=%d chunks=%d digest=%s.',
              [OBJ_FILEBLOB, Int64(info.Size), info.Chunks, info.Digest]))
          else
            PrintFail(Format('PutFile("%s"): unexpected meta name=%s size=%d chunks=%d.',
              [OBJ_FILEBLOB, info.Name, Int64(info.Size), info.Chunks]));

          info := store.GetFile(OBJ_FILEBLOB, destPath);
          got := TFile.ReadAllBytes(destPath);
          if BytesEqual(got, fileBlobBytes) and (info.Name = OBJ_FILEBLOB) and
             (info.Size = UInt64(Length(fileBlobBytes))) then
            PrintPass(Format('GetFile("%s", temp) round-trip OK (%d bytes).',
              [OBJ_FILEBLOB, Length(got)]))
          else
            PrintFail(Format('GetFile("%s"): payload/meta mismatch (got %d bytes, expected %d).',
              [OBJ_FILEBLOB, Length(got), Length(fileBlobBytes)]));

          { Drop the temp object so Keys/List still expect the original three names. }
          store.Delete(OBJ_FILEBLOB);
          PrintPass(Format('Delete("%s") after PutFile/GetFile (Keys baseline unchanged).',
            [OBJ_FILEBLOB]));
        except
          on E: Exception do
          begin
            PrintFail('PutFile/GetFile failed: ' + E.Message);
            Exit;
          end;
        end;
      finally
        if (srcPath <> '') and TFile.Exists(srcPath) then
          TFile.Delete(srcPath);
        if (destPath <> '') and TFile.Exists(destPath) then
          TFile.Delete(destPath);
      end;

      { Step 6: Keys + List. }
      try
        keys := store.Keys;
        PrintInfo(Format('Keys.Count=%d', [keys.Count]));
        if (keys.Count = 3) and KeysContain(keys, OBJ_INVOICE) and
           KeysContain(keys, OBJ_README) and KeysContain(keys, OBJ_SCRATCH) then
          PrintPass('Keys lists invoice.pdf, readme.txt, scratch.bin.')
        else
          PrintFail(Format('Keys expected 3 live names including %s/%s/%s; Count=%d.',
            [OBJ_INVOICE, OBJ_README, OBJ_SCRATCH, keys.Count]));

        listed := store.List;
        PrintInfo(Format('List.Count=%d', [listed.Count]));
        if (listed.Count = 3) and ListContainsName(listed, OBJ_INVOICE) and
           ListContainsName(listed, OBJ_README) and ListContainsName(listed, OBJ_SCRATCH) then
          PrintPass('List returns metadata for all three objects.')
        else
          PrintFail(Format('List unexpected: Count=%d (expected 3).', [listed.Count]));
      except
        on E: Exception do
        begin
          PrintFail('Keys/List failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 7: Delete. }
      try
        store.Delete(OBJ_SCRATCH);
        getRaised := False;
        try
          got := store.Get(OBJ_SCRATCH);
        except
          on E: EDextNatsObjectStoreError do
            getRaised := True;
        end;
        if getRaised then
          PrintPass(Format('Delete("%s"): Get raises ObjectStoreError.', [OBJ_SCRATCH]))
        else
          PrintFail(Format('Delete("%s"): Get still returned %d bytes.',
            [OBJ_SCRATCH, Length(got)]));

        keys := store.Keys;
        if (keys.Count = 2) and KeysContain(keys, OBJ_INVOICE) and
           KeysContain(keys, OBJ_README) and (not KeysContain(keys, OBJ_SCRATCH)) then
          PrintPass('Keys after Delete lists invoice.pdf and readme.txt only.')
        else
          PrintFail(Format('Keys after Delete: Count=%d (expected 2).', [keys.Count]));
      except
        on E: Exception do
          PrintFail('Delete failed: ' + E.Message);
      end;

      { Step 8: UpdateMeta (description + headers; before Seal). }
      try
        meta := TNatsObjectMeta.Create(OBJ_INVOICE);
        meta.Description := 'ObjectStoreE2E invoice';
        meta.Headers := nil;
        meta.Headers.Add('content-type', 'application/pdf');
        info := store.UpdateMeta(OBJ_INVOICE, meta);
        if (info.Name = OBJ_INVOICE) and (info.Description = 'ObjectStoreE2E invoice') and
           (info.Headers.GetValue('content-type') = 'application/pdf') then
          PrintPass(Format('UpdateMeta("%s"): description/headers OK.', [OBJ_INVOICE]))
        else
          PrintFail(Format('UpdateMeta("%s"): unexpected desc=%s content-type=%s.',
            [OBJ_INVOICE, info.Description, info.Headers.GetValue('content-type')]));

        info := store.GetInfo(OBJ_INVOICE);
        if (info.Description = 'ObjectStoreE2E invoice') and
           (info.Headers.GetValue('content-type') = 'application/pdf') then
          PrintPass('GetInfo after UpdateMeta reflects new description/headers.')
        else
          PrintFail('GetInfo after UpdateMeta did not reflect changes.');
      except
        on E: Exception do
          PrintFail('UpdateMeta failed: ' + E.Message);
      end;

      { Step 9: WatchAll — brief receive then Stop (before Seal). }
      watcher := nil;
      watchLock := TCriticalSection.Create;
      watchNames := TCollections.CreateList<string>;
      watchReady := TEvent.Create(nil, True, False, '');
      try
        try
          watcher := store.WatchAll(
            procedure(const AInfo: TNatsObjectInfo)
            begin
              if AInfo.Deleted then
                Exit;
              watchLock.Enter;
              try
                if not KeysContain(watchNames, AInfo.Name) then
                  watchNames.Add(AInfo.Name);
                if watchNames.Count >= 2 then
                  watchReady.SetEvent;
              finally
                watchLock.Leave;
              end;
            end);

          if watchReady.WaitFor(WATCH_TIMEOUT_MS) = wrSignaled then
          begin
            watchLock.Enter;
            try
              watchCount := watchNames.Count;
              if (watchCount >= 2) and KeysContain(watchNames, OBJ_INVOICE) and
                 KeysContain(watchNames, OBJ_README) then
                PrintPass(Format('WatchAll delivered initial snapshot (%d names); Stop.',
                  [watchCount]))
              else
                PrintFail(Format('WatchAll names unexpected: Count=%d.', [watchCount]));
            finally
              watchLock.Leave;
            end;
          end
          else
            PrintFail(Format('WatchAll timed out waiting for initial deliveries (%d ms).',
              [WATCH_TIMEOUT_MS]));

          watcher.Stop;
          if not watcher.Active then
            PrintPass('WatchAll Stop: Active=False.')
          else
            PrintFail('WatchAll Stop: Active still True.');
        except
          on E: Exception do
            PrintFail('WatchAll failed: ' + E.Message);
        end;
      finally
        FreeAndNil(watcher);
        watchReady.Free;
        watchLock.Free;
      end;

      { Step 10 (optional): AddLink + Get through link (before Seal). }
      try
        targetInfo := store.GetInfo(OBJ_INVOICE);
        linkInfo := store.AddLink(OBJ_LINK, targetInfo);
        if linkInfo.IsLink and (linkInfo.Link.Name = OBJ_INVOICE) and
           (linkInfo.Link.Bucket = bucket) then
          PrintPass(Format('AddLink("%s" -> %s/%s).',
            [OBJ_LINK, linkInfo.Link.Bucket, linkInfo.Link.Name]))
        else
          PrintFail(Format('AddLink("%s"): unexpected IsLink=%s target=%s/%s.',
            [OBJ_LINK, BoolToStr(linkInfo.IsLink, True), linkInfo.Link.Bucket,
             linkInfo.Link.Name]));

        got := store.Get(OBJ_LINK, info);
        if BytesEqual(got, invoiceBytes) and (info.Name = OBJ_INVOICE) then
          PrintPass(Format('Get("%s") follows link to "%s" (%d bytes).',
            [OBJ_LINK, OBJ_INVOICE, Length(got)]))
        else
          PrintFail(Format('Get("%s") via link: got name=%s size=%d.',
            [OBJ_LINK, info.Name, Length(got)]));
      except
        on E: Exception do
          PrintFail('AddLink/Get-via-link failed: ' + E.Message);
      end;

      { Step 11: Seal + verify further Put fails / IsSealed (after Watch/UpdateMeta/AddLink). }
      try
        if store.IsSealed then
          PrintFail('IsSealed was True before Seal.')
        else
          PrintPass('IsSealed=False before Seal.');

        store.Seal;
        if store.IsSealed then
          PrintPass('Seal: IsSealed=True.')
        else
          PrintFail('Seal: IsSealed still False.');

        putRaised := False;
        try
          store.Put('after-seal.bin', Utf8Bytes('should-fail'));
        except
          on E: EDextNatsJetStreamError do
            putRaised := True;
          on E: EDextNatsObjectStoreError do
            putRaised := True;
        end;
        if putRaised then
          PrintPass('Put after Seal raises (store sealed).')
        else
          PrintFail('Put after Seal unexpectedly succeeded.');

        { Reads still work on a sealed store. }
        got := store.Get(OBJ_INVOICE);
        if BytesEqual(got, invoiceBytes) then
          PrintPass(Format('Get("%s") still works after Seal.', [OBJ_INVOICE]))
        else
          PrintFail('Get after Seal payload mismatch.');
      except
        on E: Exception do
          PrintFail('Seal failed: ' + E.Message);
      end;

      { Step 12: DeleteStore (works even when sealed). }
      try
        FreeAndNil(store);
        osCtx.DeleteStore(bucket);
        PrintPass(Format('Deleted store "%s".', [bucket]));
      except
        on E: EDextNatsJetStreamError do
          PrintFail(Format('DeleteStore failed: %s (Code=%d, ErrCode=%d)',
            [E.Message, E.Code, E.ErrCode]));
        on E: Exception do
          PrintFail('DeleteStore failed: ' + E.Message);
      end;
    finally
      FreeAndNil(store);
      FreeAndNil(osCtx);
    end;

    { Step 13: disconnect. }
    try
      client.Disconnect;
      PrintPass('Disconnected cleanly.');
    except
      on E: Exception do
        PrintFail('Disconnect failed: ' + E.Message);
    end;
  finally
    client.Free;
  end;
end;

begin
  Randomize;
  SetConsoleCharSet;
  try
    RunObjectStoreE2E;

    Writeln;
    if GFailureCount = 0 then
      Writeln('=== ALL STEPS PASSED ===')
    else
      Writeln(Format('=== %d STEP(S) FAILED - see [FAIL] lines above ===', [GFailureCount]));

    { ConsolePause (Dext.Utils) only actually blocks on ReadLn when running under the
      debugger; pass -no-wait on the command line to force-skip it in any context, and
      it is a no-op in non-interactive/CI runs where no console is attached. }
    ConsolePause;
    ExitCode := GFailureCount;
  except
    on E: Exception do
    begin
      Writeln('FATAL ERROR: ' + E.ClassName + ': ' + E.Message);
      ConsolePause;
      ExitCode := 1;
    end;
  end;
end.
