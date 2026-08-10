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
{  Manual end-to-end smoke test for JetStream Key-Value                     }
{  (Dext.Net.Nats.KeyValue). Interactive console against a local            }
{  nats-server with JetStream enabled.                                      }
{                                                                           }
{  Covers: CreateBucket / Put / Get / Keys / History / Delete / Purge,      }
{  CAS Create/Update (including conflict errors), WatchAll with             }
{  EndOfInitial then Stop, IncludeHistory Watch on one key, and optional   }
{  per-key TTL (LimitMarkerTTL + Create with Nats-TTL; soft-skips on       }
{  older servers).                                                         }
{                                                                           }
{  REQUIRES the target nats-server to be started with JetStream enabled,   }
{  e.g.:                                                                    }
{                                                                           }
{      nats-server -js                                                     }
{                                                                           }
{  Build (Delphi 12 / Studio 23.0):                                         }
{                                                                           }
{      msbuild Demo\KeyValueE2E\KeyValueE2E.dproj /p:Config=Debug /p:Platform=Win32 }
{                                                                           }
{  Run:                                                                     }
{                                                                           }
{      Output\Win32\Debug\KeyValueE2E.exe                                   }
{      Output\Win32\Debug\KeyValueE2E.exe 127.0.0.1 4222                    }
{      Output\Win32\Debug\KeyValueE2E.exe -no-wait                          }
{                                                                           }
{  Optional: DEXT_NATS_HOST / DEXT_NATS_PORT. Pass -no-wait to skip pause.  }
{                                                                           }
{***************************************************************************}
program KeyValueE2E;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  Dext.Collections,
  Dext.Utils,
  Dext.Net.Nats.Protocol in '..\..\Source\Dext.Net.Nats.Protocol.pas',
  Dext.Net.Nats in '..\..\Source\Dext.Net.Nats.pas',
  Dext.Net.Nats.JetStream in '..\..\Source\Dext.Net.Nats.JetStream.pas',
  Dext.Net.Nats.KeyValue in '..\..\Source\Dext.Net.Nats.KeyValue.pas';

const
  { Override: KeyValueE2E.exe [host] [port]   (flags like -no-wait are ignored) }
  DEFAULT_HOST = '127.0.0.1';
  DEFAULT_PORT = 4222;

  KEY_WIDGET = 'widget-blue';
  KEY_COUNTER = 'counter';
  KEY_SCRATCH = 'scratch';
  KEY_CAS = 'cas-lock';
  KEY_CAS_REV = 'cas-rev';
  KEY_TTL = 'session';

  { 2s per-key TTL / limit-marker floor (nanoseconds). Wait budget below. }
  TTL_NANOS = Int64(2) * NATS_KV_MIN_TTL_NANOS;
  TTL_WAIT_MS = 4500;
  TTL_POLL_MS = 200;

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

procedure PrintSkip(const AMessage: string);
begin
  Writeln('[SKIP] ' + AMessage);
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

function UniqueBucketName(const APrefix: string = 'KVE2E_'): string;
begin
  { Bucket names allow only [A-Za-z0-9_-] }
  Result := APrefix + IntToHex(Random(MaxInt), 8);
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

procedure RunCasSteps(kv: TDextNatsKeyValue);
var
  rev, rev2, stale: UInt64;
  entry: TNatsKeyValueEntry;
  raised: Boolean;
begin
  { Step 8a: CAS Create — succeed when absent, KeyExists when live. }
  try
    rev := kv.Create(KEY_CAS, 'held');
    if (rev = 0) or (kv.Get(KEY_CAS).AsString <> 'held') then
      PrintFail(Format('Create("%s"): unexpected revision=%d value="%s".',
        [KEY_CAS, Int64(rev), kv.Get(KEY_CAS).AsString]))
    else
      PrintPass(Format('Create("%s") = "%s" revision=%d.',
        [KEY_CAS, 'held', Int64(rev)]));

    raised := False;
    try
      kv.Create(KEY_CAS, 'again');
    except
      on E: EDextNatsKeyExists do
        raised := True;
    end;
    if raised and (kv.Get(KEY_CAS).AsString = 'held') then
      PrintPass(Format('Create("%s") conflict raises EDextNatsKeyExists.', [KEY_CAS]))
    else
      PrintFail(Format('Create("%s") conflict: expected EDextNatsKeyExists, raised=%s value="%s".',
        [KEY_CAS, BoolToStr(raised, True), kv.Get(KEY_CAS).AsString]));

    kv.Delete(KEY_CAS);
    rev2 := kv.Create(KEY_CAS, 'held-again');
    if (rev2 > rev) and (kv.Get(KEY_CAS).AsString = 'held-again') then
      PrintPass(Format('Create("%s") after Delete succeeds (revision=%d).',
        [KEY_CAS, Int64(rev2)]))
    else
      PrintFail(Format('Create("%s") after Delete: rev=%d value="%s".',
        [KEY_CAS, Int64(rev2), kv.Get(KEY_CAS).AsString]));
  except
    on E: Exception do
      PrintFail('CAS Create failed: ' + E.Message);
  end;

  { Step 8b: CAS Update — succeed on matching revision, mismatch otherwise. }
  try
    stale := kv.Put(KEY_CAS_REV, '41');
    rev := kv.Update(KEY_CAS_REV, '40', stale);
    entry := kv.Get(KEY_CAS_REV);
    if (rev > stale) and (entry.AsString = '40') and (entry.Revision = rev) then
      PrintPass(Format('Update("%s") CAS ok: %d -> %d value="%s".',
        [KEY_CAS_REV, Int64(stale), Int64(rev), entry.AsString]))
    else
      PrintFail(Format('Update("%s"): expected value "40" rev>%d, got "%s" rev=%d.',
        [KEY_CAS_REV, Int64(stale), entry.AsString, Int64(entry.Revision)]));

    kv.Put(KEY_CAS_REV, '42'); { bump past `rev` }
    raised := False;
    try
      kv.Update(KEY_CAS_REV, 'stale', rev);
    except
      on E: EDextNatsKeyRevisionMismatch do
        raised := True;
    end;
    if raised and (kv.Get(KEY_CAS_REV).AsString = '42') then
      PrintPass(Format('Update("%s") stale revision raises EDextNatsKeyRevisionMismatch.',
        [KEY_CAS_REV]))
    else
      PrintFail(Format('Update("%s") stale: expected EDextNatsKeyRevisionMismatch, raised=%s value="%s".',
        [KEY_CAS_REV, BoolToStr(raised, True), kv.Get(KEY_CAS_REV).AsString]));
  except
    on E: Exception do
      PrintFail('CAS Update failed: ' + E.Message);
  end;
end;

procedure RunWatchSteps(kv: TDextNatsKeyValue);
var
  watcher: TDextNatsKeyValueWatcher;
  watchLock: TCriticalSection;
  watchKeys: IList<string>;
  watchReady: TEvent;
  watchOpts: TNatsKeyValueWatchOptions;
  putCount: Integer;
  keyCount: Integer;
begin
  { Step 9a: WatchAll — last-per-subject snapshot, wait for EndOfInitial, Stop. }
  watcher := nil;
  watchLock := TCriticalSection.Create;
  watchKeys := TCollections.CreateList<string>;
  watchReady := TEvent.Create(nil, True, False, '');
  try
    try
      watcher := kv.WatchAll(
        procedure(const AEntry: TNatsKeyValueEntry)
        begin
          if AEntry.IsEndOfInitial then
          begin
            watchReady.SetEvent;
            Exit;
          end;
          if not AEntry.IsPut then
            Exit;
          watchLock.Enter;
          try
            if not KeysContain(watchKeys, AEntry.Key) then
              watchKeys.Add(AEntry.Key);
          finally
            watchLock.Leave;
          end;
        end);

      if watchReady.WaitFor(WATCH_TIMEOUT_MS) = wrSignaled then
      begin
        watchLock.Enter;
        try
          keyCount := watchKeys.Count;
          if watcher.InitialDone and (keyCount >= 3) and
             KeysContain(watchKeys, KEY_COUNTER) and
             KeysContain(watchKeys, KEY_CAS) and
             KeysContain(watchKeys, KEY_CAS_REV) then
            PrintPass(Format(
              'WatchAll EndOfInitial after %d live PUT key(s); InitialDone=True.',
              [keyCount]))
          else
            PrintFail(Format(
              'WatchAll after marker: InitialDone=%s Count=%d (expected counter/cas-lock/cas-rev).',
              [BoolToStr(watcher.InitialDone, True), keyCount]));
        finally
          watchLock.Leave;
        end;
      end
      else
        PrintFail(Format('WatchAll timed out waiting for EndOfInitial (%d ms).',
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

  { Step 9b: IncludeHistory Watch on KEY_COUNTER — full history then EndOfInitial. }
  watcher := nil;
  watchLock := TCriticalSection.Create;
  putCount := 0;
  watchReady := TEvent.Create(nil, True, False, '');
  try
    try
      watchOpts := TNatsKeyValueWatchOptions.CreateDefault;
      watchOpts.IncludeHistory := True;
      watcher := kv.Watch(KEY_COUNTER,
        procedure(const AEntry: TNatsKeyValueEntry)
        begin
          if AEntry.IsEndOfInitial then
          begin
            watchReady.SetEvent;
            Exit;
          end;
          if AEntry.IsPut then
          begin
            watchLock.Enter;
            try
              Inc(putCount);
            finally
              watchLock.Leave;
            end;
          end;
        end,
        watchOpts);

      if watchReady.WaitFor(WATCH_TIMEOUT_MS) = wrSignaled then
      begin
        watchLock.Enter;
        try
          if watcher.InitialDone and (putCount >= 3) then
            PrintPass(Format(
              'IncludeHistory Watch("%s"): %d PUT(s) then EndOfInitial.',
              [KEY_COUNTER, putCount]))
          else
            PrintFail(Format(
              'IncludeHistory Watch("%s"): InitialDone=%s putCount=%d (expected >=3).',
              [KEY_COUNTER, BoolToStr(watcher.InitialDone, True), putCount]));
        finally
          watchLock.Leave;
        end;
      end
      else
        PrintFail(Format(
          'IncludeHistory Watch timed out waiting for EndOfInitial (%d ms).',
          [WATCH_TIMEOUT_MS]));

      watcher.Stop;
      if not watcher.Active then
        PrintPass('IncludeHistory Watch Stop: Active=False.')
      else
        PrintFail('IncludeHistory Watch Stop: Active still True.');
    except
      on E: Exception do
        PrintFail('IncludeHistory Watch failed: ' + E.Message);
    end;
  finally
    FreeAndNil(watcher);
    watchReady.Free;
    watchLock.Free;
  end;
end;

procedure RunOptionalPerKeyTtl(js: TDextNatsJetStreamContext);
var
  bucket: string;
  cfg: TNatsKeyValueConfig;
  kv: TDextNatsKeyValue;
  rev: UInt64;
  entry: TNatsKeyValueEntry;
  elapsed: Integer;
  stillPresent: Boolean;
begin
  { Step 10: optional per-key TTL (NATS 2.11+ / ADR-48). Soft-skip if unsupported. }
  bucket := UniqueBucketName('KVE2ETTL_');
  cfg := TNatsKeyValueConfig.CreateDefault(bucket);
  cfg.Description := 'Dext.Nats KeyValueE2E per-key TTL';
  cfg.History := 1;
  cfg.Storage := ssMemory;
  cfg.LimitMarkerTTL := TTL_NANOS;
  kv := nil;
  try
    try
      kv := TDextNatsKeyValue.CreateBucket(js, cfg);
    except
      on E: EDextNatsJetStreamError do
      begin
        PrintSkip(Format('Per-key TTL unsupported by server (%s). Need nats-server 2.11+.',
          [E.Message]));
        Exit;
      end;
      on E: Exception do
      begin
        PrintSkip(Format('Per-key TTL bucket create failed (%s); skipping.', [E.Message]));
        Exit;
      end;
    end;

    PrintPass(Format('Created TTL bucket "%s" (LimitMarkerTTL=%d ns).',
      [bucket, TTL_NANOS]));

    try
      rev := kv.Create(KEY_TTL, 'ephemeral', TTL_NANOS);
      if (rev = 0) or (not kv.TryGet(KEY_TTL, entry)) or (entry.AsString <> 'ephemeral') then
      begin
        PrintFail(Format('Create("%s", ttl): expected live "ephemeral", rev=%d.',
          [KEY_TTL, Int64(rev)]));
        Exit;
      end;
      PrintPass(Format('Create("%s", ttl=%dns) = "%s" revision=%d.',
        [KEY_TTL, TTL_NANOS, entry.AsString, Int64(rev)]));

      elapsed := 0;
      stillPresent := True;
      while (elapsed < TTL_WAIT_MS) and stillPresent do
      begin
        Sleep(TTL_POLL_MS);
        Inc(elapsed, TTL_POLL_MS);
        stillPresent := kv.TryGet(KEY_TTL, entry);
      end;

      if stillPresent then
        PrintFail(Format('Per-key TTL: key "%s" still present after %dms (expected expire).',
          [KEY_TTL, elapsed]))
      else
        PrintPass(Format('Per-key TTL: key "%s" expired within %dms.', [KEY_TTL, elapsed]));
    except
      on E: Exception do
        PrintFail('Per-key TTL Create/expire failed: ' + E.Message);
    end;
  finally
    FreeAndNil(kv);
    try
      TDextNatsKeyValue.DeleteBucket(js, bucket);
    except
    end;
  end;
end;

procedure RunKeyValueE2E;
var
  host: string;
  port: Word;
  client: TDextNatsClient;
  js: TDextNatsJetStreamContext;
  kv: TDextNatsKeyValue;
  cfg: TNatsKeyValueConfig;
  bucket: string;
  rev, rev2, rev3: UInt64;
  entry: TNatsKeyValueEntry;
  keys: IList<string>;
  hist: IList<TNatsKeyValueEntry>;
  status: TNatsKeyValueStatus;
  deleted: Boolean;
begin
  host := ResolveHost;
  port := ResolvePort;
  bucket := UniqueBucketName;

  Writeln('=== Dext.Nats JetStream Key-Value E2E ===');
  Writeln('NOTE: this requires nats-server WITH JetStream enabled (nats-server -js).');
  Writeln(Format('Target: %s:%d  Bucket: %s', [host, port, bucket]));
  Writeln;

  client := TDextNatsClient.Create;
  js := nil;
  kv := nil;
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

    js := TDextNatsJetStreamContext.Create(client);
    try
      { Step 2: create bucket (unique name; delete if a collision somehow exists). }
      try
        if TDextNatsKeyValue.BucketExists(js, bucket) then
        begin
          PrintInfo(Format('Bucket "%s" already exists; deleting first...', [bucket]));
          TDextNatsKeyValue.DeleteBucket(js, bucket);
        end;

        cfg := TNatsKeyValueConfig.CreateDefault(bucket);
        cfg.Description := 'Dext.Nats KeyValueE2E';
        cfg.History := 5;
        cfg.Storage := ssMemory;
        kv := TDextNatsKeyValue.CreateBucket(js, cfg);
        PrintPass(Format('Created bucket "%s" (stream %s, history=%d, memory).',
          [bucket, kv.StreamName, cfg.History]));
      except
        on E: EDextNatsJetStreamError do
        begin
          PrintFail(Format('CreateBucket failed: %s (Code=%d, ErrCode=%d)',
            [E.Message, E.Code, E.ErrCode]));
          Exit;
        end;
        on E: Exception do
        begin
          PrintFail('CreateBucket failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 3: Put. }
      try
        rev := kv.Put(KEY_WIDGET, '42');
        rev2 := kv.Put(KEY_COUNTER, 'one');
        rev3 := kv.Put(KEY_COUNTER, 'two');
        kv.Put(KEY_COUNTER, 'three');
        kv.Put(KEY_SCRATCH, 'temp');
        PrintPass(Format('Put %s / %s / %s (revisions widget=%d counter last>=%d).',
          [KEY_WIDGET, KEY_COUNTER, KEY_SCRATCH, Int64(rev), Int64(rev3)]));
        if (rev = 0) or (rev2 = 0) then
          PrintFail('Put returned revision 0.');
      except
        on E: Exception do
        begin
          PrintFail('Put failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 4: Get + assert. }
      try
        entry := kv.Get(KEY_WIDGET);
        if (entry.AsString = '42') and entry.IsPut and (entry.Revision = rev) then
          PrintPass(Format('Get("%s") = "%s" revision=%d.',
            [KEY_WIDGET, entry.AsString, Int64(entry.Revision)]))
        else
          PrintFail(Format('Get("%s"): expected "42" rev=%d IsPut, got "%s" rev=%d IsPut=%s.',
            [KEY_WIDGET, Int64(rev), entry.AsString, Int64(entry.Revision),
             BoolToStr(entry.IsPut, True)]));

        entry := kv.Get(KEY_COUNTER);
        if entry.AsString = 'three' then
          PrintPass(Format('Get("%s") latest = "%s".', [KEY_COUNTER, entry.AsString]))
        else
          PrintFail(Format('Get("%s"): expected "three", got "%s".',
            [KEY_COUNTER, entry.AsString]));
      except
        on E: Exception do
        begin
          PrintFail('Get failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 5: Keys. }
      try
        keys := kv.Keys;
        PrintInfo(Format('Keys.Count=%d', [keys.Count]));
        if (keys.Count = 3) and KeysContain(keys, KEY_WIDGET) and
           KeysContain(keys, KEY_COUNTER) and KeysContain(keys, KEY_SCRATCH) then
          PrintPass('Keys lists widget-blue, counter, scratch.')
        else
          PrintFail(Format('Keys expected 3 live keys including %s/%s/%s; Count=%d.',
            [KEY_WIDGET, KEY_COUNTER, KEY_SCRATCH, keys.Count]));
      except
        on E: Exception do
        begin
          PrintFail('Keys failed: ' + E.Message);
          Exit;
        end;
      end;

      { Step 6: History (bucket History=5). }
      try
        hist := kv.History(KEY_COUNTER);
        PrintInfo(Format('History("%s").Count=%d', [KEY_COUNTER, hist.Count]));
        if (hist.Count = 3) and (hist[0].AsString = 'one') and
           (hist[1].AsString = 'two') and (hist[2].AsString = 'three') and
           hist[0].IsPut and (hist[2].Revision > hist[0].Revision) then
          PrintPass('History returns three PUT revisions oldest-first.')
        else if hist.Count >= 3 then
          PrintFail(Format('History unexpected: Count=%d values=[%s,%s,%s].',
            [hist.Count, hist[0].AsString, hist[1].AsString, hist[2].AsString]))
        else
          PrintFail(Format('History unexpected: Count=%d (expected 3).', [hist.Count]));
      except
        on E: Exception do
          PrintFail('History failed: ' + E.Message);
      end;

      { Step 7: Delete + Purge. }
      try
        kv.Delete(KEY_WIDGET);
        if not kv.TryGet(KEY_WIDGET, entry) then
          PrintPass(Format('Delete("%s"): key no longer visible via TryGet.', [KEY_WIDGET]))
        else
          PrintFail(Format('Delete("%s"): TryGet still returned a value.', [KEY_WIDGET]));

        kv.Purge(KEY_SCRATCH);
        if not kv.TryGet(KEY_SCRATCH, entry) then
          PrintPass(Format('Purge("%s"): key no longer visible via TryGet.', [KEY_SCRATCH]))
        else
          PrintFail(Format('Purge("%s"): TryGet still returned a value.', [KEY_SCRATCH]));

        keys := kv.Keys;
        if (keys.Count = 1) and KeysContain(keys, KEY_COUNTER) and
           (not KeysContain(keys, KEY_WIDGET)) and (not KeysContain(keys, KEY_SCRATCH)) then
          PrintPass('Keys after Delete/Purge lists only counter.')
        else
          PrintFail(Format('Keys after Delete/Purge: Count=%d (expected 1: counter).',
            [keys.Count]));
      except
        on E: Exception do
          PrintFail('Delete/Purge failed: ' + E.Message);
      end;

      { Step 8: CAS Create / Update. }
      RunCasSteps(kv);

      { Step 9: WatchAll (EndOfInitial) + IncludeHistory Watch. }
      RunWatchSteps(kv);

      try
        status := kv.Status;
        PrintInfo(Format('Status Bucket=%s Stream=%s Values=%d Bytes=%d',
          [status.Bucket, status.StreamName, Int64(status.Values), Int64(status.Bytes)]));
      except
        on E: Exception do
          PrintFail('Status failed: ' + E.Message);
      end;

      { Step 10: optional per-key TTL (separate bucket; soft-skip if unsupported). }
      RunOptionalPerKeyTtl(js);

      { Step 11: DeleteBucket. }
      try
        FreeAndNil(kv);
        deleted := TDextNatsKeyValue.DeleteBucket(js, bucket);
        if deleted and (not TDextNatsKeyValue.BucketExists(js, bucket)) then
          PrintPass(Format('Deleted bucket "%s".', [bucket]))
        else
          PrintFail(Format('DeleteBucket("%s") deleted=%s exists=%s.',
            [bucket, BoolToStr(deleted, True),
             BoolToStr(TDextNatsKeyValue.BucketExists(js, bucket), True)]));
      except
        on E: EDextNatsJetStreamError do
          PrintFail(Format('DeleteBucket failed: %s (Code=%d, ErrCode=%d)',
            [E.Message, E.Code, E.ErrCode]));
        on E: Exception do
          PrintFail('DeleteBucket failed: ' + E.Message);
      end;
    finally
      FreeAndNil(kv);
      FreeAndNil(js);
    end;

    { Step 12: disconnect. }
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
    RunKeyValueE2E;

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
