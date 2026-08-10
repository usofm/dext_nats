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
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Drain.Tests;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats,
  Dext.Net.Nats.Protocol;

type
  /// <summary>
  ///   Focused Drain / IsDraining coverage. Live cases soft-skip when no server
  ///   (same env flags as the main suite: DEXT_NATS_SKIP_LIVE / DEXT_NATS_REQUIRE_LIVE).
  /// </summary>
  [TestFixture('NATS Client Drain')]
  TDextNatsDrainTests = class
  private
    FClient: TDextNatsClient;
    function EnsureServerOrFail: Boolean;
    function UniqueSubject(const APrefix: string): string;
  public
    [SetUp]
    procedure SetUp;
    [TearDown]
    procedure TearDown;

    [Test, Category('Unit')]
    procedure Drain_WhenNotConnected_ShouldBeIdempotent;
    [Test, Category('Unit')]
    procedure IsDraining_WhenIdle_ShouldBeFalse;

    [Test, Category('Integration')]
    procedure Drain_ShouldUnsubscribeFlushAndDisconnect;
    [Test, Category('Integration')]
    procedure Drain_ShouldRejectPublishWhileDraining;
    [Test, Category('Integration')]
    procedure Drain_ShouldDeliverInFlightBeforeClose;
    [Test, Category('Integration')]
    procedure DrainAsync_ShouldAwait;
  end;

implementation

function EnvFlagTrue(const AName: string): Boolean;
var
  v: string;
begin
  v := Trim(GetEnvironmentVariable(AName));
  Result := SameText(v, '1') or SameText(v, 'true') or SameText(v, 'yes');
end;

function NatsTestHost: string;
begin
  Result := Trim(GetEnvironmentVariable('DEXT_NATS_HOST'));
  if Result = '' then
    Result := '127.0.0.1';
end;

function NatsTestPort: Word;
begin
  Result := Word(StrToIntDef(Trim(GetEnvironmentVariable('DEXT_NATS_PORT')),
    NATS_DEFAULT_PORT));
end;

function LiveSoftSkipOrFail(const AReason: string): Boolean;
begin
  if EnvFlagTrue('DEXT_NATS_REQUIRE_LIVE') then
    raise EDextNatsException.Create(AReason);
  Result := False;
end;

function LiveSkippedByEnv: Boolean;
begin
  Result := EnvFlagTrue('DEXT_NATS_SKIP_LIVE');
end;

function TDextNatsDrainTests.UniqueSubject(const APrefix: string): string;
begin
  Result := APrefix + '.' + FormatDateTime('hhnnsszzz', Now) + '.' +
    IntToHex(Random(MaxInt), 8);
end;

function TDextNatsDrainTests.EnsureServerOrFail: Boolean;
begin
  Result := False;
  if LiveSkippedByEnv then
    Exit;
  try
    FClient.Connect(NatsTestHost, NatsTestPort);
    Result := True;
  except
    on E: Exception do
      Result := LiveSoftSkipOrFail(
        Format('NATS server not reachable at %s:%d (%s). Start nats-server, ' +
          'or omit DEXT_NATS_REQUIRE_LIVE for soft-skip.',
          [NatsTestHost, NatsTestPort, E.Message]));
  end;
end;

procedure TDextNatsDrainTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
end;

procedure TDextNatsDrainTests.TearDown;
begin
  if Assigned(FClient) then
  begin
    try
      FClient.Disconnect;
    except
    end;
    FreeAndNil(FClient);
  end;
end;

procedure TDextNatsDrainTests.Drain_WhenNotConnected_ShouldBeIdempotent;
begin
  Should(FClient.Connected).BeFalse;
  Should(FClient.IsDraining).BeFalse;
  FClient.Drain(500);
  Should(FClient.Connected).BeFalse;
  Should(FClient.IsDraining).BeFalse;
end;

procedure TDextNatsDrainTests.IsDraining_WhenIdle_ShouldBeFalse;
begin
  Should(FClient.IsDraining).BeFalse;
end;

procedure TDextNatsDrainTests.Drain_ShouldUnsubscribeFlushAndDisconnect;
var
  subject: string;
  sid: Integer;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.drain.close');
  sid := FClient.Subscribe(subject,
    procedure(const AMsg: TNatsMsg)
    begin
    end);
  Should(sid > 0).BeTrue;
  Should(FClient.SubscriptionCount).Be(1);
  Should(FClient.Connected).BeTrue;

  FClient.Drain(5000);

  Should(FClient.Connected).BeFalse;
  Should(FClient.IsDraining).BeFalse;
  Should(FClient.SubscriptionCount).Be(0);
end;

procedure TDextNatsDrainTests.Drain_ShouldRejectPublishWhileDraining;
var
  subject: string;
  started, rejected: TEvent;
  drainThread: TThread;
  sawReject: Boolean;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.drain.reject');
  started := TEvent.Create(nil, True, False, '');
  rejected := TEvent.Create(nil, True, False, '');
  sawReject := False;
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        // Hold the receive thread so Drain stays in IsDraining while we Publish.
        started.SetEvent;
        Sleep(400);
      end);
    FClient.Publish(subject, 'hold');
    Should(started.WaitFor(3000) = wrSignaled).BeTrue;

    drainThread := TThread.CreateAnonymousThread(
      procedure
      begin
        try
          FClient.Drain(5000);
        except
        end;
      end);
    drainThread.FreeOnTerminate := False;
    drainThread.Start;
    try
      // Give Drain time to set FDraining and send UNSUBs.
      Sleep(50);
      Should(FClient.IsDraining).BeTrue;
      try
        FClient.Publish(subject, 'should-fail');
      except
        on E: EDextNatsException do
        begin
          sawReject := E.Message.Contains('draining');
          rejected.SetEvent;
        end;
      end;
      Should(rejected.WaitFor(2000) = wrSignaled).BeTrue;
      Should(sawReject).BeTrue;
      drainThread.WaitFor;
    finally
      drainThread.Free;
    end;
    Should(FClient.Connected).BeFalse;
    Should(FClient.IsDraining).BeFalse;
  finally
    started.Free;
    rejected.Free;
  end;
end;

procedure TDextNatsDrainTests.Drain_ShouldDeliverInFlightBeforeClose;
var
  subject: string;
  hits: Integer;
  received: TEvent;
begin
  if not EnsureServerOrFail then
    Exit;
  subject := UniqueSubject('dext.nats.test.drain.inflight');
  hits := 0;
  received := TEvent.Create(nil, True, False, '');
  try
    FClient.Subscribe(subject,
      procedure(const AMsg: TNatsMsg)
      begin
        TInterlocked.Increment(hits);
        received.SetEvent;
      end);
    FClient.Publish(subject, 'before-drain');
    Should(received.WaitFor(3000) = wrSignaled).BeTrue;
    Should(hits).Be(1);

    FClient.Drain(5000);
    Should(FClient.Connected).BeFalse;
    Should(hits).Be(1);
  finally
    received.Free;
  end;
end;

procedure TDextNatsDrainTests.DrainAsync_ShouldAwait;
begin
  if not EnsureServerOrFail then
    Exit;
  FClient.Subscribe(UniqueSubject('dext.nats.test.drain.async'),
    procedure(const AMsg: TNatsMsg)
    begin
    end);
  Should(FClient.DrainAsync(5000).Await).BeTrue;
  Should(FClient.Connected).BeFalse;
  Should(FClient.IsDraining).BeFalse;
end;

end.
