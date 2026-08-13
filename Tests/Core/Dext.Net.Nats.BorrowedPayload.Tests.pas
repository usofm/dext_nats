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
{***************************************************************************}
unit Dext.Net.Nats.BorrowedPayload.Tests;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Dext.Core.Span,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.JetStream;

type
  [TestFixture('NATS Borrowed Payload Span')]
  TDextNatsBorrowedPayloadTests = class
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
    procedure BindBorrowed_ShouldExposeSpanAndLeavePayloadEmpty;
    [Test, Category('Unit')]
    procedure CloneOwned_ShouldCopyBorrowedBytes;
    [Test, Category('Unit')]
    procedure FromNatsMsg_ShouldCopyBorrowedPayload;
    [Test, Category('Integration')]
    procedure SubscribeInline_ShouldBorrowPayloadDuringHandler;
    [Test, Category('Integration')]
    procedure Subscribe_ShouldDeliverOwnedPayload;
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

function TDextNatsBorrowedPayloadTests.UniqueSubject(const APrefix: string): string;
begin
  Result := APrefix + '.' + FormatDateTime('hhnnsszzz', Now) + '.' +
    IntToHex(Random(MaxInt), 8);
end;

function TDextNatsBorrowedPayloadTests.EnsureServerOrFail: Boolean;
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

procedure TDextNatsBorrowedPayloadTests.SetUp;
begin
  FClient := TDextNatsClient.Create;
end;

procedure TDextNatsBorrowedPayloadTests.TearDown;
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

procedure TDextNatsBorrowedPayloadTests.BindBorrowed_ShouldExposeSpanAndLeavePayloadEmpty;
var
  Msg: TNatsMsg;
  Bytes: TBytes;
  Span: TByteSpan;
begin
  Bytes := TEncoding.UTF8.GetBytes('hello-span');
  Span := TByteSpan.FromBytes(Bytes);
  Msg := Default(TNatsMsg);
  Msg.BindBorrowedPayload(Span);

  Should(Msg.HasBorrowedPayload).BeTrue;
  Should(Length(Msg.Payload)).Be(0);
  Should(Msg.PayloadSpan.Length).Be(Length(Bytes));
  Should(Msg.PayloadSpan.EqualsString('hello-span')).BeTrue;
  Should(Msg.AsString).Be('hello-span');
  Should(TEncoding.UTF8.GetString(Msg.CopyPayload)).Be('hello-span');
end;

procedure TDextNatsBorrowedPayloadTests.CloneOwned_ShouldCopyBorrowedBytes;
var
  Msg, Owned: TNatsMsg;
  Bytes: TBytes;
begin
  Bytes := TEncoding.UTF8.GetBytes('keep-me');
  Msg := Default(TNatsMsg);
  Msg.Subject := 'orders.1';
  Msg.BindBorrowedPayload(TByteSpan.FromBytes(Bytes));

  Owned := Msg.CloneOwned;
  Should(Owned.HasBorrowedPayload).BeFalse;
  Should(Owned.Subject).Be('orders.1');
  Should(TEncoding.UTF8.GetString(Owned.Payload)).Be('keep-me');
  Should(Owned.PayloadSpan.EqualsString('keep-me')).BeTrue;
end;

procedure TDextNatsBorrowedPayloadTests.FromNatsMsg_ShouldCopyBorrowedPayload;
var
  Raw: TNatsMsg;
  Js: TNatsJsMsg;
  Bytes: TBytes;
begin
  Bytes := TEncoding.UTF8.GetBytes('js-body');
  Raw := Default(TNatsMsg);
  Raw.Subject := 'orders.1';
  Raw.StatusCode := 200;
  Raw.BindBorrowedPayload(TByteSpan.FromBytes(Bytes));
  Should(Length(Raw.Payload)).Be(0);

  Js := TNatsJsMsg.FromNatsMsg(Raw);
  Should(TEncoding.UTF8.GetString(Js.Payload)).Be('js-body');
  Should(Js.AsString).Be('js-body');
end;

procedure TDextNatsBorrowedPayloadTests.SubscribeInline_ShouldBorrowPayloadDuringHandler;
var
  Subject: string;
  Sid: Integer;
  Done: TEvent;
  Borrowed: Boolean;
  OwnedLen, SpanLen: Integer;
  Text: string;
  Kept: TBytes;
begin
  if not EnsureServerOrFail then
    Exit;

  Done := TEvent.Create(nil, True, False, '');
  try
    Borrowed := False;
    OwnedLen := -1;
    SpanLen := -1;
    Text := '';
    Subject := UniqueSubject('dext.nats.test.borrow.inline');
    Sid := FClient.SubscribeInline(Subject,
      procedure(const AMsg: TNatsMsg)
      begin
        Borrowed := AMsg.HasBorrowedPayload;
        OwnedLen := Length(AMsg.Payload);
        SpanLen := AMsg.PayloadSpan.Length;
        Text := AMsg.AsString;
        Kept := AMsg.CopyPayload;
        Done.SetEvent;
      end);
    FClient.Publish(Subject, 'hello-borrow');
    FClient.Flush(2000);
    Should(Done.WaitFor(2000) = wrSignaled).BeTrue;
    Should(Borrowed).BeTrue;
    Should(OwnedLen).Be(0);
    Should(SpanLen).Be(Length(TEncoding.UTF8.GetBytes('hello-borrow')));
    Should(Text).Be('hello-borrow');
    Should(TEncoding.UTF8.GetString(Kept)).Be('hello-borrow');
    FClient.Unsubscribe(Sid);
  finally
    Done.Free;
  end;
end;

procedure TDextNatsBorrowedPayloadTests.Subscribe_ShouldDeliverOwnedPayload;
var
  Subject: string;
  Sid: Integer;
  Done: TEvent;
  Borrowed: Boolean;
  OwnedLen: Integer;
  Text: string;
begin
  if not EnsureServerOrFail then
    Exit;

  Done := TEvent.Create(nil, True, False, '');
  try
    Borrowed := True;
    OwnedLen := 0;
    Text := '';
    Subject := UniqueSubject('dext.nats.test.borrow.queued');
    Sid := FClient.Subscribe(Subject,
      procedure(const AMsg: TNatsMsg)
      begin
        Borrowed := AMsg.HasBorrowedPayload;
        OwnedLen := Length(AMsg.Payload);
        Text := AMsg.AsString;
        Done.SetEvent;
      end);
    FClient.Publish(Subject, 'hello-owned');
    FClient.Flush(2000);
    Should(Done.WaitFor(2000) = wrSignaled).BeTrue;
    Should(Borrowed).BeFalse;
    Should(OwnedLen).Be(Length(TEncoding.UTF8.GetBytes('hello-owned')));
    Should(Text).Be('hello-owned');
    FClient.Unsubscribe(Sid);
  finally
    Done.Free;
  end;
end;

end.
