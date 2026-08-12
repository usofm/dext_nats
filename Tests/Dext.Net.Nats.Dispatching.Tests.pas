{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Bounded subscription dispatch tests                             }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.Dispatching.Tests;

interface

uses
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats,
  Dext.Net.Nats.Dispatching;

type
  [TestFixture('NATS Dispatched Subscription')]
  TDextNatsDispatchingTests = class
  public
    [Test, Category('Unit'), Category('Concurrency')]
    procedure Options_ShouldHaveBoundedDefaults;
    [Test, Category('Unit'), Category('Concurrency')]
    procedure Lifecycle_ShouldRegisterAndUnregisterWithoutConnect;
  end;

implementation

procedure TDextNatsDispatchingTests.Options_ShouldHaveBoundedDefaults;
var
  Options: TNatsDispatchOptions;
begin
  Options := TNatsDispatchOptions.CreateDefault;
  Should(Options.WorkerCount > 0).BeTrue;
  Should(Options.Capacity > 0).BeTrue;
  Should(Ord(Options.FullMode)).Be(Ord(dfmReject));
end;

procedure TDextNatsDispatchingTests.Lifecycle_ShouldRegisterAndUnregisterWithoutConnect;
var
  Client: TDextNatsClient;
  Subscription: TDextNatsDispatchedSubscription;
begin
  Client := TDextNatsClient.Create;
  try
    Should(Client.SubscriptionCount).Be(0);
    Subscription := TDextNatsDispatchedSubscription.Create(
      Client,
      'tests.dispatch',
      procedure(const AMsg: TNatsMsg)
      begin
      end);
    try
      Should(Subscription.IsActive).BeTrue;
      Should(Client.SubscriptionCount).Be(1);
      Subscription.Stop;
      Should(Subscription.IsActive).BeFalse;
      Should(Client.SubscriptionCount).Be(0);
    finally
      Subscription.Free;
    end;
  finally
    Client.Free;
  end;
end;

end.
