{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Cursor parser parity tests                                      }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.ParserV2.Tests;

interface

uses
  System.SysUtils,
  System.Classes,
  Dext.Testing,
  Dext.Testing.Attributes,
  Dext.Testing.Fluent,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Internal.Parser;

type
  [TestFixture('NATS Parser V2 parity')]
  TDextNatsParserV2Tests = class
  private
    procedure AppendText(AV1: TDextNatsFrameParserV2;
      AV2: TDextNatsFrameParserV2; const AText: RawByteString);
    procedure AssertSameFrame(const AV1, AV2: TNatsFrame);
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure ControlFrames_ShouldMatchV1;
    [Test, Category('Unit'), Category('Protocol')]
    procedure Msg_ShouldMatchV1;
    [Test, Category('Unit'), Category('Protocol')]
    procedure FragmentedMsg_ShouldMatchV1;
    [Test, Category('Unit'), Category('Protocol')]
    procedure MultipleFrames_ShouldMatchV1;
    [Test, Category('Unit'), Category('Protocol')]
    procedure HMsg_ShouldMatchV1;
    [Test, Category('Unit'), Category('Protocol')]
    procedure Clear_ShouldDropIncompleteFrame;
    [Test, Category('Unit'), Category('Protocol'), Category('Negative')]
    procedure MaxFrameBytes_ShouldMatchV1;
  end;

implementation

procedure TDextNatsParserV2Tests.AppendText(AV1: TDextNatsFrameParserV2;
  AV2: TDextNatsFrameParserV2; const AText: RawByteString);
var
  B: TBytes;
begin
  B := BytesOf(AText);
  AV1.Append(B, Length(B));
  AV2.Append(B, Length(B));
end;

procedure TDextNatsParserV2Tests.AssertSameFrame(const AV1, AV2: TNatsFrame);
begin
  Should(Ord(AV2.Kind)).Be(Ord(AV1.Kind));
  Should(AV2.Subject).Be(AV1.Subject);
  Should(AV2.ReplyTo).Be(AV1.ReplyTo);
  Should(AV2.Sid).Be(AV1.Sid);
  Should(AV2.StatusCode).Be(AV1.StatusCode);
  Should(AV2.InfoJson).Be(AV1.InfoJson);
  Should(AV2.ErrorText).Be(AV1.ErrorText);
  Should(TEncoding.UTF8.GetString(AV2.Payload)).Be(TEncoding.UTF8.GetString(AV1.Payload));
  Should(Length(AV2.Headers)).Be(Length(AV1.Headers));
  if Length(AV1.Headers) > 0 then
  begin
    Should(AV2.Headers[0].Key).Be(AV1.Headers[0].Key);
    Should(AV2.Headers[0].Value).Be(AV1.Headers[0].Value);
  end;
end;

procedure TDextNatsParserV2Tests.ControlFrames_ShouldMatchV1;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
  F1, F2: TNatsFrame;
  I: Integer;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    AppendText(Baseline, V2, 'PING'#13#10 + 'PONG'#13#10 + '+OK'#13#10 +
      '-ERR ''Permissions Violation'''#13#10 +
      'INFO {"server_id":"A","headers":true}'#13#10);
    for I := 1 to 5 do
    begin
      Should(Baseline.TryReadFrame(F1)).BeTrue;
      Should(V2.TryReadFrame(F2)).BeTrue;
      AssertSameFrame(F1, F2);
    end;
    Should(Baseline.TryReadFrame(F1)).BeFalse;
    Should(V2.TryReadFrame(F2)).BeFalse;
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

procedure TDextNatsParserV2Tests.Msg_ShouldMatchV1;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
  F1, F2: TNatsFrame;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    AppendText(Baseline, V2, 'MSG orders.created 42 _INBOX.reply 5'#13#10'hello'#13#10);
    Should(Baseline.TryReadFrame(F1)).BeTrue;
    Should(V2.TryReadFrame(F2)).BeTrue;
    AssertSameFrame(F1, F2);
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

procedure TDextNatsParserV2Tests.FragmentedMsg_ShouldMatchV1;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
  F1, F2: TNatsFrame;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    AppendText(Baseline, V2, 'MSG foo 7 11'#13);
    Should(Baseline.TryReadFrame(F1)).BeFalse;
    Should(V2.TryReadFrame(F2)).BeFalse;
    AppendText(Baseline, V2, #10'hello ');
    Should(Baseline.TryReadFrame(F1)).BeFalse;
    Should(V2.TryReadFrame(F2)).BeFalse;
    AppendText(Baseline, V2, 'world'#13#10);
    Should(Baseline.TryReadFrame(F1)).BeTrue;
    Should(V2.TryReadFrame(F2)).BeTrue;
    AssertSameFrame(F1, F2);
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

procedure TDextNatsParserV2Tests.MultipleFrames_ShouldMatchV1;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
  F1, F2: TNatsFrame;
  I: Integer;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    AppendText(Baseline, V2, 'MSG a 1 1'#13#10'x'#13#10 +
      'MSG b 2 2'#13#10'yz'#13#10 + 'PING'#13#10 +
      'MSG c 3 3'#13#10'123'#13#10);
    for I := 1 to 4 do
    begin
      Should(Baseline.TryReadFrame(F1)).BeTrue;
      Should(V2.TryReadFrame(F2)).BeTrue;
      AssertSameFrame(F1, F2);
    end;
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

procedure TDextNatsParserV2Tests.HMsg_ShouldMatchV1;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
  F1, F2: TNatsFrame;
  Header, Wire: RawByteString;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    Header := 'NATS/1.0 200 OK'#13#10'Content-Type: text/plain'#13#10#13#10;
    Wire := 'HMSG foo 9 ' + RawByteString(IntToStr(Length(Header))) + ' ' +
      RawByteString(IntToStr(Length(Header) + 4)) + #13#10 + Header + 'data'#13#10;
    AppendText(Baseline, V2, Wire);
    Should(Baseline.TryReadFrame(F1)).BeTrue;
    Should(V2.TryReadFrame(F2)).BeTrue;
    AssertSameFrame(F1, F2);
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

procedure TDextNatsParserV2Tests.Clear_ShouldDropIncompleteFrame;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
  F1, F2: TNatsFrame;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    AppendText(Baseline, V2, 'MSG stale 1 10'#13#10'abc');
    Should(Baseline.TryReadFrame(F1)).BeFalse;
    Should(V2.TryReadFrame(F2)).BeFalse;
    Baseline.Clear;
    V2.Clear;
    AppendText(Baseline, V2, 'PING'#13#10);
    Should(Baseline.TryReadFrame(F1)).BeTrue;
    Should(V2.TryReadFrame(F2)).BeTrue;
    AssertSameFrame(F1, F2);
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

procedure TDextNatsParserV2Tests.MaxFrameBytes_ShouldMatchV1;
var
  Baseline: TDextNatsFrameParserV2;
  V2: TDextNatsFrameParserV2;
begin
  Baseline := TDextNatsFrameParserV2.Create;
  V2 := TDextNatsFrameParserV2.Create;
  try
    Baseline.MaxFrameBytes := 4;
    V2.MaxFrameBytes := 4;
    AppendText(Baseline, V2, 'MSG too.big 1 5'#13#10'hello'#13#10);
    Should(procedure var F: TNatsFrame; begin Baseline.TryReadFrame(F); end).Throw(EDextNatsProtocolError);
    Should(procedure var F: TNatsFrame; begin V2.TryReadFrame(F); end).Throw(EDextNatsProtocolError);
  finally
    V2.Free;
    Baseline.Free;
  end;
end;

end.
