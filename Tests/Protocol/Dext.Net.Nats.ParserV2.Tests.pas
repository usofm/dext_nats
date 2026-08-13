{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           Production cursor parser tests                                  }
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
  Dext.Core.Span,
  Dext.Net.Nats.Protocol,
  Dext.Net.Nats.Internal.Parser;

type
  [TestFixture('NATS Production Parser')]
  TDextNatsParserV2Tests = class
  private
    procedure AppendText(AParser: TDextNatsFrameParserV2;
      const AText: RawByteString);
  public
    [Test, Category('Unit'), Category('Protocol')]
    procedure ControlFrames_ShouldDecode;
    [Test, Category('Unit'), Category('Protocol')]
    procedure Msg_ShouldDecodeReplyAndPayload;
    [Test, Category('Unit'), Category('Protocol')]
    procedure FragmentedMsg_ShouldWaitForCompletion;
    [Test, Category('Unit'), Category('Protocol')]
    procedure MultipleFrames_ShouldDecodeInOrder;
    [Test, Category('Unit'), Category('Protocol')]
    procedure HMsg_ShouldDecodeStatusHeadersAndPayload;
    [Test, Category('Unit'), Category('Protocol')]
    procedure Clear_ShouldDropIncompleteFrame;
    [Test, Category('Unit'), Category('Protocol')]
    procedure PrepareReceive_ShouldDecodeLikeAppend;
    [Test, Category('Unit'), Category('Protocol'), Category('Negative')]
    procedure MaxFrameBytes_ShouldRaise;
    [Test, Category('Unit'), Category('Protocol')]
    procedure Msg_ShouldBorrowPayloadSpanWithoutOwnedCopy;
    [Test, Category('Unit'), Category('Protocol')]
    procedure CopyPayload_ShouldSurviveSubsequentParserClear;
  end;

implementation

procedure TDextNatsParserV2Tests.AppendText(AParser: TDextNatsFrameParserV2;
  const AText: RawByteString);
var
  B: TBytes;
begin
  B := BytesOf(AText);
  AParser.Append(B, Length(B));
end;

procedure TDextNatsParserV2Tests.ControlFrames_ShouldDecode;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'PING'#13#10 + 'PONG'#13#10 + '+OK'#13#10 +
      '-ERR ''Permissions Violation'''#13#10 +
      'INFO {"server_id":"A","headers":true}'#13#10);

    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfPing));
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfPong));
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfOK));
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfErr));
    Should(Frame.ErrorText).Be('Permissions Violation');
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfInfo));
    Should(Frame.InfoJson).Be('{"server_id":"A","headers":true}');
    Should(Parser.TryReadFrame(Frame)).BeFalse;
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.Msg_ShouldDecodeReplyAndPayload;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'MSG orders.created 42 _INBOX.reply 5'#13#10'hello'#13#10);
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfMsg));
    Should(Frame.Subject).Be('orders.created');
    Should(Frame.ReplyTo).Be('_INBOX.reply');
    Should(Frame.Sid).Be(42);
    Should(Length(Frame.Payload)).Be(0);
    Should(TEncoding.UTF8.GetString(Frame.CopyPayload)).Be('hello');
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.FragmentedMsg_ShouldWaitForCompletion;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'MSG foo 7 11'#13);
    Should(Parser.TryReadFrame(Frame)).BeFalse;
    AppendText(Parser, #10'hello ');
    Should(Parser.TryReadFrame(Frame)).BeFalse;
    AppendText(Parser, 'world'#13#10);
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Frame.Subject).Be('foo');
    Should(Frame.Sid).Be(7);
    Should(Length(Frame.Payload)).Be(0);
    Should(TEncoding.UTF8.GetString(Frame.CopyPayload)).Be('hello world');
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.MultipleFrames_ShouldDecodeInOrder;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'MSG a 1 1'#13#10'x'#13#10 +
      'MSG b 2 2'#13#10'yz'#13#10 + 'PING'#13#10 +
      'MSG c 3 3'#13#10'123'#13#10);

    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Frame.Subject).Be('a');
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Frame.Subject).Be('b');
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfPing));
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Frame.Subject).Be('c');
    Should(Parser.TryReadFrame(Frame)).BeFalse;
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.HMsg_ShouldDecodeStatusHeadersAndPayload;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
  Header, Wire: RawByteString;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    Header := 'NATS/1.0 200 OK'#13#10'Content-Type: text/plain'#13#10#13#10;
    Wire := 'HMSG foo 9 ' + RawByteString(IntToStr(Length(Header))) + ' ' +
      RawByteString(IntToStr(Length(Header) + 4)) + #13#10 + Header + 'data'#13#10;
    AppendText(Parser, Wire);

    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfHMsg));
    Should(Frame.Subject).Be('foo');
    Should(Frame.Sid).Be(9);
    Should(Frame.StatusCode).Be(200);
    Should(Length(Frame.Headers)).Be(1);
    Should(Frame.Headers[0].Key).Be('Content-Type');
    Should(Frame.Headers[0].Value).Be('text/plain');
    Should(Length(Frame.Payload)).Be(0);
    Should(TEncoding.UTF8.GetString(Frame.CopyPayload)).Be('data');
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.Clear_ShouldDropIncompleteFrame;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'MSG stale 1 10'#13#10'abc');
    Should(Parser.TryReadFrame(Frame)).BeFalse;
    Parser.Clear;
    AppendText(Parser, 'PING'#13#10);
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfPing));
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.PrepareReceive_ShouldDecodeLikeAppend;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
  Dest: TByteSpan;
  Wire: RawByteString;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    Wire := 'PING'#13#10'PONG'#13#10;
    Dest := Parser.PrepareReceive(Length(Wire));
    Should(Dest.Length >= Length(Wire)).BeTrue;
    Move(Wire[1], Dest.Data^, Length(Wire));
    Parser.CommitReceive(Length(Wire));
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfPing));
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Ord(Frame.Kind)).Be(Ord(nfPong));
    Should(Parser.TryReadFrame(Frame)).BeFalse;
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.MaxFrameBytes_ShouldRaise;
var
  Parser: TDextNatsFrameParserV2;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    Parser.MaxFrameBytes := 4;
    AppendText(Parser, 'MSG too.big 1 5'#13#10'hello'#13#10);
    Should(procedure var F: TNatsFrame; begin Parser.TryReadFrame(F); end)
      .Throw(EDextNatsProtocolError);
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.Msg_ShouldBorrowPayloadSpanWithoutOwnedCopy;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'MSG foo 1 5'#13#10'hello'#13#10);
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(Length(Frame.Payload)).Be(0);
    Should(Frame.PayloadSpan.Length).Be(5);
    Should(Frame.PayloadSpan.EqualsString('hello')).BeTrue;
    Should(Assigned(Frame.PayloadSpan.Data)).BeTrue;
  finally
    Parser.Free;
  end;
end;

procedure TDextNatsParserV2Tests.CopyPayload_ShouldSurviveSubsequentParserClear;
var
  Parser: TDextNatsFrameParserV2;
  Frame: TNatsFrame;
  Kept: TBytes;
begin
  Parser := TDextNatsFrameParserV2.Create;
  try
    AppendText(Parser, 'MSG foo 1 5'#13#10'hello'#13#10);
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Kept := Frame.CopyPayload;
    Parser.Clear;
    AppendText(Parser, 'MSG bar 2 5'#13#10'xxxxx'#13#10);
    Should(Parser.TryReadFrame(Frame)).BeTrue;
    Should(TEncoding.UTF8.GetString(Kept)).Be('hello');
    Should(TEncoding.UTF8.GetString(Frame.CopyPayload)).Be('xxxxx');
  finally
    Parser.Free;
  end;
end;

end.
