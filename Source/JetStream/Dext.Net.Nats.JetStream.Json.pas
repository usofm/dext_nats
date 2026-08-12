{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream JSON core                                             }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Json;

interface

uses
  System.SysUtils,
  Dext.Json.Utf8;

type
  /// <summary>
  ///   Allocation-conscious UTF-8 sink shared by JetStream JSON codecs.
  ///   This internal implementation replaces the private writer historically
  ///   embedded in Dext.Net.Nats.JetStream.pas.
  /// </summary>
  TDextNatsJsByteWriter = record
  private
    FBuffer: TBytes;
    FLength: Integer;
    procedure EnsureCapacity(AAdditional: Integer);
  public
    procedure Reset;
    procedure WriteBytes(AData: Pointer; ALength: Integer);
    function ToBytes: TBytes;
    function ToUtf8String: string;
  end;

/// <summary>Bridge callback consumed by TUtf8JsonWriter.</summary>
procedure NatsJsUtf8Write(AContext, AData: Pointer; ALength: Integer);

/// <summary>
///   Builds the common body used by STREAM.NAMES/STREAM.LIST and consumer
///   paging APIs. Subject is omitted when empty.
/// </summary>
function NatsJsBuildPagedListRequest(AOffset: Integer;
  const ASubjectFilter: string = ''): string;

/// <summary>Builds STREAM.MSG.GET body using last_by_subj.</summary>
function NatsJsBuildGetLastMessageRequest(const ASubject: string): string;

/// <summary>Builds STREAM.MSG.GET body using stream sequence.</summary>
function NatsJsBuildGetMessageRequest(ASequence: UInt64): string;

implementation

procedure TDextNatsJsByteWriter.Reset;
begin
  FLength := 0;
end;

procedure TDextNatsJsByteWriter.EnsureCapacity(AAdditional: Integer);
var
  Needed: Integer;
  NewCapacity: Integer;
begin
  if AAdditional <= 0 then
    Exit;

  Needed := FLength + AAdditional;
  if Needed <= Length(FBuffer) then
    Exit;

  NewCapacity := Length(FBuffer);
  if NewCapacity < 256 then
    NewCapacity := 256;
  while NewCapacity < Needed do
  begin
    if NewCapacity > (MaxInt div 2) then
    begin
      NewCapacity := Needed;
      Break;
    end;
    NewCapacity := NewCapacity * 2;
  end;
  SetLength(FBuffer, NewCapacity);
end;

procedure TDextNatsJsByteWriter.WriteBytes(AData: Pointer; ALength: Integer);
begin
  if (AData = nil) or (ALength <= 0) then
    Exit;
  EnsureCapacity(ALength);
  Move(AData^, FBuffer[FLength], ALength);
  Inc(FLength, ALength);
end;

function TDextNatsJsByteWriter.ToBytes: TBytes;
begin
  SetLength(Result, FLength);
  if FLength > 0 then
    Move(FBuffer[0], Result[0], FLength);
end;

function TDextNatsJsByteWriter.ToUtf8String: string;
begin
  if FLength = 0 then
    Exit('');
  Result := TEncoding.UTF8.GetString(FBuffer, 0, FLength);
end;

procedure NatsJsUtf8Write(AContext, AData: Pointer; ALength: Integer);
begin
  if (AContext <> nil) and (AData <> nil) and (ALength > 0) then
    PDextNatsJsByteWriter(AContext)^.WriteBytes(AData, ALength);
end;

function NatsJsBuildPagedListRequest(AOffset: Integer;
  const ASubjectFilter: string): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
begin
  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;
  Json.WritePropertyName('offset');
  Json.WriteNumber(AOffset);
  if ASubjectFilter <> '' then
  begin
    Json.WritePropertyName('subject');
    Json.WriteString(ASubjectFilter);
  end;
  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

function NatsJsBuildGetLastMessageRequest(const ASubject: string): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
begin
  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;
  Json.WritePropertyName('last_by_subj');
  Json.WriteString(ASubject);
  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

function NatsJsBuildGetMessageRequest(ASequence: UInt64): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
begin
  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;
  Json.WritePropertyName('seq');
  Json.WriteNumber(Int64(ASequence));
  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

end.
