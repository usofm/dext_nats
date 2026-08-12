{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream response parsers                                      }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Parsers;

interface

uses
  Dext.Net.Nats.JetStream;

function NatsJsParseStreamInfo(const AJson: string): TNatsStreamInfo;
function NatsJsParseConsumerInfo(const AJson: string): TNatsConsumerInfo;
function NatsJsParseStoredMsg(const AJson: string): TNatsStoredMsg;
function NatsJsParsePublishAck(const AJson: string): TNatsPublishAck;
function NatsJsParseSuccess(const AJson: string): Boolean;

implementation

uses
  System.SysUtils,
  System.NetEncoding,
  Dext.Core.Span,
  Dext.Json.Utf8,
  Dext.Net.Nats.Protocol;

procedure SkipValue(var AReader: TUtf8JsonReader);
begin
  if AReader.TokenType in [TJsonTokenType.StartObject, TJsonTokenType.StartArray] then
    AReader.Skip;
end;

procedure HandlePropertyValue(var AReader: TUtf8JsonReader);
begin
  if AReader.Read then
    SkipValue(AReader);
end;

function OpenReader(const AJson, AEmptyMessage: string;
  out ABytes: TBytes): TUtf8JsonReader;
var
  Span: TByteSpan;
begin
  if Trim(AJson) = '' then
    raise EDextNatsProtocolError.Create(AEmptyMessage);
  ABytes := TEncoding.UTF8.GetBytes(AJson);
  if Length(ABytes) = 0 then
    raise EDextNatsProtocolError.Create(AEmptyMessage);
  Span := TByteSpan.Create(@ABytes[0], Length(ABytes));
  Result := TUtf8JsonReader.Create(Span);
end;

procedure RaiseFromErrorObject(var AReader: TUtf8JsonReader);
var
  Code: Integer;
  ErrCode: Integer;
  Description: string;
begin
  Code := 0;
  ErrCode := 0;
  Description := '';

  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        Code := AReader.GetInt32
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('err_code') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        ErrCode := AReader.GetInt32
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('description') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        Description := AReader.GetString
      else
        SkipValue(AReader);
    end
    else
      HandlePropertyValue(AReader);
  end;

  raise EDextNatsJetStreamError.CreateFromApi(Code, ErrCode, Description);
end;

procedure ParseStringArray(var AReader: TUtf8JsonReader; out AValues: TArray<string>);
var
  Values: TArray<string>;
begin
  SetLength(Values, 0);
  if AReader.TokenType <> TJsonTokenType.StartArray then
  begin
    SkipValue(AReader);
    AValues := Values;
    Exit;
  end;

  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndArray then
      Break;
    if AReader.TokenType = TJsonTokenType.StringValue then
    begin
      SetLength(Values, Length(Values) + 1);
      Values[High(Values)] := AReader.GetString;
    end
    else
      SkipValue(AReader);
  end;
  AValues := Values;
end;

procedure ParseSubjectTransform(var AReader: TUtf8JsonReader;
  out ATransform: TNatsSubjectTransform);
begin
  ATransform := Default(TNatsSubjectTransform);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('src') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ATransform.Source := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('dest') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ATransform.Destination := AReader.GetString
      else
        SkipValue(AReader);
    end
    else
      HandlePropertyValue(AReader);
  end;
end;

procedure ParseExternalStream(var AReader: TUtf8JsonReader;
  out AExternal: TNatsExternalStream);
begin
  AExternal := Default(TNatsExternalStream);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;
    if AReader.ValueSpanEquals('api') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AExternal.ApiPrefix := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('deliver') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AExternal.DeliverPrefix := AReader.GetString
      else
        SkipValue(AReader);
    end
    else
      HandlePropertyValue(AReader);
  end;
end;

procedure ParseStreamSource(var AReader: TUtf8JsonReader;
  out ASource: TNatsStreamSource);
var
  Transform: TNatsSubjectTransform;
  Transforms: TArray<TNatsSubjectTransform>;
begin
  ASource := Default(TNatsStreamSource);
  SetLength(Transforms, 0);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('name') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ASource.Name := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('opt_start_seq') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then
        ASource.OptStartSeq := UInt64(AReader.GetInt64)
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('opt_start_time') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ASource.OptStartTime := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('filter_subject') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ASource.FilterSubject := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('subject_transforms') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StartArray) then
      begin
        while AReader.Read do
        begin
          if AReader.TokenType = TJsonTokenType.EndArray then
            Break;
          if AReader.TokenType = TJsonTokenType.StartObject then
          begin
            ParseSubjectTransform(AReader, Transform);
            if Transform.IsSet then
            begin
              SetLength(Transforms, Length(Transforms) + 1);
              Transforms[High(Transforms)] := Transform;
            end;
          end
          else
            SkipValue(AReader);
        end;
        ASource.SubjectTransforms := Transforms;
      end
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('external') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StartObject) then
        ParseExternalStream(AReader, ASource.ExternalStream)
      else
        SkipValue(AReader);
    end
    else
      HandlePropertyValue(AReader);
  end;
end;

procedure ParseRePublish(var AReader: TUtf8JsonReader;
  out ARePublish: TNatsRePublish);
begin
  ARePublish := Default(TNatsRePublish);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('src') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ARePublish.Source := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('dest') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        ARePublish.Destination := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('headers_only') then
    begin
      if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue, TJsonTokenType.FalseValue]) then
        ARePublish.HeadersOnly := AReader.GetBoolean
      else
        SkipValue(AReader);
    end
    else
      HandlePropertyValue(AReader);
  end;
end;

procedure ParsePlacement(var AReader: TUtf8JsonReader;
  out APlacement: TNatsPlacement);
begin
  APlacement := Default(TNatsPlacement);
  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('cluster') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        APlacement.Cluster := AReader.GetString
      else
        SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('tags') then
    begin
      if AReader.Read then
        ParseStringArray(AReader, APlacement.Tags);
    end
    else
      HandlePropertyValue(AReader);
  end;
end;

procedure ParseStreamConfig(var AReader: TUtf8JsonReader;
  out AConfig: TNatsStreamConfig);
var
  S: string;
  Source: TNatsStreamSource;
  Sources: TArray<TNatsStreamSource>;
begin
  AConfig := Default(TNatsStreamConfig);
  AConfig.MaxConsumers := -1;
  AConfig.MaxMsgs := -1;
  AConfig.MaxBytes := -1;
  AConfig.MaxMsgSize := -1;
  AConfig.NumReplicas := 1;
  AConfig.Retention := srLimits;
  AConfig.Storage := ssFile;
  AConfig.Discard := sdOld;
  AConfig.Compression := scNone;
  SetLength(Sources, 0);

  while AReader.Read do
  begin
    if AReader.TokenType = TJsonTokenType.EndObject then
      Break;
    if AReader.TokenType <> TJsonTokenType.PropertyName then
      Continue;

    if AReader.ValueSpanEquals('name') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AConfig.Name := AReader.GetString
      else SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('description') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
        AConfig.Description := AReader.GetString
      else SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('subjects') then
    begin
      if AReader.Read then ParseStringArray(AReader, AConfig.Subjects);
    end
    else if AReader.ValueSpanEquals('retention') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        S := AReader.GetString;
        if SameText(S, 'interest') then AConfig.Retention := srInterest
        else if SameText(S, 'workqueue') then AConfig.Retention := srWorkQueue
        else AConfig.Retention := srLimits;
      end else SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('storage') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        if SameText(AReader.GetString, 'memory') then AConfig.Storage := ssMemory
        else AConfig.Storage := ssFile;
      end else SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('discard') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) then
      begin
        if SameText(AReader.GetString, 'new') then AConfig.Discard := sdNew
        else AConfig.Discard := sdOld;
      end else SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('max_consumers') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.MaxConsumers := AReader.GetInt32 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('max_msgs') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.MaxMsgs := AReader.GetInt64 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('max_bytes') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.MaxBytes := AReader.GetInt64 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('max_age') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.MaxAge := AReader.GetInt64 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('max_msg_size') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.MaxMsgSize := AReader.GetInt32 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('num_replicas') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.NumReplicas := AReader.GetInt32 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('duplicate_window') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.DuplicateWindow := AReader.GetInt64 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('max_msgs_per_subject') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.MaxMsgsPerSubject := AReader.GetInt64 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('allow_direct') then
    begin if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then AConfig.AllowDirect := AReader.GetBoolean else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('deny_delete') then
    begin if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then AConfig.DenyDelete := AReader.GetBoolean else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('allow_rollup_hdrs') then
    begin if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then AConfig.AllowRollup := AReader.GetBoolean else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('allow_msg_ttl') then
    begin if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then AConfig.AllowMsgTTL := AReader.GetBoolean else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('subject_delete_marker_ttl') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.Number) then AConfig.SubjectDeleteMarkerTTL := AReader.GetInt64 else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('sealed') then
    begin if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then AConfig.Sealed := AReader.GetBoolean else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('compression') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StringValue) and SameText(AReader.GetString,'s2') then AConfig.Compression := scS2
      else AConfig.Compression := scNone;
    end
    else if AReader.ValueSpanEquals('placement') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.StartObject) then ParsePlacement(AReader,AConfig.Placement) else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('mirror') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.StartObject) then ParseStreamSource(AReader,AConfig.Mirror) else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('mirror_direct') then
    begin if AReader.Read and (AReader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then AConfig.MirrorDirect := AReader.GetBoolean else SkipValue(AReader); end
    else if AReader.ValueSpanEquals('sources') then
    begin
      if AReader.Read and (AReader.TokenType = TJsonTokenType.StartArray) then
      begin
        while AReader.Read do
        begin
          if AReader.TokenType = TJsonTokenType.EndArray then Break;
          if AReader.TokenType = TJsonTokenType.StartObject then
          begin
            ParseStreamSource(AReader, Source);
            if Source.IsSet then
            begin
              SetLength(Sources, Length(Sources)+1);
              Sources[High(Sources)] := Source;
            end;
          end else SkipValue(AReader);
        end;
        AConfig.Sources := Sources;
      end else SkipValue(AReader);
    end
    else if AReader.ValueSpanEquals('republish') then
    begin if AReader.Read and (AReader.TokenType = TJsonTokenType.StartObject) then ParseRePublish(AReader,AConfig.RePublish) else SkipValue(AReader); end
    else HandlePropertyValue(AReader);
  end;
end;

function NatsJsParseStreamInfo(const AJson: string): TNatsStreamInfo;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
begin
  Result := Default(TNatsStreamInfo);
  try
    Reader := OpenReader(AJson, 'Empty JetStream API response', Bytes);
    if (not Reader.Read) or (Reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s', [AJson]);

    while Reader.Read do
    begin
      if Reader.TokenType = TJsonTokenType.EndObject then Break;
      if Reader.TokenType <> TJsonTokenType.PropertyName then Continue;
      if Reader.ValueSpanEquals('error') then
      begin
        if Reader.Read and (Reader.TokenType = TJsonTokenType.StartObject) then RaiseFromErrorObject(Reader)
        else SkipValue(Reader);
      end
      else if Reader.ValueSpanEquals('config') then
      begin
        if Reader.Read and (Reader.TokenType = TJsonTokenType.StartObject) then
        begin
          ParseStreamConfig(Reader, Result.Config);
          if Result.Name = '' then Result.Name := Result.Config.Name;
        end else SkipValue(Reader);
      end
      else if Reader.ValueSpanEquals('state') then
      begin
        if Reader.Read and (Reader.TokenType = TJsonTokenType.StartObject) then
          while Reader.Read do
          begin
            if Reader.TokenType = TJsonTokenType.EndObject then Break;
            if Reader.TokenType <> TJsonTokenType.PropertyName then Continue;
            if Reader.ValueSpanEquals('messages') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.Messages:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('bytes') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.Bytes:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('first_seq') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.FirstSeq:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('last_seq') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.LastSeq:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('consumer_count') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.ConsumerCount:=Reader.GetInt32 else SkipValue(Reader); end
            else HandlePropertyValue(Reader);
          end
        else SkipValue(Reader);
      end
      else HandlePropertyValue(Reader);
    end;
  except
    on E: EDextNatsProtocolError do raise;
    on E: EDextNatsJetStreamError do raise;
    on E: EJsonException do raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s',[AJson]);
  end;
end;

function NatsJsParseConsumerInfo(const AJson: string): TNatsConsumerInfo;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
begin
  Result := Default(TNatsConsumerInfo);
  try
    Reader := OpenReader(AJson, 'Empty JetStream API response', Bytes);
    if (not Reader.Read) or (Reader.TokenType <> TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s',[AJson]);
    while Reader.Read do
    begin
      if Reader.TokenType=TJsonTokenType.EndObject then Break;
      if Reader.TokenType<>TJsonTokenType.PropertyName then Continue;
      if Reader.ValueSpanEquals('error') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StartObject) then RaiseFromErrorObject(Reader) else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('stream_name') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.StreamName:=Reader.GetString else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('name') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.Name:=Reader.GetString else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('num_pending') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.NumPending:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('num_ack_pending') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.NumAckPending:=Reader.GetInt32 else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('num_redelivered') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.NumRedelivered:=Reader.GetInt32 else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('num_waiting') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.NumWaiting:=Reader.GetInt32 else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('config') then
      begin
        if Reader.Read and (Reader.TokenType=TJsonTokenType.StartObject) then
          while Reader.Read do
          begin
            if Reader.TokenType=TJsonTokenType.EndObject then Break;
            if Reader.TokenType<>TJsonTokenType.PropertyName then Continue;
            if Reader.ValueSpanEquals('durable_name') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.DurableName:=Reader.GetString else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('filter_subject') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.FilterSubject:=Reader.GetString else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('deliver_subject') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.DeliverSubject:=Reader.GetString else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('deliver_group') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.DeliverGroup:=Reader.GetString else SkipValue(Reader); end
            else HandlePropertyValue(Reader);
          end
        else SkipValue(Reader);
      end
      else HandlePropertyValue(Reader);
    end;
    if Result.Name='' then Result.Name:=Result.DurableName;
  except
    on E: EDextNatsProtocolError do raise;
    on E: EDextNatsJetStreamError do raise;
    on E: EJsonException do raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s',[AJson]);
  end;
end;

function NatsJsParseStoredMsg(const AJson: string): TNatsStoredMsg;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
  DataB64, HeadersB64, HeaderBlock: string;
  Status: Integer;
begin
  Result := Default(TNatsStoredMsg);
  DataB64 := '';
  HeadersB64 := '';
  try
    Reader := OpenReader(AJson,'Empty JetStream STREAM.MSG.GET response',Bytes);
    if (not Reader.Read) or (Reader.TokenType<>TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed STREAM.MSG.GET response: %s',[AJson]);
    while Reader.Read do
    begin
      if Reader.TokenType=TJsonTokenType.EndObject then Break;
      if Reader.TokenType<>TJsonTokenType.PropertyName then Continue;
      if Reader.ValueSpanEquals('error') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StartObject) then RaiseFromErrorObject(Reader) else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('message') then
      begin
        if Reader.Read and (Reader.TokenType=TJsonTokenType.StartObject) then
          while Reader.Read do
          begin
            if Reader.TokenType=TJsonTokenType.EndObject then Break;
            if Reader.TokenType<>TJsonTokenType.PropertyName then Continue;
            if Reader.ValueSpanEquals('subject') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.Subject:=Reader.GetString else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('seq') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.Sequence:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('data') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then DataB64:=Reader.GetString else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('hdrs') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then HeadersB64:=Reader.GetString else SkipValue(Reader); end
            else if Reader.ValueSpanEquals('time') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.TimeStamp:=Reader.GetString else SkipValue(Reader); end
            else HandlePropertyValue(Reader);
          end
        else SkipValue(Reader);
      end
      else HandlePropertyValue(Reader);
    end;
    if DataB64<>'' then Result.Data:=TNetEncoding.Base64.DecodeStringToBytes(DataB64);
    if HeadersB64<>'' then
    begin
      HeaderBlock:=TEncoding.UTF8.GetString(TNetEncoding.Base64.DecodeStringToBytes(HeadersB64));
      NatsParseHeaderBlock(HeaderBlock,Result.Headers,Status);
    end;
  except
    on E: EDextNatsProtocolError do raise;
    on E: EDextNatsJetStreamError do raise;
    on E: EJsonException do raise EDextNatsProtocolError.CreateFmt('Malformed STREAM.MSG.GET response: %s',[AJson]);
  end;
end;

function NatsJsParsePublishAck(const AJson: string): TNatsPublishAck;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
begin
  Result:=Default(TNatsPublishAck);
  try
    Reader:=OpenReader(AJson,'Empty JetStream publish acknowledgement payload',Bytes);
    if (not Reader.Read) or (Reader.TokenType<>TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream publish acknowledgement payload: %s',[AJson]);
    while Reader.Read do
    begin
      if Reader.TokenType=TJsonTokenType.EndObject then Break;
      if Reader.TokenType<>TJsonTokenType.PropertyName then Continue;
      if Reader.ValueSpanEquals('error') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StartObject) then RaiseFromErrorObject(Reader) else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('stream') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.Stream:=Reader.GetString else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('seq') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.Number) then Result.Sequence:=UInt64(Reader.GetInt64) else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('duplicate') then begin if Reader.Read and (Reader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then Result.Duplicate:=Reader.GetBoolean else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('domain') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StringValue) then Result.Domain:=Reader.GetString else SkipValue(Reader); end
      else HandlePropertyValue(Reader);
    end;
  except
    on E: EDextNatsProtocolError do raise;
    on E: EDextNatsJetStreamError do raise;
    on E: EJsonException do raise EDextNatsProtocolError.CreateFmt('Malformed JetStream publish acknowledgement payload: %s',[AJson]);
  end;
end;

function NatsJsParseSuccess(const AJson: string): Boolean;
var
  Bytes: TBytes;
  Reader: TUtf8JsonReader;
begin
  Result:=False;
  try
    Reader:=OpenReader(AJson,'Empty JetStream API response',Bytes);
    if (not Reader.Read) or (Reader.TokenType<>TJsonTokenType.StartObject) then
      raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s',[AJson]);
    while Reader.Read do
    begin
      if Reader.TokenType=TJsonTokenType.EndObject then Break;
      if Reader.TokenType<>TJsonTokenType.PropertyName then Continue;
      if Reader.ValueSpanEquals('error') then begin if Reader.Read and (Reader.TokenType=TJsonTokenType.StartObject) then RaiseFromErrorObject(Reader) else SkipValue(Reader); end
      else if Reader.ValueSpanEquals('success') then begin if Reader.Read and (Reader.TokenType in [TJsonTokenType.TrueValue,TJsonTokenType.FalseValue]) then Result:=Reader.GetBoolean else SkipValue(Reader); end
      else HandlePropertyValue(Reader);
    end;
  except
    on E: EDextNatsProtocolError do raise;
    on E: EDextNatsJetStreamError do raise;
    on E: EJsonException do raise EDextNatsProtocolError.CreateFmt('Malformed JetStream API response: %s',[AJson]);
  end;
end;

end.
