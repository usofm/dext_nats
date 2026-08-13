{***************************************************************************}
{                                                                           }
{           Dext.Nats                                                       }
{                                                                           }
{           JetStream configuration codecs                                  }
{                                                                           }
{***************************************************************************}
unit Dext.Net.Nats.JetStream.Codecs;

interface

uses
  Dext.Net.Nats.JetStream;

/// <summary>Encodes STREAM.CREATE / STREAM.UPDATE configuration JSON.</summary>
function NatsJsEncodeStreamConfig(const AConfig: TNatsStreamConfig): string;

/// <summary>Encodes CONSUMER.CREATE configuration JSON.</summary>
function NatsJsEncodeConsumerConfig(const AConfig: TNatsConsumerConfig): string;

/// <summary>Encodes STREAM.PURGE request JSON.</summary>
function NatsJsEncodePurgeRequest(const ARequest: TNatsStreamPurgeRequest): string;

implementation

uses
  System.SysUtils,
  Dext.Json.Utf8,
  Dext.Net.Nats.JetStream.Json;

procedure WriteStreamSource(var AWriter: TUtf8JsonWriter;
  const ASource: TNatsStreamSource);
var
  I: Integer;
begin
  AWriter.WriteStartObject;
  AWriter.WritePropertyName('name');
  AWriter.WriteString(ASource.Name);

  if ASource.OptStartSeq > 0 then
  begin
    AWriter.WritePropertyName('opt_start_seq');
    AWriter.WriteNumber(Int64(ASource.OptStartSeq));
  end;
  if ASource.OptStartTime <> '' then
  begin
    AWriter.WritePropertyName('opt_start_time');
    AWriter.WriteString(ASource.OptStartTime);
  end;
  if ASource.FilterSubject <> '' then
  begin
    AWriter.WritePropertyName('filter_subject');
    AWriter.WriteString(ASource.FilterSubject);
  end;

  if Length(ASource.SubjectTransforms) > 0 then
  begin
    AWriter.WritePropertyName('subject_transforms');
    AWriter.WriteStartArray;
    for I := 0 to High(ASource.SubjectTransforms) do
      if ASource.SubjectTransforms[I].IsSet then
      begin
        AWriter.WriteStartObject;
        if ASource.SubjectTransforms[I].Source <> '' then
        begin
          AWriter.WritePropertyName('src');
          AWriter.WriteString(ASource.SubjectTransforms[I].Source);
        end;
        AWriter.WritePropertyName('dest');
        AWriter.WriteString(ASource.SubjectTransforms[I].Destination);
        AWriter.WriteEndObject;
      end;
    AWriter.WriteEndArray;
  end;

  if ASource.ExternalStream.IsSet then
  begin
    AWriter.WritePropertyName('external');
    AWriter.WriteStartObject;
    AWriter.WritePropertyName('api');
    AWriter.WriteString(ASource.ExternalStream.ApiPrefix);
    if ASource.ExternalStream.DeliverPrefix <> '' then
    begin
      AWriter.WritePropertyName('deliver');
      AWriter.WriteString(ASource.ExternalStream.DeliverPrefix);
    end;
    AWriter.WriteEndObject;
  end;

  AWriter.WriteEndObject;
end;

function NatsJsEncodeStreamConfig(const AConfig: TNatsStreamConfig): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
  I: Integer;
  RetentionStr: string;
  StorageStr: string;
  DiscardStr: string;
begin
  case AConfig.Retention of
    srLimits: RetentionStr := 'limits';
    srInterest: RetentionStr := 'interest';
    srWorkQueue: RetentionStr := 'workqueue';
  else
    RetentionStr := 'limits';
  end;

  case AConfig.Storage of
    ssFile: StorageStr := 'file';
    ssMemory: StorageStr := 'memory';
  else
    StorageStr := 'file';
  end;

  case AConfig.Discard of
    sdOld: DiscardStr := 'old';
    sdNew: DiscardStr := 'new';
  else
    DiscardStr := 'old';
  end;

  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;
  Json.WritePropertyName('name');
  Json.WriteString(AConfig.Name);

  if AConfig.Description <> '' then
  begin
    Json.WritePropertyName('description');
    Json.WriteString(AConfig.Description);
  end;

  Json.WritePropertyName('subjects');
  Json.WriteStartArray;
  for I := 0 to High(AConfig.Subjects) do
    Json.WriteString(AConfig.Subjects[I]);
  Json.WriteEndArray;

  Json.WritePropertyName('retention');
  Json.WriteString(RetentionStr);
  Json.WritePropertyName('storage');
  Json.WriteString(StorageStr);
  Json.WritePropertyName('max_consumers');
  Json.WriteNumber(AConfig.MaxConsumers);
  Json.WritePropertyName('max_msgs');
  Json.WriteNumber(AConfig.MaxMsgs);
  Json.WritePropertyName('max_bytes');
  Json.WriteNumber(AConfig.MaxBytes);
  Json.WritePropertyName('max_age');
  Json.WriteNumber(AConfig.MaxAge);
  Json.WritePropertyName('max_msg_size');
  Json.WriteNumber(AConfig.MaxMsgSize);
  Json.WritePropertyName('discard');
  Json.WriteString(DiscardStr);
  Json.WritePropertyName('num_replicas');
  Json.WriteNumber(AConfig.NumReplicas);
  Json.WritePropertyName('duplicate_window');
  Json.WriteNumber(AConfig.DuplicateWindow);

  if AConfig.MaxMsgsPerSubject > 0 then
  begin
    Json.WritePropertyName('max_msgs_per_subject');
    Json.WriteNumber(AConfig.MaxMsgsPerSubject);
  end;
  if AConfig.AllowDirect then
  begin
    Json.WritePropertyName('allow_direct');
    Json.WriteBoolean(True);
  end;
  if AConfig.DenyDelete then
  begin
    Json.WritePropertyName('deny_delete');
    Json.WriteBoolean(True);
  end;
  if AConfig.AllowRollup then
  begin
    Json.WritePropertyName('allow_rollup_hdrs');
    Json.WriteBoolean(True);
  end;
  if AConfig.AllowMsgTTL then
  begin
    Json.WritePropertyName('allow_msg_ttl');
    Json.WriteBoolean(True);
  end;
  if AConfig.SubjectDeleteMarkerTTL > 0 then
  begin
    Json.WritePropertyName('subject_delete_marker_ttl');
    Json.WriteNumber(AConfig.SubjectDeleteMarkerTTL);
  end;
  if AConfig.Sealed then
  begin
    Json.WritePropertyName('sealed');
    Json.WriteBoolean(True);
  end;
  if AConfig.Compression = scS2 then
  begin
    Json.WritePropertyName('compression');
    Json.WriteString('s2');
  end;

  if AConfig.Placement.IsSet then
  begin
    Json.WritePropertyName('placement');
    Json.WriteStartObject;
    Json.WritePropertyName('cluster');
    Json.WriteString(AConfig.Placement.Cluster);
    if Length(AConfig.Placement.Tags) > 0 then
    begin
      Json.WritePropertyName('tags');
      Json.WriteStartArray;
      for I := 0 to High(AConfig.Placement.Tags) do
        Json.WriteString(AConfig.Placement.Tags[I]);
      Json.WriteEndArray;
    end;
    Json.WriteEndObject;
  end;

  if AConfig.Mirror.IsSet then
  begin
    Json.WritePropertyName('mirror');
    WriteStreamSource(Json, AConfig.Mirror);
  end;

  if AConfig.MirrorDirect then
  begin
    Json.WritePropertyName('mirror_direct');
    Json.WriteBoolean(True);
  end;

  if Length(AConfig.Sources) > 0 then
  begin
    Json.WritePropertyName('sources');
    Json.WriteStartArray;
    for I := 0 to High(AConfig.Sources) do
      if AConfig.Sources[I].IsSet then
        WriteStreamSource(Json, AConfig.Sources[I]);
    Json.WriteEndArray;
  end;

  if AConfig.RePublish.IsSet then
  begin
    Json.WritePropertyName('republish');
    Json.WriteStartObject;
    if AConfig.RePublish.Source <> '' then
    begin
      Json.WritePropertyName('src');
      Json.WriteString(AConfig.RePublish.Source);
    end;
    Json.WritePropertyName('dest');
    Json.WriteString(AConfig.RePublish.Destination);
    if AConfig.RePublish.HeadersOnly then
    begin
      Json.WritePropertyName('headers_only');
      Json.WriteBoolean(True);
    end;
    Json.WriteEndObject;
  end;

  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

function NatsJsEncodeConsumerConfig(const AConfig: TNatsConsumerConfig): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
  DeliverStr: string;
  AckStr: string;
  ReplayStr: string;
  I: Integer;
begin
  case AConfig.DeliverPolicy of
    dpAll: DeliverStr := 'all';
    dpLast: DeliverStr := 'last';
    dpNew: DeliverStr := 'new';
    dpByStartSequence: DeliverStr := 'by_start_sequence';
    dpByStartTime: DeliverStr := 'by_start_time';
    dpLastPerSubject: DeliverStr := 'last_per_subject';
  else
    DeliverStr := 'all';
  end;

  case AConfig.AckPolicy of
    apNone: AckStr := 'none';
    apAll: AckStr := 'all';
    apExplicit: AckStr := 'explicit';
  else
    AckStr := 'explicit';
  end;

  case AConfig.ReplayPolicy of
    rpInstant: ReplayStr := 'instant';
    rpOriginal: ReplayStr := 'original';
  else
    ReplayStr := 'instant';
  end;

  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;

  if AConfig.DurableName <> '' then
  begin
    Json.WritePropertyName('durable_name');
    Json.WriteString(AConfig.DurableName);
  end;
  if AConfig.Name <> '' then
  begin
    Json.WritePropertyName('name');
    Json.WriteString(AConfig.Name);
  end;
  if AConfig.Description <> '' then
  begin
    Json.WritePropertyName('description');
    Json.WriteString(AConfig.Description);
  end;

  if Length(AConfig.FilterSubjects) > 0 then
  begin
    Json.WritePropertyName('filter_subjects');
    Json.WriteStartArray;
    for I := 0 to High(AConfig.FilterSubjects) do
      Json.WriteString(AConfig.FilterSubjects[I]);
    Json.WriteEndArray;
  end
  else if AConfig.FilterSubject <> '' then
  begin
    Json.WritePropertyName('filter_subject');
    Json.WriteString(AConfig.FilterSubject);
  end;

  if AConfig.DeliverSubject <> '' then
  begin
    Json.WritePropertyName('deliver_subject');
    Json.WriteString(AConfig.DeliverSubject);
  end;
  if AConfig.DeliverGroup <> '' then
  begin
    Json.WritePropertyName('deliver_group');
    Json.WriteString(AConfig.DeliverGroup);
  end;

  Json.WritePropertyName('deliver_policy');
  Json.WriteString(DeliverStr);
  if (AConfig.DeliverPolicy = dpByStartSequence) and (AConfig.OptStartSeq > 0) then
  begin
    Json.WritePropertyName('opt_start_seq');
    Json.WriteNumber(Int64(AConfig.OptStartSeq));
  end;

  Json.WritePropertyName('ack_policy');
  Json.WriteString(AckStr);
  if AConfig.AckWait > 0 then
  begin
    Json.WritePropertyName('ack_wait');
    Json.WriteNumber(AConfig.AckWait);
  end;

  Json.WritePropertyName('max_deliver');
  Json.WriteNumber(AConfig.MaxDeliver);
  Json.WritePropertyName('max_ack_pending');
  Json.WriteNumber(AConfig.MaxAckPending);

  if AConfig.DeliverSubject = '' then
  begin
    Json.WritePropertyName('max_waiting');
    Json.WriteNumber(AConfig.MaxWaiting);
  end;

  Json.WritePropertyName('replay_policy');
  Json.WriteString(ReplayStr);

  if AConfig.HeadersOnly then
  begin
    Json.WritePropertyName('headers_only');
    Json.WriteBoolean(True);
  end;
  if AConfig.FlowControl then
  begin
    Json.WritePropertyName('flow_control');
    Json.WriteBoolean(True);
  end;
  if AConfig.IdleHeartbeat > 0 then
  begin
    Json.WritePropertyName('idle_heartbeat');
    Json.WriteNumber(AConfig.IdleHeartbeat);
  end;
  if AConfig.InactiveThreshold > 0 then
  begin
    Json.WritePropertyName('inactive_threshold');
    Json.WriteNumber(AConfig.InactiveThreshold);
  end;
  if AConfig.MemoryStorage then
  begin
    Json.WritePropertyName('mem_storage');
    Json.WriteBoolean(True);
  end;
  if AConfig.NumReplicas > 0 then
  begin
    Json.WritePropertyName('num_replicas');
    Json.WriteNumber(AConfig.NumReplicas);
  end;

  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

function NatsJsEncodePurgeRequest(const ARequest: TNatsStreamPurgeRequest): string;
var
  Writer: TDextNatsJsByteWriter;
  Json: TUtf8JsonWriter;
begin
  Writer.Reset;
  Json := TUtf8JsonWriter.Create(@Writer, NatsJsUtf8Write, False);
  Json.WriteStartObject;
  if ARequest.Subject <> '' then
  begin
    Json.WritePropertyName('filter');
    Json.WriteString(ARequest.Subject);
  end;
  if ARequest.Sequence > 0 then
  begin
    Json.WritePropertyName('seq');
    Json.WriteNumber(Int64(ARequest.Sequence));
  end;
  if ARequest.Keep > 0 then
  begin
    Json.WritePropertyName('keep');
    Json.WriteNumber(Int64(ARequest.Keep));
  end;
  Json.WriteEndObject;
  Result := Writer.ToUtf8String;
end;

end.
