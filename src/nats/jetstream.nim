## jetstream.nim -- JetStream API over $JS.API.* subjects.
##
## Stream and consumer CRUD via JSON request/reply.

{.experimental: "strict_funcs".}

import std/json
import basis/code/choice, conn, pub

type
  StreamConfig* = object
    name*: string
    subjects*: seq[string]
    retention*: string      ## "limits", "interest", "workqueue"
    max_msgs*: int64
    max_bytes*: int64
    storage*: string        ## "file" or "memory"

  ConsumerConfig* = object
    name*: string
    durable_name*: string
    filter_subject*: string
    ack_policy*: string     ## "explicit", "none", "all"
    deliver_policy*: string ## "all", "new", "last"

proc stream_config_json*(cfg: StreamConfig): string =
  var j = %*{
    "name": cfg.name,
    "subjects": cfg.subjects,
    "retention": cfg.retention,
    "storage": cfg.storage
  }
  if cfg.max_msgs > 0: j["max_msgs"] = %cfg.max_msgs
  if cfg.max_bytes > 0: j["max_bytes"] = %cfg.max_bytes
  $j

proc consumer_config_json*(cfg: ConsumerConfig): string =
  $(%*{
    "durable_name": cfg.durable_name,
    "filter_subject": cfg.filter_subject,
    "ack_policy": cfg.ack_policy,
    "deliver_policy": cfg.deliver_policy
  })

proc create_stream*(c: NatsConn, cfg: StreamConfig): Choice[string] =
  request(c, "$JS.API.STREAM.CREATE." & cfg.name, stream_config_json(cfg))

proc delete_stream*(c: NatsConn, name: string): Choice[string] =
  request(c, "$JS.API.STREAM.DELETE." & name, "")

proc create_consumer*(c: NatsConn, stream: string, cfg: ConsumerConfig): Choice[string] =
  request(c, "$JS.API.CONSUMER.CREATE." & stream & "." & cfg.durable_name,
          consumer_config_json(cfg))

proc delete_consumer*(c: NatsConn, stream, consumer: string): Choice[string] =
  request(c, "$JS.API.CONSUMER.DELETE." & stream & "." & consumer, "")
