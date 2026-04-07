## jetstream.nim -- JetStream API over $JS.API.* subjects.
##
## Stream and consumer CRUD via JSON request/reply.

{.experimental: "strict_funcs".}

import jsony
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

proc dumpHook*(s: var string; v: StreamConfig) =
  ## Serialize StreamConfig; omit max_msgs/max_bytes when zero.
  s.add '{'
  s.add "\"name\":"; s.dumpHook(v.name)
  s.add ",\"subjects\":"; s.dumpHook(v.subjects)
  s.add ",\"retention\":"; s.dumpHook(v.retention)
  s.add ",\"storage\":"; s.dumpHook(v.storage)
  if v.max_msgs > 0:
    s.add ",\"max_msgs\":"; s.dumpHook(v.max_msgs)
  if v.max_bytes > 0:
    s.add ",\"max_bytes\":"; s.dumpHook(v.max_bytes)
  s.add '}'

proc stream_config_json*(cfg: StreamConfig): string =
  cfg.toJson()

proc consumer_config_json*(cfg: ConsumerConfig): string =
  var s = "{"
  s.add "\"durable_name\":"; s.dumpHook(cfg.durable_name)
  s.add ",\"filter_subject\":"; s.dumpHook(cfg.filter_subject)
  s.add ",\"ack_policy\":"; s.dumpHook(cfg.ack_policy)
  s.add ",\"deliver_policy\":"; s.dumpHook(cfg.deliver_policy)
  s.add '}'
  s

proc create_stream*(c: NatsConn, cfg: StreamConfig): Choice[string] =
  request(c, "$JS.API.STREAM.CREATE." & cfg.name, stream_config_json(cfg))

proc delete_stream*(c: NatsConn, name: string): Choice[string] =
  request(c, "$JS.API.STREAM.DELETE." & name, "")

proc create_consumer*(c: NatsConn, stream: string, cfg: ConsumerConfig): Choice[string] =
  request(c, "$JS.API.CONSUMER.CREATE." & stream & "." & cfg.durable_name,
          consumer_config_json(cfg))

proc delete_consumer*(c: NatsConn, stream, consumer: string): Choice[string] =
  request(c, "$JS.API.CONSUMER.DELETE." & stream & "." & consumer, "")
