## consumer.nim -- Pull consumer. Fetch batch, ack, nak, in-progress.

{.experimental: "strict_funcs".}

import std/json
import basis/code/choice, proto, conn, pub

type
  PullConsumer* = object
    conn*: NatsConn
    stream*: string
    consumer_name*: string
    fetch_subject*: string

  FetchedMsg* = object
    subject*: string
    payload*: string
    reply*: string  ## Ack reply-to

proc new_pull_consumer*(c: NatsConn, stream, consumer_name: string): PullConsumer =
  PullConsumer(conn: c, stream: stream, consumer_name: consumer_name,
               fetch_subject: "$JS.API.CONSUMER.MSG.NEXT." & stream & "." & consumer_name)

proc fetch*(pc: PullConsumer, batch: int = 1): Choice[seq[FetchedMsg]] =
  ## Request a batch of messages from the pull consumer.
  let payload = $(%*{"batch": batch})
  let inbox = next_inbox()
  try:
    send_msg(pc.conn, NatsMsg(kind: nmkSub, sub_subject: inbox, sub_sid: "fetch_1"))
    send_msg(pc.conn, NatsMsg(kind: nmkUnsub, unsub_sid: "fetch_1", unsub_max: batch))
    publish(pc.conn, pc.fetch_subject, payload, inbox)
    var msgs: seq[FetchedMsg]
    for i in 0 ..< batch:
      let resp = recv_msg(pc.conn)
      if resp.kind == nmkMsg:
        msgs.add(FetchedMsg(subject: resp.msg_subject, payload: resp.msg_payload,
                            reply: resp.msg_reply))
      else:
        break
    good(msgs)
  except NatsError as e:
    bad[seq[FetchedMsg]]("nats", e.msg)

proc ack*(pc: PullConsumer, msg: FetchedMsg) {.raises: [NatsError].} =
  if msg.reply.len > 0:
    publish(pc.conn, msg.reply, "+ACK")

proc nak*(pc: PullConsumer, msg: FetchedMsg) {.raises: [NatsError].} =
  if msg.reply.len > 0:
    publish(pc.conn, msg.reply, "-NAK")

proc in_progress*(pc: PullConsumer, msg: FetchedMsg) {.raises: [NatsError].} =
  if msg.reply.len > 0:
    publish(pc.conn, msg.reply, "+WPI")
