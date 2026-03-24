## pub.nim -- Publish to subjects. Request-reply with inbox.

{.experimental: "strict_funcs".}

import std/atomics
import lattice, proto, conn

var inbox_counter {.global.}: Atomic[int]

proc next_inbox*(): string =
  let n = inbox_counter.fetchAdd(1)
  "_INBOX." & $n

proc publish*(c: NatsConn, subject: string, payload: string,
              reply: string = "") {.raises: [NatsError].} =
  let msg = NatsMsg(kind: nmkPub, pub_subject: subject,
                    pub_reply: reply, pub_payload: payload)
  send_msg(c, msg)

proc request*(c: NatsConn, subject: string, payload: string,
              timeout_ms: int = 5000): Result[string, NatsError] =
  ## Send request and wait for reply on auto-generated inbox.
  let inbox = next_inbox()
  let sub_msg = NatsMsg(kind: nmkSub, sub_subject: inbox, sub_sid: "req_1")
  try:
    send_msg(c, sub_msg)
    # Auto-unsubscribe after 1 message
    send_msg(c, NatsMsg(kind: nmkUnsub, unsub_sid: "req_1", unsub_max: 1))
    publish(c, subject, payload, inbox)
    let resp = recv_msg(c)
    if resp.kind == nmkMsg:
      Result[string, NatsError].good(resp.msg_payload)
    else:
      Result[string, NatsError].bad(NatsError(msg: "unexpected response: " & $resp.kind))
  except NatsError as e:
    Result[string, NatsError].bad(e[])
