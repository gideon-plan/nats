## sub.nim -- Subscribe to subjects with optional queue group. Recv loop.

{.experimental: "strict_funcs".}

import std/atomics
import lattice, proto, conn

type
  MsgHandler* = proc(subject, reply, payload: string) {.gcsafe, raises: [].}

  NatsSub* = ref object
    conn*: NatsConn
    subject*: string
    queue*: string
    sid*: string
    running: Atomic[bool]

proc new_sub*(c: NatsConn, subject: string, sid: string,
              queue: string = ""): NatsSub {.raises: [NatsError].} =
  result = NatsSub(conn: c, subject: subject, queue: queue, sid: sid)
  result.running.store(true)
  let msg = NatsMsg(kind: nmkSub, sub_subject: subject,
                    sub_queue: queue, sub_sid: sid)
  send_msg(c, msg)

proc unsubscribe*(s: NatsSub) {.raises: [NatsError].} =
  send_msg(s.conn, NatsMsg(kind: nmkUnsub, unsub_sid: s.sid))

proc stop*(s: NatsSub) =
  s.running.store(false)

proc run_loop*(s: NatsSub, handler: MsgHandler) {.raises: [].} =
  ## Blocking recv loop. Dispatches messages to handler until stopped.
  while s.running.load():
    let msg = try: recv_msg(s.conn)
              except NatsError: break
    case msg.kind
    of nmkMsg:
      handler(msg.msg_subject, msg.msg_reply, msg.msg_payload)
    of nmkPing:
      try: send_msg(s.conn, NatsMsg(kind: nmkPong))
      except NatsError: break
    else:
      discard
