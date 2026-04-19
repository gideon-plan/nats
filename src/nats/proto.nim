## proto.nim -- NATS text protocol codec.
##
## Messages: INFO, CONNECT, PUB, HPUB, SUB, UNSUB, MSG, HMSG, PING, PONG, +OK, -ERR

{.experimental: "strict_funcs".}

import std/strutils
import basis/code/choice

type
  NatsMsgKind* {.pure.} = enum
    Info, Connect, Pub, Hpub, Sub, Unsub,
    Msg, Hmsg, Ping, Pong, Ok, Err

  NatsMsg* = object
    case kind*: NatsMsgKind
    of NatsMsgKind.Info:
      info_json*: string
    of NatsMsgKind.Connect:
      connect_json*: string
    of NatsMsgKind.Pub:
      pub_subject*: string
      pub_reply*: string
      pub_payload*: string
    of NatsMsgKind.Hpub:
      hpub_subject*: string
      hpub_reply*: string
      hpub_headers*: string
      hpub_payload*: string
    of NatsMsgKind.Sub:
      sub_subject*: string
      sub_queue*: string
      sub_sid*: string
    of NatsMsgKind.Unsub:
      unsub_sid*: string
      unsub_max*: int
    of NatsMsgKind.Msg:
      msg_subject*: string
      msg_sid*: string
      msg_reply*: string
      msg_payload*: string
    of NatsMsgKind.Hmsg:
      hmsg_subject*: string
      hmsg_sid*: string
      hmsg_reply*: string
      hmsg_headers*: string
      hmsg_payload*: string
    of NatsMsgKind.Ping, NatsMsgKind.Pong, NatsMsgKind.Ok:
      discard
    of NatsMsgKind.Err:
      err_msg*: string

# =====================================================================================================================
# Encode
# =====================================================================================================================

proc encode*(msg: NatsMsg): string =
  case msg.kind
  of NatsMsgKind.Connect:
    "CONNECT " & msg.connect_json & "\r\n"
  of NatsMsgKind.Pub:
    if msg.pub_reply.len > 0:
      "PUB " & msg.pub_subject & " " & msg.pub_reply & " " & $msg.pub_payload.len & "\r\n" & msg.pub_payload & "\r\n"
    else:
      "PUB " & msg.pub_subject & " " & $msg.pub_payload.len & "\r\n" & msg.pub_payload & "\r\n"
  of NatsMsgKind.Sub:
    if msg.sub_queue.len > 0:
      "SUB " & msg.sub_subject & " " & msg.sub_queue & " " & msg.sub_sid & "\r\n"
    else:
      "SUB " & msg.sub_subject & " " & msg.sub_sid & "\r\n"
  of NatsMsgKind.Unsub:
    if msg.unsub_max > 0:
      "UNSUB " & msg.unsub_sid & " " & $msg.unsub_max & "\r\n"
    else:
      "UNSUB " & msg.unsub_sid & "\r\n"
  of NatsMsgKind.Ping: "PING\r\n"
  of NatsMsgKind.Pong: "PONG\r\n"
  else:
    ""  # Server-originated messages not encoded by client

# =====================================================================================================================
# Decode
# =====================================================================================================================

proc decode_line*(line: string): Choice[NatsMsg] =
  ## Decode a single NATS protocol line (without payload).
  let trimmed = line.strip()
  if trimmed == "PING":
    return good(NatsMsg(kind: NatsMsgKind.Ping))
  if trimmed == "PONG":
    return good(NatsMsg(kind: NatsMsgKind.Pong))
  if trimmed == "+OK":
    return good(NatsMsg(kind: NatsMsgKind.Ok))
  if trimmed.startsWith("-ERR"):
    let msg = if trimmed.len > 5: trimmed[5..^1].strip().strip(chars = {'\'', '"'}) else: ""
    return good(NatsMsg(kind: NatsMsgKind.Err, err_msg: msg))
  if trimmed.startsWith("INFO "):
    return good(
      NatsMsg(kind: NatsMsgKind.Info, info_json: trimmed[5..^1]))
  if trimmed.startsWith("MSG "):
    let parts = trimmed[4..^1].strip().split(' ')
    if parts.len == 3:
      return good(
        NatsMsg(kind: NatsMsgKind.Msg, msg_subject: parts[0], msg_sid: parts[1],
                msg_payload: ""))  # payload read separately
    elif parts.len == 4:
      return good(
        NatsMsg(kind: NatsMsgKind.Msg, msg_subject: parts[0], msg_sid: parts[1],
                msg_reply: parts[2], msg_payload: ""))
  bad[NatsMsg]("nats", "unknown NATS message: " & trimmed)
