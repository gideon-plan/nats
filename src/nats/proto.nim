## proto.nim -- NATS text protocol codec.
##
## Messages: INFO, CONNECT, PUB, HPUB, SUB, UNSUB, MSG, HMSG, PING, PONG, +OK, -ERR

{.experimental: "strict_funcs".}

import std/strutils
import basis/code/choice

type
  NatsMsgKind* = enum
    nmkInfo, nmkConnect, nmkPub, nmkHpub, nmkSub, nmkUnsub,
    nmkMsg, nmkHmsg, nmkPing, nmkPong, nmkOk, nmkErr

  NatsMsg* = object
    case kind*: NatsMsgKind
    of nmkInfo:
      info_json*: string
    of nmkConnect:
      connect_json*: string
    of nmkPub:
      pub_subject*: string
      pub_reply*: string
      pub_payload*: string
    of nmkHpub:
      hpub_subject*: string
      hpub_reply*: string
      hpub_headers*: string
      hpub_payload*: string
    of nmkSub:
      sub_subject*: string
      sub_queue*: string
      sub_sid*: string
    of nmkUnsub:
      unsub_sid*: string
      unsub_max*: int
    of nmkMsg:
      msg_subject*: string
      msg_sid*: string
      msg_reply*: string
      msg_payload*: string
    of nmkHmsg:
      hmsg_subject*: string
      hmsg_sid*: string
      hmsg_reply*: string
      hmsg_headers*: string
      hmsg_payload*: string
    of nmkPing, nmkPong, nmkOk:
      discard
    of nmkErr:
      err_msg*: string

# =====================================================================================================================
# Encode
# =====================================================================================================================

proc encode*(msg: NatsMsg): string =
  case msg.kind
  of nmkConnect:
    "CONNECT " & msg.connect_json & "\r\n"
  of nmkPub:
    if msg.pub_reply.len > 0:
      "PUB " & msg.pub_subject & " " & msg.pub_reply & " " & $msg.pub_payload.len & "\r\n" & msg.pub_payload & "\r\n"
    else:
      "PUB " & msg.pub_subject & " " & $msg.pub_payload.len & "\r\n" & msg.pub_payload & "\r\n"
  of nmkSub:
    if msg.sub_queue.len > 0:
      "SUB " & msg.sub_subject & " " & msg.sub_queue & " " & msg.sub_sid & "\r\n"
    else:
      "SUB " & msg.sub_subject & " " & msg.sub_sid & "\r\n"
  of nmkUnsub:
    if msg.unsub_max > 0:
      "UNSUB " & msg.unsub_sid & " " & $msg.unsub_max & "\r\n"
    else:
      "UNSUB " & msg.unsub_sid & "\r\n"
  of nmkPing: "PING\r\n"
  of nmkPong: "PONG\r\n"
  else:
    ""  # Server-originated messages not encoded by client

# =====================================================================================================================
# Decode
# =====================================================================================================================

proc decode_line*(line: string): Choice[NatsMsg] =
  ## Decode a single NATS protocol line (without payload).
  let trimmed = line.strip()
  if trimmed == "PING":
    return good(NatsMsg(kind: nmkPing))
  if trimmed == "PONG":
    return good(NatsMsg(kind: nmkPong))
  if trimmed == "+OK":
    return good(NatsMsg(kind: nmkOk))
  if trimmed.startsWith("-ERR"):
    let msg = if trimmed.len > 5: trimmed[5..^1].strip().strip(chars = {'\'', '"'}) else: ""
    return good(NatsMsg(kind: nmkErr, err_msg: msg))
  if trimmed.startsWith("INFO "):
    return good(
      NatsMsg(kind: nmkInfo, info_json: trimmed[5..^1]))
  if trimmed.startsWith("MSG "):
    let parts = trimmed[4..^1].strip().split(' ')
    if parts.len == 3:
      return good(
        NatsMsg(kind: nmkMsg, msg_subject: parts[0], msg_sid: parts[1],
                msg_payload: ""))  # payload read separately
    elif parts.len == 4:
      return good(
        NatsMsg(kind: nmkMsg, msg_subject: parts[0], msg_sid: parts[1],
                msg_reply: parts[2], msg_payload: ""))
  bad[NatsMsg]("nats", "unknown NATS message: " & trimmed)
