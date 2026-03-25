## tnats.nim -- Tests for NATS protocol codec (no live server needed).
{.experimental: "strict_funcs".}
import std/[unittest, strutils]
import basis/code/choice
import nats/[proto, jetstream]

suite "proto encode":
  test "encode PING":
    check encode(NatsMsg(kind: nmkPing)) == "PING\r\n"

  test "encode PONG":
    check encode(NatsMsg(kind: nmkPong)) == "PONG\r\n"

  test "encode PUB":
    let msg = NatsMsg(kind: nmkPub, pub_subject: "test", pub_payload: "hello")
    let enc = encode(msg)
    check enc.startsWith("PUB test")
    check enc.contains("5")
    check enc.endsWith("hello\r\n")

  test "encode PUB with reply":
    let msg = NatsMsg(kind: nmkPub, pub_subject: "test", pub_reply: "inbox.1",
                      pub_payload: "hi")
    let enc = encode(msg)
    check enc.contains("inbox.1")

  test "encode SUB":
    let msg = NatsMsg(kind: nmkSub, sub_subject: "events.>", sub_sid: "1")
    check encode(msg) == "SUB events.> 1\r\n"

  test "encode SUB with queue":
    let msg = NatsMsg(kind: nmkSub, sub_subject: "work", sub_queue: "workers",
                      sub_sid: "2")
    check encode(msg) == "SUB work workers 2\r\n"

  test "encode UNSUB":
    check encode(NatsMsg(kind: nmkUnsub, unsub_sid: "1")) == "UNSUB 1\r\n"

  test "encode UNSUB with max":
    check encode(NatsMsg(kind: nmkUnsub, unsub_sid: "1", unsub_max: 5)) == "UNSUB 1 5\r\n"

  test "encode CONNECT":
    let msg = NatsMsg(kind: nmkConnect, connect_json: "{\"verbose\":false}")
    check encode(msg) == "CONNECT {\"verbose\":false}\r\n"

suite "proto decode":
  test "decode PING":
    let result = decode_line("PING")
    check result.is_good
    check result.val.kind == nmkPing

  test "decode PONG":
    let result = decode_line("PONG")
    check result.is_good
    check result.val.kind == nmkPong

  test "decode +OK":
    let result = decode_line("+OK")
    check result.is_good
    check result.val.kind == nmkOk

  test "decode -ERR":
    let result = decode_line("-ERR 'Authorization Violation'")
    check result.is_good
    check result.val.kind == nmkErr
    check result.val.err_msg.contains("Authorization")

  test "decode INFO":
    let result = decode_line("INFO {\"server_id\":\"test\"}")
    check result.is_good
    check result.val.kind == nmkInfo
    check result.val.info_json.contains("server_id")

  test "decode MSG":
    let result = decode_line("MSG test.subject 1 5")
    check result.is_good
    check result.val.kind == nmkMsg
    check result.val.msg_subject == "test.subject"
    check result.val.msg_sid == "1"

  test "decode MSG with reply":
    let result = decode_line("MSG test.subject 1 reply.to 5")
    check result.is_good
    check result.val.msg_reply == "reply.to"

  test "decode unknown":
    let result = decode_line("UNKNOWN command")
    check result.is_bad

suite "jetstream config":
  test "stream config json":
    let cfg = StreamConfig(name: "ORDERS", subjects: @["orders.>"],
                           retention: "workqueue", storage: "file")
    let j = stream_config_json(cfg)
    check j.contains("ORDERS")
    check j.contains("workqueue")

  test "consumer config json":
    let cfg = ConsumerConfig(durable_name: "worker1", filter_subject: "orders.*",
                             ack_policy: "explicit", deliver_policy: "all")
    let j = consumer_config_json(cfg)
    check j.contains("worker1")
    check j.contains("explicit")
