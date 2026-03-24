## conn.nim -- NATS TCP connection, INFO/CONNECT handshake, keepalive.

{.experimental: "strict_funcs".}

import std/[net, strutils]
import lattice, proto

type
  NatsConn* = ref object
    sock*: Socket
    server_info*: string
    verbose*: bool

proc read_line(sock: Socket): string {.raises: [NatsError].} =
  result = ""
  while true:
    var c: char
    let n = try: sock.recv(addr c, 1)
            except OSError as e: raise newException(NatsError, "recv: " & e.msg)
            except TimeoutError as e: raise newException(NatsError, "timeout: " & e.msg)
    if n == 0: raise newException(NatsError, "connection closed")
    if c == '\n':
      if result.len > 0 and result[^1] == '\r':
        result.setLen(result.len - 1)
      return
    result.add(c)

proc send_raw(conn: NatsConn, data: string) {.raises: [NatsError].} =
  try: conn.sock.send(data)
  except OSError as e: raise newException(NatsError, "send: " & e.msg)

proc read_payload(sock: Socket, n: int): string {.raises: [NatsError].} =
  result = ""
  while result.len < n:
    let buf = try: sock.recv(n - result.len)
              except OSError as e: raise newException(NatsError, "recv: " & e.msg)
              except TimeoutError as e: raise newException(NatsError, "timeout: " & e.msg)
    if buf.len == 0: raise newException(NatsError, "connection closed")
    result.add(buf)
  # Read trailing \r\n
  discard try: sock.recv(2)
          except CatchableError: ""

proc open_nats*(host: string, port: int = 4222, verbose: bool = false,
                name: string = "gideon-nats"): NatsConn {.raises: [NatsError].} =
  result = NatsConn(verbose: verbose)
  try:
    result.sock = newSocket()
    result.sock.connect(host, Port(port))
  except OSError as e:
    raise newException(NatsError, "connect: " & e.msg)
  # Read INFO
  let info_line = read_line(result.sock)
  let info = decode_line(info_line)
  if info.is_bad: raise newException(NatsError, "handshake: " & info.err.msg)
  if info.val.kind != nmkInfo: raise newException(NatsError, "expected INFO")
  result.server_info = info.val.info_json
  # Send CONNECT
  let connect_json = "{\"verbose\":" & $verbose & ",\"name\":\"" & name & "\",\"protocol\":1}"
  send_raw(result, encode(NatsMsg(kind: nmkConnect, connect_json: connect_json)))
  if verbose:
    let ok_line = read_line(result.sock)
    let ok = decode_line(ok_line)
    if ok.is_bad or ok.val.kind != nmkOk:
      raise newException(NatsError, "expected +OK after CONNECT")

proc send_msg*(conn: NatsConn, msg: NatsMsg) {.raises: [NatsError].} =
  send_raw(conn, encode(msg))

proc recv_msg*(conn: NatsConn): NatsMsg {.raises: [NatsError].} =
  let line = read_line(conn.sock)
  let parsed = decode_line(line)
  if parsed.is_bad: raise newException(NatsError, parsed.err.msg)
  result = parsed.val
  if result.kind == nmkMsg:
    let parts = line.strip()[4..^1].strip().split(' ')
    let payload_len = try: parseInt(parts[^1]) except ValueError: 0
    if payload_len > 0:
      result.msg_payload = read_payload(conn.sock, payload_len)

proc ping*(conn: NatsConn) {.raises: [NatsError].} =
  send_raw(conn, "PING\r\n")
  let resp = recv_msg(conn)
  if resp.kind != nmkPong:
    raise newException(NatsError, "expected PONG")

proc close_nats*(conn: NatsConn) {.raises: [].} =
  if conn != nil and conn.sock != nil:
    try: conn.sock.close() except CatchableError: discard
