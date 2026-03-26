## pool.nim -- NATS connection pool with Lock+Cond.

{.experimental: "strict_funcs".}

import std/locks
import basis/code/choice, conn

type
  NatsPool* = ref object
    lock: Lock
    cond: Cond
    conns: seq[NatsConn]
    host: string
    port: int

proc new_nats_pool*(size: int, host: string, port: int = 4222): NatsPool {.raises: [NatsError].} =
  result = NatsPool(host: host, port: port)
  initLock(result.lock)
  initCond(result.cond)
  for i in 0 ..< size:
    result.conns.add(open_nats(host, port, name = "pool_" & $i))

proc borrow*(pool: NatsPool): NatsConn {.raises: [].} =
  acquire(pool.lock)
  while pool.conns.len == 0:
    wait(pool.cond, pool.lock)
  result = pool.conns.pop()
  release(pool.lock)

proc recycle*(pool: NatsPool, conn: NatsConn) {.raises: [].} =
  acquire(pool.lock)
  pool.conns.add(conn)
  signal(pool.cond)
  release(pool.lock)

proc close_pool*(pool: NatsPool) {.raises: [].} =
  if pool == nil: return
  acquire(pool.lock)
  for c in pool.conns: close_nats(c)
  pool.conns.setLen(0)
  release(pool.lock)
  deinitLock(pool.lock)
  deinitCond(pool.cond)

proc try_new_nats_pool*(size: int, host: string, port: int = 4222): Choice[NatsPool] =
  ## Create a NATS connection pool, returning Choice instead of raising.
  try: good(new_nats_pool(size, host, port))
  except NatsError as e: bad[NatsPool]("nats", e.msg)
