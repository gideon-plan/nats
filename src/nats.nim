## nats.nim -- Pure Nim NATS/JetStream client. Re-export module.
{.experimental: "strict_funcs".}
import nats/[proto, conn, pub, sub, jetstream, consumer, pool]
export proto, conn, pub, sub, jetstream, consumer, pool
