# Choice/Life Adoption Plan: nats

## Summary

- **Error type**: `NatsError` defined in lattice.nim -- move to `conn.nim`
- **Files to modify**: 8 + re-export module
- **Result sites**: 20
- **Life**: Not applicable

## Steps

1. Delete `src/nats/lattice.nim`
2. Move `NatsError* = object of CatchableError` to `src/nats/conn.nim`
3. Add `requires "basis >= 0.1.0"` to nimble
4. In every file importing lattice:
   - Replace `import.*lattice` with `import basis/code/choice`
   - Replace `Result[T, E].good(v)` with `good(v)`
   - Replace `Result[T, E].bad(e[])` with `bad[T]("nats", e.msg)`
   - Replace `Result[T, E].bad(NatsError(msg: "x"))` with `bad[T]("nats", "x")`
   - Replace return type `Result[T, NatsError]` with `Choice[T]`
5. Update re-export: `export lattice` -> `export choice`
6. Update tests
