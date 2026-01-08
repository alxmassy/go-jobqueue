# Job Queue / Background Worker (Go + PostgreSQL)

A production-style background job processing system built from first principles in Go, using PostgreSQL as the durable source of truth.

This project focuses on **correctness, failure handling, and explicit invariants**, not frameworks or managed infrastructure.

---

## Key Properties

- Durable job persistence (PostgreSQL)
- Atomic job claiming with row-level locking
- Explicit job state machine
- At-least-once execution semantics
- Attempt-aware retries with backoff
- Lease-based ownership and crash recovery
- Automatic reaping of abandoned jobs
- No in-memory queue as a source of truth