# Usenet segment streaming — final acceptance record

This record applies to the implementation based on
`dev/usenet-streaming/concept/usenet-segment-streaming-modes.md`. It documents
the automated evidence and the intentionally retained MVP boundary; the
canonical concept itself is unchanged.

## Compatibility

- [x] `segment_buffering` remains the default.
- [x] Existing performance-profile fields and values remain unchanged.
- [x] The buffering stream has a byte-identity regression test against the same
      fragmented fake-NNTP articles used by the spooling E2E path.
- [x] Provider order, 430 failover, range semantics and hole behavior retain
      dedicated unit/integration coverage.
- [x] Existing persistent segment-cache entries remain readable.

## Segment Spooling

- [x] BODY decode is incremental; the raw article and decoded segment are not
      materialized as a required full-buffer intermediate.
- [x] Memory and disk ownership use hard byte leases with peak/waiter stats.
- [x] The first growing artifact is readable before producer completion, with
      EOF withheld until decoder/sink/length validation.
- [x] Completed read-ahead waits in transient spool files and emits in order.
- [x] Free-disk reserve, open-file cap, client abort and engine/process cleanup
      are tested.
- [x] Persistent cache hits and promotion are file-backed.
- [x] No disk-error fallback to the buffering fetch path exists.

## System evidence

- [x] Engine live stats and dashboard data expose mode, memory, spool, file and
      arena accounting, including actual-owner read/write rates and cleanup
      errors.
- [x] The dashboard presents resource owners without per-chunk detail.
- [x] Shutdown first fences HTTP, stream-session and engine admission; provider
      change and process shutdown then await the prior engine's spool and final
      stable-cache index writer before replacement/exit.
- [x] A `StreamHandle` retained by a pre-seal request is terminal: late
      `attach()` destroys the resource, late `onKill()` runs once, and late
      byte/metadata updates cannot republish the finalised session.
- [x] `UsenetEngine.close()` alone waits for actual reader `close`, pool owners
      and cache finalisation; previously issued seekable wrappers reject new or
      crossing work once the synchronous engine fence is published.
- [x] Coordinated process shutdown seals `StreamRegistry` with the distinct
      `shutdown` reason before engine retirement. Readers already terminalised
      by that seal are awaited through their real `close` event without turning
      expected lifecycle errors into engine-cleanup failures. A bounded
      engine-owned terminal-error handoff is installed when each reader is
      registered. Only a reader destroy synchronously marked with an explicit
      lifecycle cause can publish a later `_destroy()` replacement such as
      `EIO` into that handoff; ordinary provider/playback errors disappear with
      their closed reader and cannot poison a later engine retirement.
- [x] The synchronous HTTP admission fence also publishes one stable process
      abort signal. Early cached-NZB GET/HEAD, stream HEAD, POST/PUT/PATCH and
      every Undici redirect hop observe it. Pre-header shutdown returns the
      stable 503 contract; a post-header response is destroyed without a second
      response attempt. Limit, stale, administrative stop and client abort
      retain their distinct disconnect semantics.
- [x] Native shared session opens use bounded per-key flights and bounded
      request waiters. The process-wide NZB grab owner independently limits
      itself to 256 producers and 64 waiters per URL; request abort removes only
      that waiter, while process close fences hits, aborts each owner once,
      awaits producer/cache finalizers and prevents late cache or warm-session
      publication before engines and the database retire.
- [x] Final persistence is explicit: every registered disk cache is attempted
      and flush failures are aggregated. Layout/hole/status repository writes
      use one bounded, keyed-serial closeable owner. Each key has at most one
      active write and one latest-wins successor, so layout invalidation follows
      an active patch and hole generations cannot finish out of order. Shutdown
      stops eviction/debounce timers, flushes the latest pending values, awaits
      crossing writes and propagates failures before database close. Status
      latest-wins keys are separated by semantic class, and the database guard
      makes `failed` terminal over concurrent `degraded` updates. Engine readers
      retire before repository persistence freezes its immutable close snapshot,
      so their final hooks remain part of the durability barrier.
- [x] Census continuations use a process-wide owner capped at 64 active tasks
      with one current monotonic generation per NZB hash. Reimports and engine
      retirement invalidate stale generations, while an identity-safe per-hash
      retirement tail remains until both the predecessor's `CensusRun.done` and
      repository task settle. `CensusShadowHandle.done` therefore represents
      the complete owned census/repository lifecycle, and the 64-task capacity
      includes every accepted census until that full settlement. Concurrent
      invalidators share the same tail. Every hole, streamability and status
      mutation rechecks publication ownership before and after its awaited
      operation; release feedback additionally fences each individual key
      mutation. Process shutdown synchronously observes every cleanup
      rejection, fences new shadows, cancels census workers, and awaits the
      actual engine-owned `CensusRun` finalizers in parallel with readers, pool
      and shadow tasks. Crossing repository writes and bounded failures settle
      before stream-hook persistence and the database close barrier.
- [x] Startup orphan cleanup is heartbeat/fence protected and emits a structured
      completion record.
- [x] Stable spool failures map to actionable user and HTTP/debrid errors.
- [x] Local fake-NNTP E2E coverage includes real TLS, 1×1 admission,
      fragmentation, local backpressure, pipelining, out-of-order completion,
      ranges, parallel clients, 430 failover, injected slow disk, ENOSPC/EACCES,
      client-only abort cleanup and engine-close cleanup. The parallel-client
      case starts both readers before the shared BODY completes and aborts one
      without affecting the other.
- [x] `benchmark:segment-spooling` reports `arrayBuffers`, `external`, first
      byte, throughput, event-loop lag and spool/internal-budget peaks. Its
      correctness gates use internal byte budgets, never RSS. The benchmark
      drives the actual ordered `SpoolingSegmentsStream`, deterministic
      out-of-order completion, bounded concurrent producers and a yielding slow
      consumer.

The benchmark defaults to the concept's full 500 one-MiB segment run with a
64-segment prefetch window and 60 concurrent producer slots:

```bash
pnpm -F core benchmark:segment-spooling
```

Short diagnostic runs remain selectable through `USENET_BENCHMARK_BYTES`,
`USENET_BENCHMARK_SEGMENT_BYTES`, `USENET_BENCHMARK_PREFETCH_SEGMENTS` and
`USENET_BENCHMARK_MAX_CONCURRENT_DOWNLOADS`.

Latency, throughput, V8 memory and event-loop values are diagnostic because
machine and filesystem variance makes fixed CI thresholds flaky. Internal
memory/spool ceilings and final-zero ownership remain hard assertions.

## Production TLS smoke test

Use an existing deployment secret/provider configuration; do not put
credentials in commands, screenshots or logs. Select `segment_spooling`, then
restart the service before each gate so the effective settings are explicit.

1. **Minimal 1×1:** set provider connections, effective pipeline depth and
   concurrent downloads to `1`. Continuously read several hundred MiB from a
   known-good release. Require sustained byte progress, a healthy provider,
   and no protocol-desync, circuit-breaker or `USENET_SPOOL_IO` event.
2. **Slow client:** rate-limit the same client, pause it for at least 30 seconds,
   then resume. Memory and spool values in the Usenet resource dashboard must
   stay at or below their configured maxima and the resumed bytes must match an
   unrestricted download.
3. **Abort/reuse:** abort a paused request and immediately open the same release
   again. The old request must reach zero memory, spool, artifact, open-file and
   waiter ownership; the new request must succeed without a local-backpressure
   circuit penalty.
4. **Target concurrency:** restore the normal connection, pipeline, download
   and prefetch values one setting at a time. Repeat the progress, boundedness
   and byte-identity checks; correctness must not depend on the 1×1 settings.

For diagnosis, capture only the structured Usenet resource and failure fields.
Redact URLs, tokens, message IDs, provider credentials and spool/cache paths.

## Intentional boundary / deviation

`FileStream.readAt()` and archive-window streams remain on the compatible
arena/buffering path in this MVP. Their L1 memory is bounded by
`segmentMemoryCacheBytes`; direct file playback and HTTP ranges use the complete
spooling path. A future file-backed random-access/archive extension can remove
this boundary without changing the current transport mode contract.

The E2E suite uses a deterministic in-process NNTP server and injected
filesystem failures. Windows rename/delete behavior is covered through the
generic cache's deterministic Windows-style tests; a native Windows runner is
still missing and remains a platform-evidence risk rather than being reported
as proven. The `readAt()`/archive MVP boundary above also remains intentional.
