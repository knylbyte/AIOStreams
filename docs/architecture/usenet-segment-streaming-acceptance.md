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
      expected lifecycle errors into engine-cleanup failures.
- [x] A proxy request waiting for upstream headers receives the stable 503
      shutdown response only for the explicit shutdown reason; its active
      Undici request (including the current redirect hop) is actually aborted.
      Limit, stale and administrative stops retain their disconnect semantics.
- [x] Native shared session opens use bounded per-key flights and bounded
      request waiters. Process shutdown synchronously fences new opens, aborts
      remote NZB work, awaits every flight finalizer, and prevents late warm
      session publication before engines and the database retire.
- [x] Startup orphan cleanup is heartbeat/fence protected and emits a structured
      completion record.
- [x] Stable spool failures map to actionable user and HTTP/debrid errors.
- [x] Local fake-NNTP E2E coverage includes fragmentation, local backpressure,
      pipelining, out-of-order completion, ranges, parallel clients, 430
      failover, injected slow disk, ENOSPC/EACCES, client-only abort cleanup and
      engine-close cleanup. The parallel-client case starts both readers before
      the shared BODY completes and aborts one without affecting the other.
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
