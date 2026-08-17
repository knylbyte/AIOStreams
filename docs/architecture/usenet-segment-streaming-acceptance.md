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
      arena accounting.
- [x] The dashboard presents resource owners without per-chunk detail.
- [x] Provider change and process shutdown await the prior engine's spool and
      final stable-cache index writer before replacement/exit.
- [x] Startup orphan cleanup is heartbeat/fence protected and emits a structured
      completion record.
- [x] Stable spool failures map to actionable user and HTTP/debrid errors.
- [x] Local fake-NNTP E2E coverage includes fragmentation, local backpressure,
      pipelining, out-of-order completion, ranges, parallel clients, 430
      failover, injected slow disk, ENOSPC/EACCES and client abort.
- [x] `benchmark:segment-spooling` reports `arrayBuffers`, `external`, first
      byte, throughput, event-loop lag and spool/internal-budget peaks. Its
      correctness gates use internal byte budgets, never RSS.

The benchmark defaults to a short 64 MiB developer run. The concept's example
of 500 one-MiB segments is selectable without editing source:

```bash
USENET_BENCHMARK_BYTES=524288000 \
USENET_BENCHMARK_SEGMENT_BYTES=1048576 \
pnpm -F core benchmark:segment-spooling
```

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
still valuable platform evidence but is not required for the internal budget
contract.
