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
- [x] TLS late-read carry uses exact unpooled owners. Its byte cap and dashboard
      value count physically retained backing allocations; partially consumed
      owners retain their full accounting until removal.
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
      consumer. Production and benchmark obtain the 256 KiB decoder chunk and
      1,572,864-byte decoder/sink/TLS-carry base lease from the same resource-
      plan helper. Each Writer chunk is an exact logical child inside that
      global base lease and is not acquired from the global budget twice.

The benchmark defaults to the concept's full 500 one-MiB segment run with a
64-segment prefetch window, a 16 MiB memory budget and 60 configured producer
slots. It reports the lower effective producer limit derived from the hard
stream and productive per-download windows rather than claiming all 60 slots
when that memory cannot be leased. With the current 786,430-byte stream window,
the 16 MiB default admits ten downloads. The regression suite uses 96 MiB to
prove the complete 60-download ceiling; 80 MiB is explicitly insufficient:

```bash
pnpm -F core benchmark:segment-spooling
```

Short diagnostic runs remain selectable through `USENET_BENCHMARK_BYTES`,
`USENET_BENCHMARK_SEGMENT_BYTES`, `USENET_BENCHMARK_CHUNK_BYTES`,
`USENET_BENCHMARK_PREFETCH_SEGMENTS` and
`USENET_BENCHMARK_MAX_CONCURRENT_DOWNLOADS`. The chunk setting controls only
the benchmark Writer/output chunks; it never changes the productive decoder or
base-lease window. A larger explicit
`USENET_BENCHMARK_MEMORY_BYTES` can exercise the complete 60-producer ceiling.

Latency, throughput, V8 memory and event-loop values are diagnostic because
machine and filesystem variance makes fixed CI thresholds flaky. Internal
memory/spool ceilings and final-zero ownership remain hard assertions.

## CPU hotpath acceptance

The dedicated `benchmark:segment-spooling:cpu` command drives the production
path from a local TLS NNTP server through native incremental yEnc decode,
spooling, a growing file reader and the ordered `FileStream` consumer. Its
defaults are 512 MiB per scenario, one-MiB segments, a 32-MiB warmup and five
measured runs for each of S1–S5. The comparison below used base
`3ae95e7a1f5f840067663812f5967bde63a472fe`, followed by two independent
five-run final series on the same machine. The final medians therefore cover
ten measured runs; p95 values remain diagnostic.

The production spooling decoder now writes directly into one unpooled 512-KiB
batch (`2 × 256 KiB`). Its exact child lease remains writer-owned until the
complete short-write-safe file operation settles. The first payload flushes
immediately; steady state combines up to two decoder inputs. Header transition
pieces are decoded sequentially into the same window. Because the physical
batch is contiguous, one ordinary positional write is cheaper and simpler than
`writev`; no bounded iovec list is needed.

On-wire memory admission remains one atomic 1,572,864-byte lease:

```text
512 KiB direct decode/write batch
+ 1 MiB guaranteed TLS/onread carry
= 1,572,864 bytes
```

Artifact readers use at most 128-KiB chunks. Each Node queue reserves
`HWM + 128 KiB - 1`, and a direct stream/FileStream relay atomically reserves
two/three such capacities. The 128-KiB choice keeps the minimum 2-MiB stream
window valid (`3 × (512 KiB + 128 KiB - 1) = 1,966,077` bytes). The measured
reader alternatives were:

| Reader chunk | S1 CPU ms/GiB | S1 first byte | Internal peak | Decision                                 |
| ------------ | ------------: | ------------: | ------------: | ---------------------------------------- |
| 64 KiB       |         7,471 |      18.07 ms |  19,070,973 B | CPU gate missed                          |
| 128 KiB      |         6,268 |      17.50 ms |  19,267,581 B | selected                                 |
| 256 KiB      |         6,030 |      19.27 ms |  19,660,797 B | exceeds the 2-MiB minimum queue contract |

The NNTP/decoder window stays at 256 KiB. A production-equivalent 512-KiB
experiment did not reduce TLS read/decode callbacks, raised the S1 internal
peak to 31,850,493 bytes and moved first byte to 19.61 ms; S3 first byte rose to
21.21 ms. It was therefore rejected rather than trading concurrency and memory
for fewer writes.

Median results (bytes/second for throughput) were:

| Scenario         | CPU ms/GiB before | CPU ms/GiB after | CPU delta | First byte before/after |   Throughput before/after | Event-loop p95 before/after |
| ---------------- | ----------------: | ---------------: | --------: | ----------------------: | ------------------------: | --------------------------: |
| S1 single        |             8,497 |            6,268 |    −26.2% |        17.74 / 17.50 ms | 264,365,521 / 355,721,894 |            10.36 / 10.46 ms |
| S2 + promotion   |            10,482 |            7,051 |    −32.7% |        17.72 / 16.96 ms | 227,272,949 / 328,847,881 |            10.54 / 10.54 ms |
| S3 two streams   |             7,912 |            5,763 |    −27.2% |        16.88 / 18.38 ms | 274,159,093 / 385,288,899 |            10.78 / 10.76 ms |
| S4 shared stream |             7,798 |            5,567 |    −28.6% |        17.38 / 17.24 ms | 303,365,568 / 397,095,571 |            10.62 / 10.63 ms |
| S5 slow consumer |             8,060 |            6,489 |    −19.5% |        16.89 / 17.91 ms | 272,424,039 / 357,378,578 |            10.35 / 10.53 ms |

The largest median p95-lag change is 0.172 ms against a 10-ms sampling
resolution; mean lag is unchanged to 0.001 ms in S5. This is treated as
measurement granularity rather than a material event-loop regression. First
byte stays within 10% in every scenario (the largest change is +8.9% for S3),
and throughput improves throughout.

For one 512-segment S1 run, output-backing allocations fell from 33,792 to 512;
spool writes, syscalls and drain cycles fell from 33,792 to 2,560; socket
pause/resume calls fell from 13,824/13,824 to 2,560/2,560. Transition copies
are zero. With debug disabled, 2,561 raw resource events produce no debug
records; the deterministic debug-level regression test reduces 500 successful
events to two summaries (99.6%) while errors and long waits remain immediate.

Persistent promotion has no queue. New work is skipped for memory, spool,
open-file or foreground-download pressure, and active playback admits at most
one promotion. Global download admission keeps High/Low priority behavior and
uses bounded owner round-robin within a priority, FIFO within each owner. S3
recorded exactly 256 admits for each independent owner. All measured runs ended
with zero download, memory, spool-artifact and open-file owners.

Reproduce the benchmark and a Node 24 CPU profile with:

```bash
LOG_LEVEL=error pnpm -F core benchmark:segment-spooling:cpu

cd packages/core
LOG_LEVEL=error \
AIOSTREAMS_CPU_BENCHMARK_SCENARIOS=S1 \
AIOSTREAMS_CPU_BENCHMARK_RUNS=1 \
AIOSTREAMS_CPU_BENCHMARK_TOTAL_BYTES=536870912 \
AIOSTREAMS_CPU_BENCHMARK_WARMUP_BYTES=33554432 \
node --cpu-prof --cpu-prof-dir="$(mktemp -d)" \
  --import tsx --import ./test/setup.ts \
  src/usenet/benchmarks/segment-spooling-cpu.ts
```

Profiles remain local and are excluded from commits and source exports. Before
the change, native decode and file writes were leading application hot spots;
afterward, benchmark checksum hashing and file writes lead, while sampled
native `decodeChunk` work is substantially lower. A Worker-thread path was not
adopted: native decode no longer dominates, and moving the same work would add
transfer/ownership complexity without evidence of lower total CPU.

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
