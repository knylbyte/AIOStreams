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
- [x] Client/range abort keeps the active artifact reader's `error` and `close`
      ownership installed until its asynchronous `_destroy()` settles. Expected
      `USENET_SPOOL_ABORTED` errors stay inside the stream lifecycle; a genuine
      replacement close error is observed and propagated exactly once.
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
- [x] The CPU harness runs its TLS NNTP provider in a separate Node child
      process. Parent `process.cpuUsage()` therefore excludes provider-side TLS
      encryption and writes. Full SHA-256 byte verification runs separately
      from the timed window; timed runs use only byte counts and fixed boundary
      samples.

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
path through native incremental yEnc decode, spooling, a growing file reader
and the ordered `FileStream` consumer. The bounded TLS NNTP fixture runs in a
separate Node child process and transfers only control metadata over IPC. The
provider creates and caches yEnc fixtures itself, so provider TLS encryption,
socket writes and command parsing do not enter the parent CPU gate.

The timed parent window performs no SHA-256 work. It counts delivered bytes and
checks a fixed number of boundary samples independent of 64/128/256-KiB reader
chunking. A separate, untimed run through the identical production pipeline
performs the complete SHA-256 byte-identity check; its corruption regression
proves that this gate fails on a valid-yEnc payload mutation.

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

The release comparison uses the same copied harness for base
`3ae95e7a1f5f840067663812f5967bde63a472fe` and the final worktree. Its
identities are:

```text
harness SHA-256:       be7ae4f20ccd5297fd63da85e5f751af47ca7c118058dba46255e23ac8d803b4
provider-child SHA-256:bfa2764362b8a8a4258355aa07802055d8862194f9d78e978eaa1b69f6bc81e4
fixture SHA-256:       ea584ab7d2dc0d33918d424d7e771039d06aa4410e53499b6e58adf6543c3b55
configuration SHA-256: 3997d74b1f87bf35b37b898ae6883d8b6ece760d07cac1b2ffe19b8a5e92efe1
Node/host:              v24.10.0, Apple M1 arm64, 8 logical CPUs
```

Five base and five final 256-MiB runs were interleaved in the order
base/final/final/base/base/final/final/base/base/final, with one-MiB segments,
a 32-MiB warmup, a real 30-second S5 pause and a 16-MiB/s resumed reader. The
medians are:

| Scenario         | CPU ms/GiB base/final | CPU delta | First byte base/final | Throughput MiB/s base/final | Event-loop p95 base/final |
| ---------------- | --------------------: | --------: | --------------------: | --------------------------: | ------------------------: |
| S1 single        |         7,331 / 4,701 |    −35.9% |        9.29 / 8.83 ms |               314.7 / 468.2 |          10.30 / 10.27 ms |
| S2 + promotion   |         9,505 / 5,923 |    −37.7% |        8.68 / 9.52 ms |               256.9 / 421.2 |          10.25 / 10.33 ms |
| S3 two streams   |         6,572 / 4,380 |    −33.4% |        9.31 / 8.96 ms |               340.7 / 512.0 |          10.44 / 11.32 ms |
| S4 shared stream |        10,294 / 4,270 |    −58.5% |        8.45 / 9.41 ms |               430.1 / 565.8 |          10.31 / 10.97 ms |
| S5 slow consumer |       27,182 / 20,862 |    −23.3% |        8.79 / 8.54 ms |                 5.56 / 5.56 |          12.16 / 12.13 ms |

Nearest-rank p95 values across those five runs are retained rather than
discarded:

| Scenario | CPU ms/GiB base/final | CPU delta | First byte base/final | Event-loop p95 base/final |
| -------- | --------------------: | --------: | --------------------: | ------------------------: |
| S1       |         8,936 / 6,279 |    −29.7% |      11.47 / 13.91 ms |          10.46 / 10.60 ms |
| S2       |        11,797 / 7,657 |    −35.1% |      11.38 / 14.75 ms |          10.32 / 10.96 ms |
| S3       |         7,409 / 5,537 |    −25.3% |      12.66 / 19.92 ms |          11.15 / 12.02 ms |
| S4       |        12,736 / 6,154 |    −51.7% |      10.41 / 19.61 ms |          10.82 / 11.46 ms |
| S5       |       27,550 / 21,523 |    −21.9% |      10.61 / 13.84 ms |          12.16 / 12.19 ms |

The S1 and S3 parent-CPU gates pass by wide margins, median throughput improves
in S1–S4 and remains unchanged in the deliberately rate-limited S5, and median
first byte improves in S1/S3/S5. The p95 first-byte tail and the 0.88-ms S3
median event-loop-p95 increase remain explicit diagnostic evidence rather than
being presented as a passed no-regression gate. Every S5 run records one real
pause, one resume and 129 rate-limit waits; all memory, spool, artifact and
open-file owners finish at zero.

For one 512-segment S1 run, output-backing allocations fell from 33,792 to 512;
spool writes, syscalls and drain cycles fell from 33,792 to 2,560; socket
pause/resume calls fell from 13,824/13,824 to 2,560/2,560. Transition copies
are zero. With debug disabled, 2,561 raw resource events produce no debug
records; the deterministic debug-level regression test reduces 500 successful
events to two summaries (99.6%) while errors and long waits remain immediate.

Persistent promotion has no queue. New work is skipped for memory, spool,
open-file or foreground-download pressure, and active playback admits at most
one promotion. Global download admission keeps High/Low priority behavior and
uses intrusive per-owner FIFO deques and circular owner rings. Enqueue, owner
turn, grant and arbitrary waiter abort are O(1); terminal close walks only the
bounded retained queues. There are at most 128 distinct queued owners, 1,024
waiters per owner and 65,536 waiters globally, with typed rejections at each
boundary. The anonymous compatibility owner participates in the same bound.
The structural scheduler counter reports `globalScans = 0`; S3 records equal
admission for both owners.

Reproduce the benchmark and a Node 24 CPU profile with:

```bash
LOG_LEVEL=error pnpm -F core benchmark:segment-spooling:cpu

cd packages/core
LOG_LEVEL=error \
AIOSTREAMS_CPU_BENCHMARK_SCENARIOS=S1 \
AIOSTREAMS_CPU_BENCHMARK_RUNS=1 \
AIOSTREAMS_CPU_BENCHMARK_TOTAL_BYTES=536870912 \
AIOSTREAMS_CPU_BENCHMARK_WARMUP_BYTES=33554432 \
AIOSTREAMS_CPU_BENCHMARK_CORRECTNESS=0 \
node --cpu-prof --cpu-prof-dir="$(mktemp -d)" \
  --import tsx --import ./test/setup.ts \
  src/usenet/benchmarks/segment-spooling-cpu.ts
```

Profiles remain local and are excluded from commits and source exports. Before
the change, the parent-only S1 profile contains 48 sampled native
`decodeChunk` frames and 30 `writeBuffer` frames; the final profile contains 29
`decodeChunk` frames and no `writeBuffer` frame in its top 20. SHA-256 work is
absent, and no provider PID receives a CPU profile. A Worker-thread path was
not adopted: native decode no longer dominates, and moving the same work would
add transfer/ownership complexity without evidence of lower total CPU.

## Production TLS smoke test

Use an existing deployment secret/provider configuration; do not put
credentials in commands, screenshots or logs. Select `segment_spooling`, then
restart the service before each gate so the effective settings are explicit.

1. **P1 — one stream / minimal 1×1:** set provider connections, effective pipeline depth and
   concurrent downloads to `1`. Continuously read several hundred MiB from a
   known-good release. Require sustained byte progress, a healthy provider,
   and no protocol-desync, circuit-breaker or `USENET_SPOOL_IO` event.
2. **P2 — two clients, same content:** start both ranges before the first BODY
   completes. Pause one client and let the other finish. Require one shared
   on-wire flight where applicable, byte-identical fast-client output and no
   global cancellation when the paused client closes.
3. **P3 — two different contents:** stream two releases concurrently. Require
   bounded independent progress, owner round-robin turns and no starvation by
   either release's prefetch tail.
4. **P4 — slow plus fast client:** rate-limit one client, pause it for at least
   30 seconds, keep the other client reading, then resume the slow client.
   Require the fast client to progress independently. Memory and spool values
   in the Usenet resource dashboard must stay at or below their configured
   maxima and the resumed bytes must match an unrestricted download.
5. **P5 — abort/reuse:** abort the paused request and immediately open the same
   release again. The old request must reach zero memory, spool, artifact,
   open-file and waiter ownership; the new request must succeed without a
   local-backpressure circuit penalty. Logs must contain neither a fatal
   `uncaughtException` nor an `USENET_SPOOL_ABORTED` outside the controlled
   stream failure path.

After P1–P5 pass, restore normal connection, pipeline, download and prefetch
values one setting at a time and repeat progress, boundedness and byte-identity
checks; correctness must not depend on the 1×1 settings.

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
as proven. External provider credentials were not available in the build
environment, so P1–P5 above remain explicit deployment gates and are not
reported as synthetically passed. The CPU-series p95 first-byte outliers and
S3 event-loop-p95 increase likewise remain open performance evidence. The
`readAt()`/archive MVP boundary above also remains intentional.
