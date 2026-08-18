const DOWNLOAD_OWNER_SLOTS = 16;

export interface SegmentSpoolingDownloadAdmitCount {
  readonly ownerKey: string;
  readonly admits: number;
}

/** Fixed-size diagnostic snapshot used by the CPU benchmark. */
export interface SegmentSpoolingHotpathSnapshot {
  readonly rawReadCallbacks: number;
  readonly rawReadBytes: number;
  readonly yencDecodeCalls: number;
  readonly yencDecodedBytes: number;
  readonly yencOutputBackingAllocations: number;
  readonly yencOutputBackingReuses: number;
  readonly decodedBatchesCommitted: number;
  readonly sinkDrainCycles: number;
  readonly socketPauseCalls: number;
  readonly socketResumeCalls: number;
  readonly spoolWriteOperations: number;
  readonly spoolWriteSyscalls: number;
  readonly spoolShortWrites: number;
  readonly spoolBytesWritten: number;
  readonly headerLinesParsed: number;
  readonly headerTransitionCopies: number;
  readonly resourceEventsObserved: number;
  readonly resourceLogRecordsEmitted: number;
  readonly resourceLogRecordsSuppressed: number;
  readonly promotionsStarted: number;
  readonly promotionsSkippedForForegroundPressure: number;
  readonly promotionBytesCopied: number;
  readonly activeDownloads: number;
  readonly activeDownloadsPeak: number;
  readonly semaphoreOwnerCountPeak: number;
  readonly semaphoreWaiterCountPeak: number;
  readonly semaphoreGrants: number;
  readonly semaphoreAborts: number;
  readonly semaphoreCapacityRejects: number;
  readonly semaphoreOwnerTurns: number;
  readonly semaphoreGlobalScans: number;
  readonly perStreamDownloadAdmits: readonly SegmentSpoolingDownloadAdmitCount[];
  readonly downloadOwnerOverflowAdmits: number;
}

interface DownloadOwnerSlot {
  ownerKey: string;
  admits: number;
}

/**
 * Optional, allocation-free hotpath telemetry. The mutable state has a fixed
 * shape and at most {@link DOWNLOAD_OWNER_SLOTS} bounded owner counters; it
 * never retains payloads, message IDs, or per-chunk history.
 */
export class SegmentSpoolingHotpathCounters {
  rawReadCallbacks = 0;
  rawReadBytes = 0;
  yencDecodeCalls = 0;
  yencDecodedBytes = 0;
  yencOutputBackingAllocations = 0;
  yencOutputBackingReuses = 0;
  decodedBatchesCommitted = 0;
  sinkDrainCycles = 0;
  socketPauseCalls = 0;
  socketResumeCalls = 0;
  spoolWriteOperations = 0;
  spoolWriteSyscalls = 0;
  spoolShortWrites = 0;
  spoolBytesWritten = 0;
  headerLinesParsed = 0;
  headerTransitionCopies = 0;
  resourceEventsObserved = 0;
  resourceLogRecordsEmitted = 0;
  resourceLogRecordsSuppressed = 0;
  promotionsStarted = 0;
  promotionsSkippedForForegroundPressure = 0;
  promotionBytesCopied = 0;
  activeDownloads = 0;
  activeDownloadsPeak = 0;
  semaphoreOwnerCountPeak = 0;
  semaphoreWaiterCountPeak = 0;
  semaphoreGrants = 0;
  semaphoreAborts = 0;
  semaphoreCapacityRejects = 0;
  semaphoreOwnerTurns = 0;
  /** Intrusive owner queues never scan the global waiter population. */
  readonly semaphoreGlobalScans = 0;
  private readonly downloadOwners: Array<DownloadOwnerSlot | undefined> =
    Array.from({ length: DOWNLOAD_OWNER_SLOTS });
  private downloadOwnerOverflowAdmitsValue = 0;

  downloadStarted(ownerKey: string): void {
    this.activeDownloads++;
    this.activeDownloadsPeak = Math.max(
      this.activeDownloadsPeak,
      this.activeDownloads
    );
    const existing = this.downloadOwners.find(
      (entry) => entry?.ownerKey === ownerKey
    );
    if (existing) {
      existing.admits++;
      return;
    }
    const empty = this.downloadOwners.findIndex((entry) => entry === undefined);
    if (empty < 0) {
      this.downloadOwnerOverflowAdmitsValue++;
      return;
    }
    this.downloadOwners[empty] = { ownerKey, admits: 1 };
  }

  downloadEnded(): void {
    this.activeDownloads--;
    if (this.activeDownloads < 0) {
      throw new Error('Segment-spooling active download counter underflow');
    }
  }

  semaphoreQueued(ownerCount: number, waiterCount: number): void {
    this.semaphoreOwnerCountPeak = Math.max(
      this.semaphoreOwnerCountPeak,
      ownerCount
    );
    this.semaphoreWaiterCountPeak = Math.max(
      this.semaphoreWaiterCountPeak,
      waiterCount
    );
  }

  semaphoreGrant(): void {
    this.semaphoreGrants++;
  }

  semaphoreAbort(): void {
    this.semaphoreAborts++;
  }

  semaphoreCapacityReject(): void {
    this.semaphoreCapacityRejects++;
  }

  semaphoreOwnerTurn(): void {
    this.semaphoreOwnerTurns++;
  }

  snapshot(): SegmentSpoolingHotpathSnapshot {
    return {
      rawReadCallbacks: this.rawReadCallbacks,
      rawReadBytes: this.rawReadBytes,
      yencDecodeCalls: this.yencDecodeCalls,
      yencDecodedBytes: this.yencDecodedBytes,
      yencOutputBackingAllocations: this.yencOutputBackingAllocations,
      yencOutputBackingReuses: this.yencOutputBackingReuses,
      decodedBatchesCommitted: this.decodedBatchesCommitted,
      sinkDrainCycles: this.sinkDrainCycles,
      socketPauseCalls: this.socketPauseCalls,
      socketResumeCalls: this.socketResumeCalls,
      spoolWriteOperations: this.spoolWriteOperations,
      spoolWriteSyscalls: this.spoolWriteSyscalls,
      spoolShortWrites: this.spoolShortWrites,
      spoolBytesWritten: this.spoolBytesWritten,
      headerLinesParsed: this.headerLinesParsed,
      headerTransitionCopies: this.headerTransitionCopies,
      resourceEventsObserved: this.resourceEventsObserved,
      resourceLogRecordsEmitted: this.resourceLogRecordsEmitted,
      resourceLogRecordsSuppressed: this.resourceLogRecordsSuppressed,
      promotionsStarted: this.promotionsStarted,
      promotionsSkippedForForegroundPressure:
        this.promotionsSkippedForForegroundPressure,
      promotionBytesCopied: this.promotionBytesCopied,
      activeDownloads: this.activeDownloads,
      activeDownloadsPeak: this.activeDownloadsPeak,
      semaphoreOwnerCountPeak: this.semaphoreOwnerCountPeak,
      semaphoreWaiterCountPeak: this.semaphoreWaiterCountPeak,
      semaphoreGrants: this.semaphoreGrants,
      semaphoreAborts: this.semaphoreAborts,
      semaphoreCapacityRejects: this.semaphoreCapacityRejects,
      semaphoreOwnerTurns: this.semaphoreOwnerTurns,
      semaphoreGlobalScans: this.semaphoreGlobalScans,
      perStreamDownloadAdmits: this.downloadOwners
        .filter((entry): entry is DownloadOwnerSlot => entry !== undefined)
        .map((entry) => ({ ...entry })),
      downloadOwnerOverflowAdmits: this.downloadOwnerOverflowAdmitsValue,
    };
  }
}
