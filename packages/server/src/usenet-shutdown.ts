export interface UsenetOwnerShutdown {
  readonly closeOpenings: () => Promise<unknown>;
  readonly closeGrabs: () => Promise<unknown>;
  readonly closeCensusShadows: () => Promise<unknown>;
  readonly closeEngines: () => Promise<unknown>;
  readonly closePersistence: () => Promise<unknown>;
}

type CleanupSettlement =
  | { readonly status: 'fulfilled' }
  | { readonly status: 'rejected'; readonly reason: unknown };

function fulfilledCleanup(): CleanupSettlement {
  return { status: 'fulfilled' };
}

function rejectedCleanup(reason: unknown): CleanupSettlement {
  return { status: 'rejected', reason };
}

/** Attach both Promise observers in the same synchronous cleanup turn. */
function settleCleanup(
  cleanup: () => Promise<unknown>
): Promise<CleanupSettlement> {
  try {
    return Promise.resolve(cleanup()).then(fulfilledCleanup, rejectedCleanup);
  } catch (error) {
    return Promise.resolve(rejectedCleanup(error));
  }
}

async function collectFailures(
  operations: readonly Promise<CleanupSettlement>[],
  failures: unknown[]
): Promise<void> {
  for (const result of await Promise.all(operations)) {
    if (result.status === 'rejected') failures.push(result.reason);
  }
}

/**
 * Ordered usenet-owner shutdown. Opening, grab and census-shadow admission
 * fences publish synchronously before the first await. Engine retirement then
 * stops every already-admitted producer; census continuations settle before
 * persistence freezes its final close snapshot. Every phase still runs when
 * an earlier phase fails.
 */
export async function closeUsenetOwners(
  owners: UsenetOwnerShutdown
): Promise<void> {
  const failures: unknown[] = [];
  const openingClose = settleCleanup(owners.closeOpenings);
  const grabClose = settleCleanup(owners.closeGrabs);
  const censusClose = settleCleanup(owners.closeCensusShadows);

  await collectFailures([openingClose, grabClose], failures);
  await collectFailures([settleCleanup(owners.closeEngines)], failures);
  await collectFailures([censusClose], failures);
  await collectFailures([settleCleanup(owners.closePersistence)], failures);

  if (failures.length > 0) {
    throw new AggregateError(failures, 'Usenet shutdown failed');
  }
}
