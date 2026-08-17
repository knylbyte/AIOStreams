export interface UsenetOwnerShutdown {
  readonly closeOpenings: () => Promise<unknown>;
  readonly closeGrabs: () => Promise<unknown>;
  readonly closeCensusShadows: () => Promise<unknown>;
  readonly closeEngines: () => Promise<unknown>;
  readonly closePersistence: () => Promise<unknown>;
}

function invokeCleanup(cleanup: () => Promise<unknown>): Promise<unknown> {
  try {
    return Promise.resolve(cleanup());
  } catch (error) {
    return Promise.reject(error);
  }
}

async function collectFailures(
  operations: readonly Promise<unknown>[],
  failures: unknown[]
): Promise<void> {
  for (const result of await Promise.allSettled(operations)) {
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
  const openingClose = invokeCleanup(owners.closeOpenings);
  const grabClose = invokeCleanup(owners.closeGrabs);
  const censusClose = invokeCleanup(owners.closeCensusShadows);

  await collectFailures([openingClose, grabClose], failures);
  await collectFailures([invokeCleanup(owners.closeEngines)], failures);
  await collectFailures([censusClose], failures);
  await collectFailures([invokeCleanup(owners.closePersistence)], failures);

  if (failures.length > 0) {
    throw new AggregateError(failures, 'Usenet shutdown failed');
  }
}
