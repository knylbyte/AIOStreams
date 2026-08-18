import assert from 'node:assert/strict';
import test from 'node:test';
import { CommandPriority } from '../types.js';
import {
  ResourceEventLogAggregator,
  type ResourceEventLogSink,
} from './resource-events.js';

interface LoggedRecord {
  readonly level: 'debug' | 'warn';
  readonly fields: Record<string, unknown>;
}

function captureSink(enabled = true): {
  readonly sink: ResourceEventLogSink;
  readonly records: LoggedRecord[];
  readonly counts: { emitted: number; suppressed: number };
} {
  const records: LoggedRecord[] = [];
  const counts = { emitted: 0, suppressed: 0 };
  return {
    records,
    counts,
    sink: {
      debugEnabled: () => enabled,
      debug: (fields) => records.push({ level: 'debug', fields }),
      warn: (fields) => records.push({ level: 'warn', fields }),
      emitted: () => counts.emitted++,
      suppressed: () => counts.suppressed++,
    },
  };
}

test('aggregates 500 successful hotpath events into two fixed summaries', () => {
  const captured = captureSink();
  const aggregator = new ResourceEventLogAggregator(captured.sink, () => 0);
  for (let index = 0; index < 500; index++) {
    aggregator.observe({ type: 'promotion_result', outcome: 'success' });
  }
  aggregator.flush();

  assert.equal(captured.counts.suppressed, 500);
  assert.equal(captured.counts.emitted, 2);
  assert.equal(captured.records.length, 2);
  assert.equal(
    captured.records.reduce(
      (sum, record) => sum + Number(record.fields.resourceEvents),
      0
    ),
    500
  );
  assert.equal(
    captured.records.reduce(
      (sum, record) => sum + Number(record.fields.promotionSuccess),
      0
    ),
    500
  );
});

test('logs warnings, failures, aborts, and long waits immediately', () => {
  const captured = captureSink();
  const aggregator = new ResourceEventLogAggregator(captured.sink, () => 0);
  aggregator.observe({
    type: 'disk_safety_warning',
    requestedBytes: 1,
    freeBytes: 2,
    requiredBytes: 3,
    minFreeDiskBytes: 4,
  });
  aggregator.observe({ type: 'promotion_result', outcome: 'failed' });
  aggregator.observe({
    type: 'memory_wait_end',
    kind: 'download',
    bytes: 10,
    priority: CommandPriority.High,
    queueDepth: 0,
    waitMs: 1,
    outcome: 'aborted',
  });
  aggregator.observe({
    type: 'spool_wait_end',
    kind: 'spool',
    bytes: 10,
    priority: CommandPriority.High,
    queueDepth: 0,
    waitMs: 50,
    outcome: 'granted',
  });

  assert.deepEqual(
    captured.records.map((record) => record.level),
    ['warn', 'warn', 'debug', 'debug']
  );
  assert.equal(captured.counts.emitted, 4);
  assert.equal(captured.counts.suppressed, 0);
});

test('disabled debug level performs no summary serialization', () => {
  const captured = captureSink(false);
  const aggregator = new ResourceEventLogAggregator(captured.sink, () => 0);
  for (let index = 0; index < 500; index++) {
    aggregator.observe({ type: 'promotion_result', outcome: 'skipped' });
  }
  aggregator.flush();
  assert.equal(captured.records.length, 0);
  assert.equal(captured.counts.emitted, 0);
  assert.equal(captured.counts.suppressed, 500);
});

test('a pending summary is discarded when debug logging is disabled before flush', () => {
  let enabled = true;
  const records: LoggedRecord[] = [];
  const counts = { emitted: 0, suppressed: 0 };
  const aggregator = new ResourceEventLogAggregator(
    {
      debugEnabled: () => enabled,
      debug: (fields) => records.push({ level: 'debug', fields }),
      warn: (fields) => records.push({ level: 'warn', fields }),
      emitted: () => counts.emitted++,
      suppressed: () => counts.suppressed++,
    },
    () => 0
  );
  aggregator.observe({ type: 'promotion_result', outcome: 'success' });
  enabled = false;
  aggregator.flush();

  assert.equal(records.length, 0);
  assert.equal(counts.emitted, 0);
  assert.equal(counts.suppressed, 1);
});
