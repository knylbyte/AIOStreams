import assert from 'node:assert/strict';
import test from 'node:test';
import { DOT_TERMINATOR, NntpOnreadParser } from './protocol.js';

test('streaming BODY terminator is recognized at every read boundary', () => {
  const body = Buffer.from('first\r\n..dot-stuffed\r\nlast', 'latin1');
  const following = Buffer.from('223 0 <next>\r\n', 'latin1');
  const wire = Buffer.concat([body, DOT_TERMINATOR, following]);

  for (let split = 0; split <= wire.length; split++) {
    const parser = new NntpOnreadParser();
    const chunks: Buffer[] = [];
    parser.beginStreamingBody((chunk) => {
      chunks.push(Buffer.from(chunk));
      return true;
    });

    const first = parser.feedBody(wire, 0, split);
    let final = first;
    if (!first.ended) final = parser.feedBody(wire, split, wire.length);

    assert.equal(final.ended, true, `ended split ${split}`);
    assert.equal(final.backpressured, false, `pressure split ${split}`);
    assert.equal(
      final.off,
      body.length + DOT_TERMINATOR.length,
      `offset split ${split}`
    );
    assert.deepEqual(Buffer.concat(chunks), body, `body split ${split}`);
    assert.equal(parser.streamed, body.length, `count split ${split}`);
    assert.deepEqual(wire.subarray(final.off), following);
  }
});

test('streaming BODY preserves terminator state across single-byte reads', () => {
  const body = Buffer.from('body-with-small-reads', 'latin1');
  const wire = Buffer.concat([body, DOT_TERMINATOR]);
  const parser = new NntpOnreadParser();
  const chunks: Buffer[] = [];
  parser.beginStreamingBody((chunk) => {
    chunks.push(Buffer.from(chunk));
    return true;
  });

  let ended = false;
  for (let offset = 0; offset < wire.length; offset++) {
    const step = parser.feedBody(wire, offset, offset + 1);
    ended = step.ended;
    assert.equal(step.off, offset + 1);
    if (ended) {
      assert.equal(offset, wire.length - 1);
      break;
    }
  }

  assert.equal(ended, true);
  assert.deepEqual(Buffer.concat(chunks), body);
});

test('streaming BODY propagates consumer backpressure with exact consumed offset', () => {
  const body = Buffer.from('accepted-before-pause', 'latin1');
  const following = Buffer.from('111 20260101000000\r\n', 'latin1');
  const wire = Buffer.concat([body, DOT_TERMINATOR, following]);
  const parser = new NntpOnreadParser();
  const chunks: Buffer[] = [];
  parser.beginStreamingBody((chunk) => {
    chunks.push(Buffer.from(chunk));
    return false;
  });

  const step = parser.feedBody(wire, 0, wire.length);

  assert.deepEqual(step, {
    ended: true,
    backpressured: true,
    off: body.length + DOT_TERMINATOR.length,
  });
  assert.deepEqual(Buffer.concat(chunks), body);
  assert.deepEqual(wire.subarray(step.off), following);
});

test('buffered BODY behavior remains byte-identical across terminator boundaries', () => {
  const body = Buffer.from('buffered\r\narticle', 'latin1');
  const wire = Buffer.concat([body, DOT_TERMINATOR]);

  for (let split = 0; split <= wire.length; split++) {
    const parser = new NntpOnreadParser();
    parser.beginBufferedBody(Buffer.allocUnsafe(8));
    const first = parser.feedBody(wire, 0, split);
    const final = first.ended
      ? first
      : parser.feedBody(wire, split, wire.length);

    assert.equal(final.ended, true, `ended split ${split}`);
    assert.equal(final.backpressured, false, `pressure split ${split}`);
    assert.deepEqual(parser.dest?.subarray(0, parser.bodyLen), body);
  }
});
