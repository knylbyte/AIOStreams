import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import tls from 'node:tls';
import yencode from 'yencode';

const MAX_COMMAND_BYTES = 16 * 1024;
const MAX_REGISTERED_STREAMS = 128;

export interface BenchmarkProviderStreamSpec {
  readonly key: string;
  readonly totalBytes: number;
  readonly segmentBytes: number;
  readonly fill: number;
  /** Test-only valid yEnc payload corruption caught by parent correctness. */
  readonly corruptFirstSegment?: boolean;
}

export type BenchmarkProviderRequest =
  | { readonly type: 'register'; readonly spec: BenchmarkProviderStreamSpec }
  | { readonly type: 'burn'; readonly iterations: number }
  | { readonly type: 'close' };

export type BenchmarkProviderResponse =
  | { readonly type: 'ready'; readonly port: number }
  | { readonly type: 'registered' }
  | {
      readonly type: 'burned';
      readonly cpuUserMicros: number;
      readonly cpuSystemMicros: number;
      readonly checksum: number;
    }
  | {
      readonly type: 'closed';
      readonly cpuUserMicros: number;
      readonly cpuSystemMicros: number;
    }
  | { readonly type: 'error'; readonly message: string };

interface EncodedBody {
  readonly decodedBytes: number;
  readonly tail: Buffer;
}

class ChildTlsNntpProvider {
  private readonly server: tls.Server;
  private readonly clients = new Set<tls.TLSSocket>();
  private readonly streams = new Map<string, BenchmarkProviderStreamSpec>();
  private readonly encoded = new Map<string, EncodedBody>();

  private constructor(credentials: {
    readonly key: Buffer;
    readonly cert: Buffer;
  }) {
    this.server = tls.createServer(credentials, (socket) => {
      this.clients.add(socket);
      socket.setNoDelay(true);
      socket.on('error', () => undefined);
      socket.on('close', () => this.clients.delete(socket));
      socket.write('200 benchmark nntp ready\r\n', 'latin1');
      let pending = '';
      let response = Promise.resolve();
      socket.on('data', (chunk: Buffer) => {
        pending += chunk.toString('latin1');
        if (pending.length > MAX_COMMAND_BYTES) {
          socket.destroy(new Error('benchmark command capacity reached'));
          return;
        }
        for (;;) {
          const end = pending.indexOf('\r\n');
          if (end < 0) return;
          const command = pending.slice(0, end);
          pending = pending.slice(end + 2);
          response = response
            .then(() => this.respond(socket, command))
            .catch(() => {
              socket.destroy();
            });
        }
      });
    });
  }

  static async create(): Promise<ChildTlsNntpProvider> {
    const [key, cert] = await Promise.all([
      readFile(
        new URL('../../../test/fixtures/nntp-test-key.pem', import.meta.url)
      ),
      readFile(
        new URL('../../../test/fixtures/nntp-test-cert.pem', import.meta.url)
      ),
    ]);
    const provider = new ChildTlsNntpProvider({ key, cert });
    await new Promise<void>((resolve, reject) => {
      provider.server.once('error', reject);
      provider.server.listen(0, '127.0.0.1', () => {
        provider.server.removeListener('error', reject);
        resolve();
      });
    });
    return provider;
  }

  get port(): number {
    const address = this.server.address();
    assert(address && typeof address !== 'string');
    return address.port;
  }

  register(spec: BenchmarkProviderStreamSpec): void {
    if (
      !this.streams.has(spec.key) &&
      this.streams.size >= MAX_REGISTERED_STREAMS
    ) {
      throw new Error('provider stream capacity reached');
    }
    this.streams.set(spec.key, spec);
    const full = Math.min(spec.segmentBytes, spec.totalBytes);
    this.ensureEncoded(spec.fill, full);
    if (spec.corruptFirstSegment) this.ensureEncoded(spec.fill ^ 0xff, full);
    const final = spec.totalBytes % spec.segmentBytes;
    if (final > 0) this.ensureEncoded(spec.fill, final);
  }

  async close(): Promise<void> {
    for (const client of this.clients) client.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }

  private ensureEncoded(fill: number, bytes: number): void {
    const key = `${fill}:${bytes}`;
    if (this.encoded.has(key)) return;
    const post = yencode.post('benchmark.bin', Buffer.alloc(bytes, fill), 128);
    const firstLineEnd = post.indexOf('\r\n');
    assert(firstLineEnd >= 0);
    this.encoded.set(key, {
      decodedBytes: bytes,
      tail: post.subarray(firstLineEnd + 2),
    });
  }

  private async respond(socket: tls.TLSSocket, command: string): Promise<void> {
    if (command === 'DATE') {
      await this.write(socket, Buffer.from('111 20260818120000\r\n'));
      return;
    }
    if (!command.startsWith('BODY <') || !command.endsWith('>')) {
      await this.write(socket, Buffer.from('500 unsupported\r\n'));
      return;
    }
    const messageId = command.slice(6, -1);
    const separator = messageId.lastIndexOf('-');
    const stream = this.streams.get(messageId.slice(0, separator));
    const index = Number.parseInt(messageId.slice(separator + 1), 10);
    if (!stream || !Number.isSafeInteger(index) || index < 0) {
      await this.write(socket, Buffer.from('430 no such article\r\n'));
      return;
    }
    const begin = index * stream.segmentBytes;
    const decodedBytes = Math.min(
      stream.segmentBytes,
      stream.totalBytes - begin
    );
    const fill =
      stream.corruptFirstSegment && index === 0
        ? stream.fill ^ 0xff
        : stream.fill;
    const encoded = this.encoded.get(`${fill}:${decodedBytes}`);
    assert(encoded);
    const parts = Math.ceil(stream.totalBytes / stream.segmentBytes);
    const header = Buffer.from(
      [
        '222 article follows',
        `=ybegin part=${index + 1} total=${parts} line=128 size=${stream.totalBytes} name=benchmark.bin`,
        `=ypart begin=${begin + 1} end=${begin + encoded.decodedBytes}`,
        '',
      ].join('\r\n'),
      'latin1'
    );
    await this.write(socket, header);
    await this.write(socket, encoded.tail);
    await this.write(socket, Buffer.from('\r\n.\r\n', 'latin1'));
  }

  private async write(socket: tls.TLSSocket, chunk: Buffer): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error): void => {
        socket.removeListener('error', onError);
        reject(error);
      };
      socket.once('error', onError);
      socket.write(chunk, () => {
        socket.removeListener('error', onError);
        resolve();
      });
    });
  }
}

function burnCpu(iterations: number): {
  readonly cpuUserMicros: number;
  readonly cpuSystemMicros: number;
  readonly checksum: number;
} {
  if (!Number.isSafeInteger(iterations) || iterations <= 0) {
    throw new Error('invalid provider burn iteration count');
  }
  const started = process.cpuUsage();
  let checksum = 0x12345678;
  for (let index = 0; index < iterations; index++) {
    checksum = Math.imul(checksum ^ index, 1_664_525) + 1_013_904_223;
  }
  const cpu = process.cpuUsage(started);
  return {
    cpuUserMicros: cpu.user,
    cpuSystemMicros: cpu.system,
    checksum: checksum >>> 0,
  };
}

async function childMain(): Promise<void> {
  const send = (message: BenchmarkProviderResponse): void => {
    process.send?.(message);
  };
  const provider = await ChildTlsNntpProvider.create();
  const providerCpuStarted = process.cpuUsage();
  let busy = false;
  let closed = false;
  send({ type: 'ready', port: provider.port });

  process.on('message', (message: BenchmarkProviderRequest) => {
    if (busy || closed) {
      send({
        type: 'error',
        message: 'provider control operation unavailable',
      });
      return;
    }
    busy = true;
    void (async () => {
      if (message.type === 'register') {
        provider.register(message.spec);
        send({ type: 'registered' });
        return;
      }
      if (message.type === 'burn') {
        send({ type: 'burned', ...burnCpu(message.iterations) });
        return;
      }
      closed = true;
      await provider.close();
      const cpu = process.cpuUsage(providerCpuStarted);
      send({
        type: 'closed',
        cpuUserMicros: cpu.user,
        cpuSystemMicros: cpu.system,
      });
      process.disconnect();
    })()
      .catch((error: unknown) => {
        send({
          type: 'error',
          message: error instanceof Error ? error.message : 'provider failed',
        });
      })
      .finally(() => {
        busy = false;
      });
  });

  process.once('disconnect', () => {
    if (!closed) void provider.close().finally(() => process.exit(0));
  });
}

if (process.send) {
  await childMain();
}
