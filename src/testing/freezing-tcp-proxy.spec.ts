import { connect, createServer, type Server, type Socket } from 'node:net';

import { FreezingTcpProxy } from './freezing-tcp-proxy';

/**
 * Замёрзший прокси не пропускает байтов, даже если цель за это время
 * разгребла свой буфер: `pipe()` по событию `drain` цели сам возобновлял
 * поставленный на паузу источник, и «замёрзшая» сеть продолжала передавать.
 */
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const TOTAL = 32 * 1024 * 1024;

describe('FreezingTcpProxy', () => {
  let upstream: Server;
  let accepted: Socket | undefined;
  let received = 0;

  beforeEach(async () => {
    received = 0;
    upstream = createServer((socket) => {
      accepted = socket;
      // Цель сначала не читает: буфер прокси к ней заполняется, pipe ждёт drain.
      socket.pause();
      socket.on('data', (chunk) => {
        received += chunk.length;
      });
    });
    await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
  });
  afterEach(async () => {
    accepted?.destroy();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  });

  const settled = async () => {
    // Ждать, пока поток байтов не остановится (или 5 с).
    let last = -1;
    for (let i = 0; i < 50 && last !== received; i += 1) {
      last = received;
      await sleep(100);
    }
  };

  it('после freeze цель, разгрёбшая буфер, не получает новых байтов; после thaw — получает всё', async () => {
    const address = upstream.address();
    if (address === null || typeof address === 'string') {
      throw new Error('нет порта');
    }
    const proxy = await FreezingTcpProxy.start('127.0.0.1', address.port);
    const client = connect(proxy.port, '127.0.0.1');
    client.on('error', () => undefined);
    try {
      await new Promise<void>((resolve) => client.once('connect', () => resolve()));
      client.write(Buffer.alloc(TOTAL, 1));
      await sleep(300);
      proxy.freeze();
      accepted?.resume();
      await settled();
      const duringFreeze = received;
      // Пропущено не больше того, что уже лежало в буферах до заморозки.
      expect(duringFreeze).toBeLessThan(TOTAL / 2);

      proxy.thaw();
      for (let i = 0; i < 100 && received < TOTAL; i += 1) {
        await sleep(50);
      }
      expect(received).toBe(TOTAL);
    } finally {
      client.destroy();
      await proxy.close();
    }
  }, 20_000);
});
