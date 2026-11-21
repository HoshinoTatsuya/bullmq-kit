import { connect, createServer, type Server, type Socket } from 'node:net';

/**
 * TCP-прокси, который умеет ЗАМЕРЗАТЬ: байты перестают идти, сокеты открыты.
 * Это не обрыв (на обрыв клиент Redis отвечает ошибкой и переподключением), а
 * то, что видит процесс при зависшем узле, потерянных пакетах, полуоткрытом
 * соединении: команды уходят в сокет и не возвращаются, и ни
 * `maxRetriesPerRequest: null`, ни умолчания ioredis отказа не дают.
 *
 * `freeze()` ставит чтение обоих концов на паузу: данные копятся в буферах
 * ядра, как при настоящей потере связи, и уходят разом после `thaw()`.
 */
export class FreezingTcpProxy {
  private readonly _server: Server;
  private readonly _sockets = new Set<Socket>();
  private _frozen = false;

  private constructor(
    private readonly _upstreamHost: string,
    private readonly _upstreamPort: number,
  ) {
    this._server = createServer((client) => this._pipe(client));
  }

  /** Прокси на свободном порту 127.0.0.1 к `host:port`. */
  public static async start(host: string, port: number): Promise<FreezingTcpProxy> {
    const proxy = new FreezingTcpProxy(host, port);
    await new Promise<void>((resolve) => proxy._server.listen(0, '127.0.0.1', resolve));
    return proxy;
  }

  public get port(): number {
    const address = this._server.address();
    if (address === null || typeof address === 'string') {
      throw new Error('прокси не слушает TCP-порт');
    }
    return address.port;
  }

  public freeze(): void {
    this._frozen = true;
    for (const socket of this._sockets) {
      socket.pause();
    }
  }

  public thaw(): void {
    this._frozen = false;
    for (const socket of this._sockets) {
      socket.resume();
    }
  }

  public async close(): Promise<void> {
    for (const socket of this._sockets) {
      socket.destroy();
    }
    await new Promise<void>((resolve) => this._server.close(() => resolve()));
  }

  /**
   * Перекладка байтов с обратным давлением — без `pipe()`: тот по `drain` цели
   * сам возобновляет источник, в том числе замороженный, и «замёрзшая» сеть
   * продолжала бы передавать, как только цель разгребёт буфер.
   */
  private _forward(from: Socket, to: Socket): void {
    from.on('data', (chunk) => {
      if (!to.write(chunk)) {
        from.pause();
        to.once('drain', () => {
          if (!this._frozen) {
            from.resume();
          }
        });
      }
    });
    from.on('end', () => to.end());
  }

  private _pipe(client: Socket): void {
    const upstream = connect(this._upstreamPort, this._upstreamHost);
    for (const socket of [client, upstream]) {
      this._sockets.add(socket);
      socket.on('error', () => undefined);
      socket.on('close', () => this._sockets.delete(socket));
      if (this._frozen) {
        socket.pause();
      }
    }
    this._forward(client, upstream);
    this._forward(upstream, client);
    client.on('close', () => upstream.destroy());
    upstream.on('close', () => client.destroy());
  }
}
