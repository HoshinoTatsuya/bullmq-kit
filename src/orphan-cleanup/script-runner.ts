import { createHash } from 'node:crypto';

import type { CleanupRedis } from './types/cleanup-redis.type';

/**
 * Исполнение скриптов уборки по SHA (`EVALSHA`), с повтором через `EVAL` на
 * `NOSCRIPT`. Скрипт проверки — ~2.5 КБ текста, и `EVAL` слал бы его на
 * КАЖДОГО кандидата. `NOSCRIPT` бывает после рестарта Redis, `SCRIPT FLUSH`,
 * переключения на реплику: повтор через `EVAL` заодно кладёт скрипт в кэш.
 * Клиент без `evalsha` — прежний путь `EVAL`.
 */
export class ScriptRunner {
  private readonly _shas = new Map<string, string>();

  public constructor(private readonly _redis: CleanupRedis) {}

  public async eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown> {
    if (typeof this._redis.evalsha !== 'function') {
      return await this._redis.eval(script, numKeys, ...args);
    }
    try {
      return await this._redis.evalsha(this._sha(script), numKeys, ...args);
    } catch (error) {
      if (!ScriptRunner._isNoScript(error)) {
        throw error;
      }
      return await this._redis.eval(script, numKeys, ...args);
    }
  }

  /**
   * Один пайплайн вызовов одного скрипта; ответы — по порядку вызовов, как у
   * `exec()`. Вызовы, получившие `NOSCRIPT`, повторяются вторым пайплайном
   * через `EVAL`. `exec()` вернул `null` — ответ на каждый вызов — ошибка.
   */
  public async pipeline(
    script: string,
    calls: ReadonlyArray<readonly [number, ...(string | number)[]]>,
  ): Promise<Array<[Error | null, unknown]>> {
    const first = this._redis.pipeline();
    const bySha = typeof first.evalsha === 'function';
    for (const [numKeys, ...args] of calls) {
      if (bySha) {
        first.evalsha?.(this._sha(script), numKeys, ...args);
      } else {
        first.eval(script, numKeys, ...args);
      }
    }
    const replies = ScriptRunner._complete(await first.exec(), calls.length);
    const missing = replies.flatMap((reply, index) => (ScriptRunner._isNoScript(reply[0]) ? [index] : []));
    if (missing.length === 0) {
      return replies;
    }
    const retry = this._redis.pipeline();
    for (const index of missing) {
      const [numKeys, ...args] = calls[index] as readonly [number, ...(string | number)[]];
      retry.eval(script, numKeys, ...args);
    }
    const retried = ScriptRunner._complete(await retry.exec(), missing.length);
    for (const [position, index] of missing.entries()) {
      replies[index] = retried[position] as [Error | null, unknown];
    }
    return replies;
  }

  private _sha(script: string): string {
    let sha = this._shas.get(script);
    if (sha === undefined) {
      sha = createHash('sha1').update(script).digest('hex');
      this._shas.set(script, sha);
    }
    return sha;
  }

  private static _complete(
    replies: Array<[Error | null, unknown]> | null,
    expected: number,
  ): Array<[Error | null, unknown]> {
    return Array.from(
      { length: expected },
      (_, index) => replies?.[index] ?? [new Error('пайплайн не вернул ответ на вызов'), null],
    );
  }

  private static _isNoScript(error: unknown): boolean {
    return error instanceof Error && error.message.startsWith('NOSCRIPT');
  }
}
