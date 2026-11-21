import type { AcquireResult } from '../group-concurrency/acquire-result.type';

/** Исход захвата пула: как у `acquire`, плюс доля группы и сколько она держит после ответа. */
export interface FairAcquireResult extends AcquireResult {
  readonly share: number;
  readonly held: number;
}
