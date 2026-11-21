import { randomUUID } from 'node:crypto';
import { errorText } from '../error-text';
import { clientKeyPrefix } from '../group-concurrency/client-key-prefix';
import { BULLMQ_KEYS } from './constants/bullmq-keys.constant';
import { BULLMQ_SUPPORTED_VERSIONS } from './constants/bullmq-supported-versions.constant';
import { CLEANUP_DEFAULTS } from './constants/cleanup-defaults.constant';
import {
  CLEANUP_SCRIPTS,
  DRY_RUN_ARG,
  LIST_SCAN_ARG,
  UNTIMESTAMPED_ARG,
} from './constants/cleanup-scripts.constant';
import { KEPT_BY_VERDICT } from './constants/kept-by-verdict.constant';
import { NESTED_MARKER } from './constants/nested-marker.constant';
import { CLEANUP_STOP_REASON, ORPHAN_VERDICT } from './constants/orphan-verdict.constant';
import { ScriptRunner } from './script-runner';
import type { OrphanedJobsCleanupOptions } from './types/cleanup-options.type';
import type { CleanupRedis, CleanupScanner } from './types/cleanup-redis.type';
import type { CleanupKept, CleanupReport, UnsupportedQueue } from './types/cleanup-report.type';
import type { CursorState } from './types/cursor-state.type';
import { unsupportedBullmqVersion } from './unsupported-bullmq-version';

const SCAN_CURSOR_START = '0';
/** Ответ скрипта лока «взят». */
const LOCK_TAKEN = 1;

type StopReason = (typeof CLEANUP_STOP_REASON)[keyof typeof CLEANUP_STOP_REASON];

interface RunState {
  scanned: number;
  keys: number;
  deleted: number;
  orphans: number;
  readonly orphanKeys: string[];
  readonly kept: Record<keyof CleanupKept, number>;
  readonly keptKeys: Record<keyof CleanupKept, string[]>;
  /** Хэши без `timestamp`, увиденные в этом прогоне, — остальные забываются. */
  readonly untimestampedSeen: Set<string>;
  truncatedBy: StopReason | null;
  readonly unsnapshotted: string[];
  readonly unsupported: UnsupportedQueue[];
}

/**
 * Уступить цикл событий: `setImmediate`, а не `await Promise.resolve()` —
 * второй разрешается в той же фазе микрозадач и не пропускает ни таймеры, ни
 * ввод-вывод. Ответы Redis, уже лежащие в буфере, разрешают промисы без
 * возврата в цикл, и обход на тысячи ключей держал бы процесс целиком.
 */
const yieldToEventLoop = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

/** Метасимволы glob в префиксе — буквально: `queue[1]` иначе совпал бы с `queue1`, а не с самим собой. */
const escapeGlob = (text: string): string => text.replace(/[*?[\]\\]/g, (char) => `\\${char}`);

/** Служебные суффиксы очереди — множеством для поиска; источник — замороженный `BULLMQ_KEYS.INTERNAL`. */
const INTERNAL_SUFFIXES: ReadonlySet<string> = new Set(BULLMQ_KEYS.INTERNAL);

/**
 * Уборка «осиротевших» хэшей задач BullMQ: хэш, на который не ссылается ни
 * один индекс статуса (рассинхрон при обрыве соединения посреди перехода).
 *
 * Свойства:
 *  - обход нескольких очередей с общим бюджетом;
 *  - `waiting-children`, пер-задачные суффиксы по регулярке (jobId с `:`
 *    допустим);
 *  - страховочного TTL на ЖИВЫЕ хэши НЕТ: задача, простоявшая в очереди дольше
 *    TTL (ночной батч), теряла бы тело и доставалась воркеру пустой. Память
 *    ограничивают removeOnComplete/removeOnFail и эта уборка.
 *
 * Два шага. Снимок индексов (без огромных sorted set'ов) отсекает живые задачи
 * без команды на каждую: задач в очереди может быть миллион, а сирот — единицы.
 * Кандидаты проверяются и удаляются ОДНИМ скриптом на хэш: тип, вложенная
 * очередь, лок, все индексы, форма и возраст по `timestamp`, отложенная задача
 * дедупликации — и удаление в том же скрипте. Между проверкой и удалением
 * задача не может появиться в индексе. Ошибка проверки — «жива»: безопаснее не
 * удалить сироту, чем удалить живую.
 *
 * Расписание — у вызывающего (`@Cron`, setInterval): библиотека не зависит от
 * Nest. Одновременные `run()` одного экземпляра — один прогон (второй вызов
 * получает тот же отчёт); между репликами — `runLockKey`.
 */
export class OrphanedJobsCleanup {
  private readonly _max: number;
  private readonly _maxKeys: number;
  private readonly _maxRunMs: number;
  private readonly _scanCount: number;
  private readonly _batch: number;
  private readonly _page: number;
  private readonly _maxSnapshotZset: number;
  private readonly _maxListScan: number;
  private readonly _maxListWork: number;
  private readonly _highShare: number;
  private readonly _graceMs: number;
  private readonly _reportKeysLimit: number;
  private readonly _now: () => number;
  private readonly _scripts: ScriptRunner;

  /**
   * Хэш без `timestamp` → когда впервые увиден сиротой (только с
   * `deleteUntimestamped`). Память процесса, а не Redis: свои ключи в
   * пространстве очереди сами стали бы кандидатами. Рестарт лишь откладывает
   * удаление.
   */
  private readonly _untimestampedSince = new Map<string, number>();

  /**
   * Место SCAN, на котором прогон остановился по потолку, — по узлу и очереди.
   * Следующий прогон продолжает с него, а не с нуля: иначе каждый прогон
   * смотрел бы одни и те же первые ключи, и сироты дальше по курсору не
   * убирались бы никогда. Узел — сам объект клиента: узел, выпавший из
   * `scanClients` после failover, уносит свои курсоры, а курсор одного узла не
   * применяется к другому.
   */
  private readonly _cursors = new WeakMap<CleanupScanner, Map<string, CursorState>>();

  /**
   * Очереди, которые узел уже прошёл до конца в ТЕКУЩЕМ цикле обхода. Прогон,
   * оборванный потолком на втором узле, начинал следующий с первого узла с нуля
   * (его курсор удалён как пройденный), снова упирался в тот же потолок — и до
   * последних узлов кластера не доходил никогда. Пройденный узел пропускается,
   * пока цикл не завершат все узлы списка.
   */
  private readonly _finished = new WeakMap<CleanupScanner, Set<string>>();

  /**
   * Кандидаты, собранные прогоном и не проверенные до потолка времени, — по
   * очереди. Курсор SCAN их уже перешагнул: без переноса они ждали бы
   * следующего полного цикла обхода. Не больше кандидатов одного прогона;
   * следующий прогон проверяет их первыми — тем же скриптом, что проверяет все
   * индексы и лок в момент удаления, так что ожившая за это время задача
   * остаётся.
   */
  private readonly _pending = new Map<string, string[]>();

  /** С какой очереди начать следующий прогон: с той, где остановился прошлый. */
  private _resumeQueue = 0;

  private readonly _maxSnapshotList: number;
  private readonly _deleteUntimestamped: boolean;
  private _inFlight: Promise<CleanupReport> | null = null;

  public constructor(
    private readonly _redis: CleanupRedis,
    private readonly _options: OrphanedJobsCleanupOptions,
  ) {
    // Пустой список очередей — запрет, а не «чистить всё»: шаблон `:*` от
    // пустого префикса прошёлся бы по всему Redis.
    if (
      _options.queuePrefixes.length === 0 ||
      _options.queuePrefixes.some((prefix) => prefix.trim() === '')
    ) {
      throw new Error('OrphanedJobsCleanup: список префиксов очередей пуст или содержит пустой префикс');
    }
    // keyPrefix клиента ioredis не получают ключи, которые скрипт собирает сам
    // (de:, dn:, <P>:meta): уборка читала бы и удаляла не те ключи. BullMQ сама
    // keyPrefix не поддерживает — пространство задаёт префикс очереди.
    if (clientKeyPrefix(_redis) !== '') {
      throw new Error(
        `OrphanedJobsCleanup: у клиента keyPrefix «${clientKeyPrefix(_redis)}» — ключи, собранные в скрипте, его не получат; задайте префикс очереди в queuePrefixes`,
      );
    }
    // Ноль в шаге страницы или батча — вечный цикл, а не «не читать»: проверка до первого прогона.
    this._max = positive('maxJobsPerRun', _options.maxJobsPerRun ?? CLEANUP_DEFAULTS.MAX_JOBS_PER_RUN);
    this._maxKeys = positive(
      'maxScannedKeysPerRun',
      _options.maxScannedKeysPerRun ?? CLEANUP_DEFAULTS.MAX_SCANNED_KEYS_PER_RUN,
    );
    this._maxRunMs = positive('maxRunMs', _options.maxRunMs ?? CLEANUP_DEFAULTS.MAX_RUN_MS);
    this._scanCount = positive('scanCount', _options.scanCount ?? CLEANUP_DEFAULTS.SCAN_COUNT);
    this._batch = positive('batchSize', _options.batchSize ?? CLEANUP_DEFAULTS.BATCH_SIZE);
    this._page = positive('snapshotPage', _options.snapshotPage ?? CLEANUP_DEFAULTS.SNAPSHOT_PAGE);
    this._maxSnapshotZset = nonNegative(
      'maxSnapshotZsetSize',
      _options.maxSnapshotZsetSize ?? CLEANUP_DEFAULTS.MAX_SNAPSHOT_ZSET_SIZE,
    );
    // Ноль здесь — отказ, а не «без потолка»: `LPOS … MAXLEN 0` Redis понимает
    // как проход всего списка, и настройка «0» молча значила бы обратное.
    this._maxListScan = positive('maxListScan', _options.maxListScan ?? CLEANUP_DEFAULTS.MAX_LIST_SCAN);
    this._maxListWork = positive(
      'maxListWorkPerBatch',
      _options.maxListWorkPerBatch ?? CLEANUP_DEFAULTS.MAX_LIST_WORK_PER_BATCH,
    );
    this._graceMs = nonNegative('graceMs', _options.graceMs ?? CLEANUP_DEFAULTS.GRACE_MS);
    this._reportKeysLimit = nonNegative(
      'reportKeysLimit',
      _options.reportKeysLimit ?? CLEANUP_DEFAULTS.REPORT_KEYS_LIMIT,
    );
    this._highShare = _options.highOrphanShare ?? CLEANUP_DEFAULTS.HIGH_ORPHAN_SHARE;
    this._deleteUntimestamped = _options.deleteUntimestamped === true;
    this._now = _options.now ?? Date.now;
    this._scripts = new ScriptRunner(_redis);
    // Пустой список узлов — запрет, как и пустой список очередей: обход «ничего»
    // отвечал бы «сирот нет» неотличимо от исправной работы.
    if (Array.isArray(_options.scanClients) && _options.scanClients.length === 0) {
      throw new Error('OrphanedJobsCleanup: scanClients пуст — обходить нечем');
    }
    if (_options.scanClients === undefined && (_redis as { isCluster?: unknown }).isCluster === true) {
      // Бесключевой SCAN клиент кластера шлёт на случайный узел: «сирот нет»
      // означало бы «обошли один узел из N».
      throw new Error(
        'OrphanedJobsCleanup: клиент Redis Cluster без scanClients — обход шёл бы по одному узлу',
      );
    }
    this._maxSnapshotList = positive(
      'maxSnapshotListSize',
      _options.maxSnapshotListSize ?? CLEANUP_DEFAULTS.MAX_SNAPSHOT_LIST_SIZE,
    );
    if (this._maxSnapshotList !== Number.POSITIVE_INFINITY && typeof _redis.llen !== 'function') {
      throw new Error(
        'OrphanedJobsCleanup: потолок снимка списков (maxSnapshotListSize, по умолчанию конечный) требует llen у клиента',
      );
    }
    if (_options.runLockKey !== undefined && _options.runLockKey.trim() === '') {
      throw new Error('OrphanedJobsCleanup: runLockKey пуст');
    }
    if (_options.runLockKey !== undefined && !Number.isFinite(this._maxRunMs)) {
      throw new Error(
        'OrphanedJobsCleanup: лок прогона требует конечный maxRunMs — из него выводится срок лока',
      );
    }
  }

  /** Прогон уборки. Вызов во время идущего прогона того же экземпляра получает его отчёт. */
  public run(): Promise<CleanupReport> {
    if (this._inFlight === null) {
      this._inFlight = this._run().finally(() => {
        this._inFlight = null;
      });
    }
    return this._inFlight;
  }

  /** Хэш задачи очереди? Служебные ключи, пер-задачные хвосты и вложенные пространства — нет. */
  public static isJobKeySuffix(suffix: string): boolean {
    return (
      suffix !== '' &&
      !suffix.startsWith(BULLMQ_KEYS.EMPTY_SEGMENT_PREFIX) &&
      !INTERNAL_SUFFIXES.has(suffix) &&
      !BULLMQ_KEYS.PER_JOB_AUX_SUFFIX.test(suffix) &&
      !BULLMQ_KEYS.NESTED_INTERNAL_PREFIX.test(suffix)
    );
  }

  private async _run(): Promise<CleanupReport> {
    const startedAt = this._now();
    const categories = [...Object.values(KEPT_BY_VERDICT), 'errors' as const];
    const state: RunState = {
      scanned: 0,
      keys: 0,
      deleted: 0,
      orphans: 0,
      orphanKeys: [],
      kept: Object.fromEntries(categories.map((name) => [name, 0])) as Record<keyof CleanupKept, number>,
      keptKeys: Object.fromEntries(categories.map((name) => [name, [] as string[]])) as Record<
        keyof CleanupKept,
        string[]
      >,
      truncatedBy: null,
      unsnapshotted: [],
      unsupported: [],
      untimestampedSeen: new Set(),
    };
    const lock = await this._takeLock();
    if (lock === false) {
      this._options.logger?.log('[OrphanCleanup] лок прогона занят другим прогоном — пропуск');
      return this._report(state, startedAt, true);
    }
    try {
      const scanners = await this._resolveScanners();
      const prefixes = this._options.queuePrefixes;
      const first = this._resumeQueue % prefixes.length;
      for (let step = 0; step < prefixes.length; step += 1) {
        const index = (first + step) % prefixes.length;
        const prefix = prefixes[index] as string;
        await this._cleanupQueue(prefix, scanners, state, startedAt);
        if (state.truncatedBy !== null) {
          // Следующий прогон начнёт с этой очереди и её сохранённого курсора.
          this._resumeQueue = index;
          break;
        }
      }
      if (state.truncatedBy === null) {
        this._resumeQueue = 0;
        // Полный обход: хэш без timestamp, не увиденный сиротой, исчез или ожил —
        // его наблюдение забывается, и память не растёт.
        for (const key of this._untimestampedSince.keys()) {
          if (!state.untimestampedSeen.has(key)) {
            this._untimestampedSince.delete(key);
          }
        }
      }
    } finally {
      if (lock !== null) {
        await this._releaseLock(lock);
      }
    }
    const report = this._report(state, startedAt, false);
    this._logReport(report);
    return report;
  }

  private async _resolveScanners(): Promise<readonly CleanupScanner[]> {
    const configured = this._options.scanClients;
    if (configured === undefined) {
      return [this._redis];
    }
    const scanners = typeof configured === 'function' ? await configured() : configured;
    if (scanners.length === 0) {
      throw new Error('OrphanedJobsCleanup: scanClients вернул пустой список — обходить нечем');
    }
    return scanners;
  }

  private _finishedOf(scanner: CleanupScanner): Set<string> {
    let finished = this._finished.get(scanner);
    if (finished === undefined) {
      finished = new Set();
      this._finished.set(scanner, finished);
    }
    return finished;
  }

  private _cursorsOf(scanner: CleanupScanner): Map<string, CursorState> {
    let cursors = this._cursors.get(scanner);
    if (cursors === undefined) {
      cursors = new Map();
      this._cursors.set(scanner, cursors);
    }
    return cursors;
  }

  private async _cleanupQueue(
    prefix: string,
    scanners: readonly CleanupScanner[],
    state: RunState,
    startedAt: number,
  ): Promise<void> {
    // Версия BullMQ — до всего остального: незнакомую раскладку ключей уборка
    // не трогает вовсе (ни снимка, ни SCAN). Нет версии — тоже отказ.
    let raw: unknown;
    try {
      raw = await this._scripts.eval(
        CLEANUP_SCRIPTS.readQueueVersion,
        1,
        `${prefix}:${BULLMQ_SUPPORTED_VERSIONS.META_SUFFIX}`,
        BULLMQ_SUPPORTED_VERSIONS.META_FIELD,
      );
    } catch (error) {
      // `meta` не хэш (WRONGTYPE) или Redis отказал: эта очередь не убирается,
      // а остальные — да. Исключение уронило бы весь прогон, и очереди после
      // этой не убирались бы никогда.
      state.unsupported.push({
        prefix,
        version: null,
        reason: `meta очереди не читается: ${errorText(error)}`,
      });
      return;
    }
    const reason = unsupportedBullmqVersion(raw);
    if (reason !== null) {
      state.unsupported.push({ prefix, version: typeof raw === 'string' ? raw : null, reason });
      return;
    }
    // Снимок индексов — ДО обхода хэшей: хэш и запись в индексе создаются
    // атомарно одним Lua-скриптом BullMQ, значит задача, чей хэш нашёл SCAN,
    // в момент снимка уже была в каком-то индексе.
    let snapshot: Awaited<ReturnType<OrphanedJobsCleanup['_snapshot']>>;
    try {
      snapshot = await this._snapshot(prefix, state, startedAt);
    } catch (error) {
      // Индекс не своего типа (WRONGTYPE) или отказ Redis: без снимка кандидатов
      // не отличить от живых — очередь не убирается, остальные убираются.
      state.unsupported.push({
        prefix,
        version: typeof raw === 'string' ? raw : null,
        reason: `индексы очереди не читаются: ${errorText(error)}`,
      });
      return;
    }
    if (snapshot === null) {
      // Снимок не собран (время вышло, список больше потолка): без него
      // кандидатов не отличить от живых задач — очередь в этом прогоне не трогается.
      return;
    }
    // SCAN может вернуть ключ повторно, а узлы кластера — нет, но дубль из
    // одного узла считался бы дважды и проверялся бы дважды.
    const seen = new Set<string>();
    const candidates: string[] = [];
    for (const jobId of this._pending.get(prefix) ?? []) {
      seen.add(jobId);
      if (!snapshot.ids.has(jobId)) {
        candidates.push(jobId);
      }
    }
    this._pending.delete(prefix);
    // Префиксы возможных вложенных очередей: ключи `<prefix>:<P>:meta` и `<prefix>:<P>:id`.
    const nestedSuspects = new Set<string>();
    const pattern = `${escapeGlob(prefix)}:*`;
    if (scanners.every((scanner) => this._finished.get(scanner)?.has(prefix) === true)) {
      // Все узлы списка прошли цикл — следующий начинается с нуля на каждом.
      for (const scanner of scanners) {
        this._finished.get(scanner)?.delete(prefix);
      }
    }
    scanning: for (const scanner of scanners) {
      const finished = this._finishedOf(scanner);
      if (finished.has(prefix)) {
        continue;
      }
      const cursors = this._cursorsOf(scanner);
      const saved = cursors.get(prefix);
      let cursor = saved?.cursor ?? SCAN_CURSOR_START;
      // Ключи порции, на которой прошлый прогон остановился, уже просмотренные.
      let done: ReadonlySet<string> = new Set(saved?.done ?? []);
      do {
        if (this._timeIsUp(startedAt)) {
          state.truncatedBy = CLEANUP_STOP_REASON.time;
          cursors.set(prefix, { cursor, done: [...done] });
          break scanning;
        }
        const current = cursor;
        const [next, keys] = await scanner.scan(cursor, 'MATCH', pattern, 'COUNT', this._scanCount);
        cursor = next;
        const handled: string[] = [];
        for (const key of keys) {
          if (done.has(key)) {
            handled.push(key);
            continue;
          }
          if (state.scanned >= this._max || state.keys >= this._maxKeys) {
            state.truncatedBy =
              state.scanned >= this._max ? CLEANUP_STOP_REASON.jobs : CLEANUP_STOP_REASON.keys;
            // С этой порции, без её просмотренных ключей: просмотренное не повторяется, непросмотренное не теряется.
            cursors.set(prefix, { cursor: current, done: handled });
            break scanning;
          }
          handled.push(key);
          state.keys++;
          const jobId = key.slice(prefix.length + 1);
          const marker = NESTED_MARKER.exec(jobId);
          if (marker !== null) {
            nestedSuspects.add(`${prefix}:${marker[1]}`);
          }
          if (!OrphanedJobsCleanup.isJobKeySuffix(jobId) || seen.has(jobId)) {
            continue;
          }
          seen.add(jobId);
          state.scanned++;
          if (!snapshot.ids.has(jobId)) {
            candidates.push(jobId);
          }
        }
        done = new Set();
        await yieldToEventLoop();
      } while (cursor !== SCAN_CURSOR_START);
      // Узел обойдён до конца: в этом цикле он больше не обходится, в следующем — с нуля.
      cursors.delete(prefix);
      finished.add(prefix);
    }
    const nested = await this._nestedQueues(nestedSuspects);
    if (nested.length > 0) {
      // Задачи вложенной очереди по индексам этой не отличить от сирот: до
      // того, как её уберут из пространства, эта очередь не убирается вовсе.
      state.unsupported.push({
        prefix,
        version: raw as string,
        reason: `внутри пространства очереди — другие очереди BullMQ (префикс вложен): ${nested.join(', ')}`,
      });
      return;
    }
    await this._verifyAndDelete(prefix, candidates, snapshot.listLength, state, startedAt);
  }

  /** Какие из подозреваемых префиксов — очереди BullMQ (признаки — `BULLMQ_KEYS.NESTED_QUEUE_*`). */
  private async _nestedQueues(suspects: ReadonlySet<string>): Promise<string[]> {
    const found: string[] = [];
    for (const suspect of suspects) {
      const reply = await this._scripts.eval(
        CLEANUP_SCRIPTS.nestedQueue,
        1,
        `${suspect}:${BULLMQ_KEYS.NESTED_QUEUE_META_SUFFIX}`,
        suspect,
      );
      if (Number(reply) === 1) {
        found.push(suspect);
      }
    }
    return found.sort();
  }

  /**
   * jobId из индексов очереди: постранично, каждый индекс читается один раз.
   * Sorted set больше `maxSnapshotZsetSize` не читается — его задачи станут
   * кандидатами и проверятся ZSCORE в скрипте удаления. `listLength` — сколько
   * id прочитано из списков: из него выводится батч удаления.
   */
  private async _snapshot(
    prefix: string,
    state: RunState,
    startedAt: number,
  ): Promise<{ ids: Set<string>; listLength: number } | null> {
    const ids = new Set<string>();
    let timedOut = false;
    let read = 0;
    const readAll = async (
      key: string,
      range: (key: string, start: number, stop: number) => Promise<string[]>,
    ) => {
      for (let start = 0; ; start += this._page) {
        if (this._timeIsUp(startedAt)) {
          // Потолок длительности действует и на снимок: огромный индекс иначе
          // читался бы дольше лока прогона, и стартовал бы параллельный.
          timedOut = true;
          return;
        }
        const page = await range(key, start, start + this._page - 1);
        read += page.length;
        for (const id of page) {
          ids.add(id);
        }
        if (page.length < this._page) {
          return;
        }
        await yieldToEventLoop();
      }
    };
    for (const status of BULLMQ_KEYS.LIST_STATUSES) {
      const key = `${prefix}:${status}`;
      if (
        this._maxSnapshotList !== Number.POSITIVE_INFINITY &&
        ((await this._redis.llen?.(key)) ?? 0) > this._maxSnapshotList
      ) {
        state.unsnapshotted.push(key);
        return null;
      }
      await readAll(key, (indexKey, start, stop) => this._redis.lrange(indexKey, start, stop));
      if (timedOut) {
        state.truncatedBy = CLEANUP_STOP_REASON.time;
        return null;
      }
    }
    const listLength = read;
    for (const status of BULLMQ_KEYS.ZSET_STATUSES) {
      const key = `${prefix}:${status}`;
      if ((await this._redis.zcard(key)) > this._maxSnapshotZset) {
        state.unsnapshotted.push(key);
        continue;
      }
      await readAll(key, (indexKey, start, stop) => this._redis.zrange(indexKey, start, String(stop)));
      if (timedOut) {
        state.truncatedBy = CLEANUP_STOP_REASON.time;
        return null;
      }
    }
    return { ids, listLength };
  }

  /**
   * Проверка и удаление кандидатов: скрипт на хэш, скрипты батча — одним
   * пайплайном по SHA. Батч не больше `batchSize` и такой, чтобы «кандидатов ×
   * длина списков» не превышало `maxListWorkPerBatch`: пайплайн Redis исполняет
   * подряд, и сироты при длинном `wait` иначе держали бы его секундами.
   */
  private async _verifyAndDelete(
    prefix: string,
    jobIds: readonly string[],
    listLength: number,
    state: RunState,
    startedAt: number,
  ): Promise<void> {
    // Списки — последними: LPOS отсутствующего id проходит список целиком, а
    // задача, найденная ZSCORE/SISMEMBER, до списков не доходит.
    const indexKeys = [
      ...BULLMQ_KEYS.ZSET_STATUSES,
      ...BULLMQ_KEYS.SET_STATUSES,
      ...BULLMQ_KEYS.LIST_STATUSES,
    ].map((status) => `${prefix}:${status}`);
    const dryRun = this._options.dryRun === true;
    const listScan =
      this._maxListScan === Number.POSITIVE_INFINITY ? LIST_SCAN_ARG.unbounded : this._maxListScan;
    const perCandidate = Math.max(
      1,
      Math.min(listLength, this._maxListScan * BULLMQ_KEYS.LIST_STATUSES.length),
    );
    const batchSize = Math.max(1, Math.min(this._batch, Math.floor(this._maxListWork / perCandidate)));
    for (let start = 0; start < jobIds.length; start += batchSize) {
      if (this._timeIsUp(startedAt)) {
        state.truncatedBy = CLEANUP_STOP_REASON.time;
        // Курсор их уже перешагнул — проверит следующий прогон.
        this._pending.set(prefix, jobIds.slice(start));
        return;
      }
      const batch = jobIds.slice(start, start + batchSize);
      const now = this._now();
      const calls = batch.map((jobId): readonly [number, ...(string | number)[]] => {
        const jobKey = `${prefix}:${jobId}`;
        const since = this._untimestampedSince.get(jobKey);
        const confirmed =
          this._deleteUntimestamped && since !== undefined && now - since >= this._graceMs && now > since;
        const auxKeys = BULLMQ_KEYS.AUX_SUFFIXES_DELETED_WITH_JOB.map((suffix) => `${jobKey}:${suffix}`);
        return [
          1 + indexKeys.length + auxKeys.length,
          jobKey,
          ...indexKeys,
          ...auxKeys,
          jobId,
          indexKeys.length,
          this._graceMs,
          dryRun ? DRY_RUN_ARG.on : DRY_RUN_ARG.off,
          `${prefix}:`,
          listScan,
          confirmed ? UNTIMESTAMPED_ARG.confirmed : UNTIMESTAMPED_ARG.firstSight,
        ];
      });
      const replies = await this._scripts.pipeline(CLEANUP_SCRIPTS.checkAndDelete, calls);
      for (const [index, jobId] of batch.entries()) {
        const reply = replies[index] as [Error | null, unknown];
        if (reply[0] !== null) {
          // Ошибка команды — «не смогли проверить»: не удалено и не «жива».
          this._keep(state, 'errors', `${prefix}:${jobId}`);
          continue;
        }
        this._account(state, `${prefix}:${jobId}`, Number(reply[1]), now);
      }
      await yieldToEventLoop();
    }
  }

  private _account(state: RunState, key: string, verdict: number, now: number): void {
    if (verdict === ORPHAN_VERDICT.untimestamped) {
      state.untimestampedSeen.add(key);
      if (
        this._deleteUntimestamped &&
        !this._untimestampedSince.has(key) &&
        this._untimestampedSince.size < CLEANUP_DEFAULTS.MAX_UNTIMESTAMPED_TRACKED
      ) {
        this._untimestampedSince.set(key, now);
      }
      this._keep(state, 'untimestamped', key);
      return;
    }
    // Любой другой ответ — хэш больше не «сирота без timestamp» (удалён, ожил,
    // исчез): прежнее наблюдение не должно разрешить удаление его преемника.
    this._untimestampedSince.delete(key);
    if (verdict === ORPHAN_VERDICT.deleted || verdict === ORPHAN_VERDICT.wouldDelete) {
      state.orphans++;
      if (verdict === ORPHAN_VERDICT.deleted) {
        state.deleted++;
      }
      if (state.orphanKeys.length < this._reportKeysLimit) {
        state.orphanKeys.push(key);
      }
      this._options.logger?.debug?.(
        `[OrphanCleanup] ${verdict === ORPHAN_VERDICT.deleted ? 'удалена' : 'dryRun: удалили бы'} ${key}`,
      );
      return;
    }
    // Незнакомый ответ — «не смогли определить», то есть не удалено и не «жива».
    this._keep(state, KEPT_BY_VERDICT[verdict] ?? 'errors', key);
  }

  private _keep(state: RunState, category: keyof CleanupKept, key: string): void {
    state.kept[category]++;
    const sample = state.keptKeys[category];
    if (sample.length < this._reportKeysLimit) {
      sample.push(key);
    }
  }

  /** `null` — лок не настроен, `false` — занят другим прогоном, строка — токен своего лока. */
  private async _takeLock(): Promise<string | null | false> {
    const key = this._options.runLockKey;
    if (key === undefined) {
      return null;
    }
    const token = randomUUID();
    const reply = await this._scripts.eval(
      CLEANUP_SCRIPTS.acquireRunLock,
      1,
      key,
      token,
      this._maxRunMs + CLEANUP_DEFAULTS.RUN_LOCK_MARGIN_MS,
    );
    return Number(reply) === LOCK_TAKEN ? token : false;
  }

  private async _releaseLock(token: string): Promise<void> {
    try {
      await this._scripts.eval(CLEANUP_SCRIPTS.releaseRunLock, 1, this._options.runLockKey as string, token);
    } catch (error) {
      // Не снятый лок истечёт по сроку; отказ снятия не должен прятать отчёт прогона.
      this._options.logger?.warn(`[OrphanCleanup] лок прогона не снят (истечёт сам): ${errorText(error)}`);
    }
  }

  private _timeIsUp(startedAt: number): boolean {
    return this._now() - startedAt >= this._maxRunMs;
  }

  private _report(state: RunState, startedAt: number, lockedOut: boolean): CleanupReport {
    return {
      scanned: state.scanned,
      scannedKeys: state.keys,
      deleted: state.deleted,
      durationMs: this._now() - startedAt,
      highOrphanShare: state.scanned > 0 && state.orphans / state.scanned > this._highShare,
      orphans: state.orphans,
      orphanKeys: state.orphanKeys,
      keptKeys: state.keptKeys,
      dryRun: this._options.dryRun === true,
      kept: { ...state.kept },
      truncatedBy: state.truncatedBy,
      unsnapshottedIndexes: state.unsnapshotted,
      unsupportedQueues: state.unsupported,
      lockedOut,
    };
  }

  private _logReport(report: CleanupReport): void {
    const logger = this._options.logger;
    if (logger === undefined) {
      return;
    }
    const { kept } = report;
    logger.log(
      `[OrphanCleanup] scanned=${report.scanned} orphans=${report.orphans} deleted=${report.deleted}` +
        ` dryRun=${report.dryRun} kept(alive=${kept.alive} locked=${kept.locked} young=${kept.young}` +
        ` notHash=${kept.notHash} untimestamped=${kept.untimestamped} notJob=${kept.notJob} nested=${kept.nested})` +
        ` ms=${report.durationMs}`,
    );
    if (report.truncatedBy !== null) {
      logger.warn(`[OrphanCleanup] обход прерван потолком «${report.truncatedBy}»: просмотрено не всё`);
    }
    if (kept.errors + kept.unverifiable + kept.unknownAge > 0) {
      logger.warn(
        `[OrphanCleanup] не проверены и оставлены: errors=${kept.errors} unverifiable=${kept.unverifiable}` +
          ` unknownAge=${kept.unknownAge}`,
      );
    }
    for (const queue of report.unsupportedQueues) {
      logger.warn(`[OrphanCleanup] очередь ${queue.prefix} не убиралась: ${queue.reason}`);
    }
    if (report.unsnapshottedIndexes.length > 0) {
      logger.warn(
        `[OrphanCleanup] индексы больше потолка снимка: ${report.unsnapshottedIndexes.join(', ')}` +
          ' (sorted set — задачи проверены поштучно, список — очередь не убиралась)',
      );
    }
  }
}

function positive(name: string, value: number): number {
  if (!(value === Number.POSITIVE_INFINITY || (Number.isInteger(value) && value > 0))) {
    throw new Error(`OrphanedJobsCleanup: ${name} — целое больше нуля или Infinity, получено ${value}`);
  }
  return value;
}

function nonNegative(name: string, value: number): number {
  if (!(value === Number.POSITIVE_INFINITY || (Number.isInteger(value) && value >= 0))) {
    throw new Error(`OrphanedJobsCleanup: ${name} — целое ≥ 0 или Infinity, получено ${value}`);
  }
  return value;
}
