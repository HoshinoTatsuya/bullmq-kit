import { deepFreeze } from '../../deep-freeze';

/**
 * Раскладка ключей очереди BullMQ 5: `<prefix>:<queue>:<суффикс>`.
 *
 * `waiting-children` — индекс родителей flow-задач. Без него в списке хэш
 * родителя, ждущего детей, считался бы сиротой и удалялся.
 */
export const BULLMQ_KEYS = {
  ZSET_STATUSES: ['prioritized', 'delayed', 'failed', 'completed', 'waiting-children'],
  LIST_STATUSES: ['wait', 'active', 'paused'],
  /**
   * Индексы-множества: `stalled` — задачи active, заподозренные в зависании.
   * В снимок не входит (задача в нём и так в active), но поштучная проверка
   * перед удалением смотрит и его: проверка дешёвая (SISMEMBER), а лишний
   * рубеж здесь — в пользу «жива».
   */
  SET_STATUSES: ['stalled'],
  /**
   * Пер-задачные ключи, удаляемые ВМЕСТЕ с хэшем сироты, — ровно то, что
   * удаляет сам BullMQ при удалении задачи (removeJobKeys.lua). `:lock` здесь
   * НЕТ и быть не должно: лок значит, что воркер, возможно, ещё держит задачу,
   * и удалённый лок отдал бы её второму воркеру. Лок истечёт сам.
   */
  AUX_SUFFIXES_DELETED_WITH_JOB: ['logs', 'dependencies', 'processed', 'failed', 'unsuccessful'],
  /** Поле хэша задачи с временем создания (мс), его пишет storeJob.lua в том же HMSET, что создаёт хэш. */
  JOB_TIMESTAMP_FIELD: 'timestamp',
  /**
   * Поля, которые storeJob.lua пишет тем же HMSET, что создаёт хэш задачи
   * (вместе с `timestamp`). Положительная форма: хэш с `timestamp`, но без
   * них — не хэш задачи (чужой ключ, служебный хэш будущей версии), и уборка
   * его не трогает. Отрицательной формы («не служебный суффикс») мало: она
   * пропускает всё, чего нет в списке.
   */
  JOB_SHAPE_FIELDS: ['name', 'opts'],
  /** `<jobKey>:lock` — лок воркера, держащего задачу; живой лок — задача жива. */
  JOB_LOCK_SUFFIX: 'lock',
  /**
   * Признаки очереди BullMQ по её префиксу `P`: хэш `P:meta` с одним из полей
   * (`version` пишет `new Queue`, `opts.maxLenEvents` — каждый скрипт добавления
   * задачи) или строка-счётчик `P:id` (INCR в каждом скрипте добавления).
   * Префикс BullMQ может содержать `:`, а имя очереди — нет, поэтому очередь
   * `x` с префиксом `bull:jobs` живёт внутри пространства очереди `bull:jobs`.
   */
  NESTED_QUEUE_META_SUFFIX: 'meta',
  /** Поле `meta`, которое ставит `Queue.pause()` и снимает `resume()` (BullMQ 5 и 6). */
  QUEUE_PAUSED_FIELD: 'paused',
  NESTED_QUEUE_META_FIELDS: ['version', 'opts.maxLenEvents'],
  NESTED_QUEUE_ID_SUFFIX: 'id',
  /** Поле хэша задачи с id дедупликации. */
  JOB_DEDUPLICATION_ID_FIELD: 'deid',
  /** `<prefix>:de:<id>` — строка с id ТЕКУЩЕЙ задачи дедупликации. */
  DEDUPLICATION_KEY_PREFIX: 'de:',
  /** `<prefix>:dn:<id>` — хэш отложенной «следующей» задачи; поле `jid` — id, который она получит. */
  DEDUPLICATION_NEXT_KEY_PREFIX: 'dn:',
  DEDUPLICATION_NEXT_JOB_ID_FIELD: 'jid',
  /** Служебные ключи очереди — не хэши задач. Массив, а не Set: замороженный Set всё равно принимал бы `add`. */
  INTERNAL: [
    'events',
    'id',
    'meta',
    'repeat',
    'stalled',
    'stalled-check',
    'marker',
    'pc',
    'limiter',
    'de',
    'metrics',
    'prioritized',
    'delayed',
    'failed',
    'completed',
    'waiting-children',
    'wait',
    'active',
    'paused',
  ],
  /**
   * Пер-задачные служебные ключи `<jobId>:lock`, `<jobId>:logs` и родня.
   * Опаснее всех `:lock` — он держит АКТИВНУЮ задачу: удалив его, мы отдаём
   * её второму воркеру. Признак «в суффиксе есть двоеточие» не годится: jobId
   * вида `<uuid>::<projectId>` содержит двоеточия, и уборка отключилась бы молча.
   * `unprocessed`, `dependencies-count`, `processed-count` BullMQ 5 и 6 не
   * создают (проверено по их Lua) — они здесь только затем, чтобы такой ключ
   * не сочли хэшем задачи; удалять их вместе с сиротой незачем (см. спеку
   * против removeJobKeys.lua).
   */
  PER_JOB_AUX_SUFFIX:
    /:(lock|logs|dependencies|processed|unprocessed|unsuccessful|failed|dependencies-count|processed-count|de)$/,
  /**
   * Префиксы вложенных служебных пространств: `repeat:*`, `metrics:*`, `de:*`,
   * `dn:*`. `dn:<deduplicationId>` — ХЭШ отложенной «последней» задачи
   * дедупликации (`keepLastIfActive`, storeDeduplicatedNextJob.lua): в индексах
   * статусов его нет до завершения активной, и без префикса здесь уборка
   * удаляла его как сироту — задача молча не выполнялась (проба 13.11.2026 на
   * BullMQ 5.81.3).
   */
  NESTED_INTERNAL_PREFIX: /^(repeat|metrics|de|dn|limiter):/,
  /**
   * Пустой сегмент сразу после префикса очереди (`<P>::…`) — ни задача, ни ключ
   * очереди. Так BullMQ 6 отделяет relay: `<prefix>::relay:{<ns>}:…`, среди них
   * хэши узлов и удержанных сообщений без `timestamp`. При префиксе relay,
   * равном префиксу очереди (`bull:orders`), они лежат ВНУТРИ её пространства,
   * и `deleteUntimestamped` удалял их по второму наблюдению (проба на BullMQ
   * 6.7.7: `:node`, `:layout`, `:<topic>:ret`). Id задачи с ведущим `:` BullMQ
   * формально пропускает (`:a:b`), но такая задача лишь не убирается — ошибка в
   * безопасную сторону.
   */
  EMPTY_SEGMENT_PREFIX: ':',
} as const;

deepFreeze(BULLMQ_KEYS);
