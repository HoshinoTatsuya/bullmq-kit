import { HandleTracker } from './handle-tracker';

/**
 * Гейт КАЖДОГО теста: после его `afterEach` в процессе не остаётся живых
 * ресурсов, держащих цикл событий (таймер, сокет, сервер), которые тест
 * создал. Иначе jest замечает их только в конце прогона и без имени файла:
 * воркер не выходит за 500 мс и снимается с «A worker process has failed to
 * exit gracefully», а тесты зелёные. Здесь — красный тест с местом создания.
 *
 * В счёт идёт созданное от начала `beforeEach` теста до его `afterEach`.
 * Созданное в `beforeAll` живёт до `afterAll` и не в счёт; ресурс, который
 * тест оставил жить дольше себя, — в счёт. Поэтому общий клиент файла
 * подключается целиком в `beforeAll`: клиент кластера, например, открывает
 * сокет узла лениво, при первой команде в его слот. Закрытие сокета после
 * `quit()` занимает миллисекунды, поэтому проверка ждёт до секунды.
 */
const tracker = new HandleTracker();

// Сначала — один оборот цикла: клиент Redis из `beforeAll` открывает сокет в
// process.nextTick, а цепочка хуков jest на промисах доходит сюда раньше, и
// общий клиент файла засчитывался бы тесту.
beforeEach(async () => {
  await new Promise((resolve) => setImmediate(resolve));
  tracker.start();
});

// Учёт снимается ОТДЕЛЬНЫМ синхронным хуком раньше проверки: таймер таймаута,
// который jest заводит на каждый хук, иначе попал бы в учёт и был бы жив во
// время проверки — ложная находка в каждом тесте.
afterEach(() => {
  tracker.stop();
});

afterEach(async () => {
  for (let poll = 0; poll < 100 && tracker.alive().length > 0; poll += 1) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  const left = tracker.alive();
  if (left.length > 0) {
    throw new Error(
      `тест оставил ресурсы, держащие процесс (${left.map(({ type }) => type).join(', ')}):\n` +
        left.map(({ type, stack }) => `${type} создан:${stack.replace(/^Error/, '')}`).join('\n\n'),
    );
  }
});
