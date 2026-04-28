# Training Logic Review

Дата проверки: 2026-04-28

Проверенные файлы:

- `polymarket_signal_bot/scoring.py`
- `polymarket_signal_bot/cohorts.py`
- `polymarket_signal_bot/auto_trainer.py`
- `crypto-dashboard/src/components/WalletCohorts.jsx`
- `server/api.py` как слой между React и обучением

## Краткий вывод

Текущая система уже имеет правильный общий контур: кнопка `Train Model` запускает backend-цикл, цикл вызывает скоринг, затем обновление когорт, затем создание модели/статистики выхода. Данные берутся из `data/indexer.db`, а `scored_wallets`, `wallet_cohorts`, `exit_stats.json` и `exit_examples.json` используются дашбордом/API.

Но полному ТЗ реализация соответствует только частично. Самые важные расхождения:

1. Скоринг фильтрует `event_type = 'OrderFilled'`, но часть метрик считается как прокси, а не по точной логике из ТЗ.
2. Когорты сейчас назначаются по `score + trade_count + volume`, а не по `sharpe/win_rate/trade_count/max_drawdown/avg_hold_time`.
3. Early Exit Model пока не строит строгие пары `BUY -> SELL`, не делает train/test split, не считает `MAE/R2`, не включает fallback на dummy-модель при плохом `R2`.
4. React показывает кнопку `Train Model` и счетчик `exit examples`, но не отображает таблицу `EXIT EXAMPLES`.

## Этап 1: Scoring

Статус: частично соответствует.

Что совпадает:

- `calculate_all()` работает с `raw_transactions`.
- В SQL есть фильтр `event_type = 'OrderFilled'`.
- Технические события `TransferSingle`, `TransferBatch` и другие не попадают в основную выборку скоринга.
- Расчет сделан через SQL CTE/агрегации, а не через Python-цикл по каждому кошельку.
- Результат сохраняется в `scored_wallets`.
- Перед сохранением таблица очищается через `DELETE FROM scored_wallets`, затем идет вставка/upsert.
- Есть индексы для ускорения запросов по `OrderFilled`, `side`, `user_address`, `timestamp`.

Отклонения:

| Требование ТЗ | Текущая реализация | Статус |
|---|---|---|
| `scored_wallets` должна иметь поле `user_address` | Таблица использует `wallet` как primary key и дополнительно `address`; поля `user_address` нет | Нужно исправить или добавить совместимый alias |
| PnL: SELL положительный, BUY отрицательный | Реализовано как proxy `SELL +notional`, `BUY -notional` | В целом совпадает с простой формулой |
| Sharpe: средний PnL сделки / std PnL сделки | Сейчас считается в Python из daily PnL variance после SQL-агрегации | Не полностью соответствует |
| Max drawdown по кумулятивной кривой PnL | Считается SQL-окном, но на дневной кривой, а не на каждой сделке | Частично соответствует |
| Profit factor по прибыльным/убыточным сделкам | Считается по proxy BUY/SELL notional | Частично соответствует, но это не realized pair PnL |
| Win rate как доля прибыльных сделок | Сейчас BUY почти всегда отрицательный, SELL положительный; получается proxy доли SELL/положительных proxy-сделок | Не соответствует смыслу win rate |
| Avg hold time между BUY и SELL | Используется `LEAD(timestamp)` для следующей сделки по wallet/market, без требования `BUY -> SELL` | Не соответствует |
| Consistency как доля месяцев с положительным PnL | Сейчас это доля активных дней в периоде активности | Не соответствует |
| `calculate_all()` возвращает DataFrame по всем кошелькам | Да, с учетом `limit/min_trades` | Соответствует |

Что нужно исправить:

- Добавить в `scored_wallets` поле `user_address` или совместимый view/alias, чтобы downstream-код мог читать именно ожидаемую схему.
- Пересчитать `consistency` как долю месяцев, где месячный PnL > 0.
- Пересчитать `avg_hold_time` только по парам `BUY -> SELL` внутри одного `user_address + market_id`.
- Явно определить `win_rate`: либо по realized `BUY -> SELL` парам, либо документировать текущий proxy. Для соответствия ТЗ нужен realized/profit proxy по закрытым парам.
- Перенести расчет Sharpe ближе к ТЗ: средний PnL сделки / std PnL сделки, либо отдельным SQL CTE, либо финальной векторной операцией без Python-цикла.

## Этап 2: Cohorts

Статус: существенно не соответствует ТЗ по логике порогов.

Что совпадает:

- `update_cohorts()` читает свежие данные из `scored_wallets`.
- Используется `data/best_policy.json`, если файл существует.
- Старые записи в `wallet_cohorts` очищаются перед обновлением.
- Возвращается словарь с количеством кошельков по когортам.
- Таблица `wallet_cohorts` реально заполняется и используется API/дашбордом.

Отклонения:

| Требование ТЗ | Текущая реализация | Статус |
|---|---|---|
| STABLE: `sharpe >= 1.5`, `win_rate >= 0.55`, `trade_count >= 50`, `max_drawdown <= 20`, `avg_hold_time >= 300` | STABLE сейчас: `score >= stable_score`, `trade_count >= stable_min_trades`, `volume >= stable_min_volume` | Не соответствует |
| CANDIDATE: `sharpe >= 1.0`, `win_rate >= 0.50`, `trade_count >= 20`, `max_drawdown <= 30` | CANDIDATE тоже назначается по `score/trade_count/volume` | Не соответствует |
| WATCH: `win_rate >= 0.45`, `trade_count >= 10` | WATCH назначается по `score/trade_count/volume` | Не соответствует |
| NOISE: все остальные | Есть | Соответствует |
| `wallet_cohorts(user_address, cohort, updated_at)` | Таблица использует `wallet`, `status`, плюс дополнительные метрики | Схема не совпадает |
| Пороговые значения по умолчанию из ТЗ | В `CohortThresholds` другие дефолты: score/volume/trades | Не соответствует |

Что нужно исправить:

- Переделать `CohortThresholds` под метрики из ТЗ: `stable_min_sharpe`, `stable_min_win_rate`, `stable_min_trades`, `stable_max_drawdown`, `stable_min_avg_hold_time`, и аналогично для CANDIDATE/WATCH.
- Обновить SQL `CASE` в `update_cohorts()` так, чтобы он использовал `sharpe`, `win_rate`, `trade_count`, `max_drawdown`, `avg_hold_time`.
- Добавить совместимые поля `user_address` и `cohort` либо заменить `wallet/status`, если это не сломает текущий API.
- Поддержать `best_policy.json` в формате, который может задавать именно эти пороги, а не только `score/volume/trades`.

## Этап 3: Early Exit Model

Статус: прототип есть, но строгому ТЗ в основном не соответствует.

Что совпадает:

- Обучение запускается после скоринга и когорт.
- Берутся только кошельки из `STABLE/CANDIDATE`.
- В выборке используется `event_type = 'OrderFilled'`.
- Файлы `data/exit_model.pkl`, `data/exit_stats.json`, `data/exit_examples.json` создаются.
- При отсутствии `sklearn` есть rule-based fallback.

Отклонения:

| Требование ТЗ | Текущая реализация | Статус |
|---|---|---|
| Строить пары `BUY -> SELL` для одного адреса и рынка | Используется `LEAD()` по следующей сделке, но не проверяется, что entry = BUY и exit = SELL | Не соответствует |
| Без промежуточных сделок по рынку | `LEAD()` берет ближайшую следующую сделку, но может быть BUY->BUY или SELL->SELL | Частично, но логика выхода неверная |
| `time_to_exit` как target | Есть `hold_seconds` | Соответствует |
| `position_size` | Есть `entry_notional` | Соответствует по смыслу |
| `market_volatility` за 60 минут до входа | Используется `ABS(exit_price - entry_price)` как `volatility_proxy` | Не соответствует |
| `hour_of_day` | Есть | Соответствует |
| `day_of_week` | Нет | Не соответствует |
| `whale_sharpe`, `whale_win_rate` из `scored_wallets` | Нет join к `scored_wallets` | Не соответствует |
| Если пар меньше 50, модель не обучается, только медианные статистики | Такого порога нет; Ridge обучается при любом числе примеров, если установлен sklearn | Не соответствует |
| Train/test 80/20 | Нет, Ridge обучается на всех примерах | Не соответствует |
| MAE и R2 на test | Не считаются | Не соответствует |
| Если R2 < 0, Dummy-модель | Нет | Не соответствует |
| Сохранение через `joblib` | Используется `pickle.dump()` | Не соответствует |
| `exit_examples.json`: 5 случайных пар из test | Сохраняются первые 10 примеров из общей выборки | Не соответствует |
| Поля примеров: `whale_address`, `market_id`, `entry_time`, `exit_time`, `pnl_percent`, `predicted_time` | Сейчас поля: `wallet`, `entry_ts`, `exit_ts`, `hold_seconds`, `entry_notional`, `volatility_proxy`, `hour_of_day`, `pnl_proxy`, `partial_fixation` | Не соответствует |

Что нужно исправить:

- Генерировать обучающие примеры только из строгих пар `BUY -> ближайший SELL` внутри `user_address + market_id`.
- Добавить расчет `market_volatility` как стандартное отклонение цен по рынку за 60 минут до входа.
- Добавить признаки `day_of_week`, `whale_sharpe`, `whale_win_rate`.
- Ввести правило: если примеров < 50, не обучать Ridge, сохранить только статистики и медианную dummy/rule-модель.
- Делить данные на train/test 80/20.
- Считать `MAE` и `R2`.
- Если `R2 < 0`, сохранять Dummy-модель, предсказывающую медиану.
- Сохранять модель через `joblib.dump()`.
- Сохранять `exit_examples.json` в формате, ожидаемом ТЗ, включая `predicted_time`.

## Общий цикл Train Model

Статус: структура соответствует, детали частично расходятся.

Что совпадает:

- React-кнопка `Train Model` находится в `WalletCohorts.jsx`.
- Кнопка вызывает `POST /api/training/start`.
- Backend запускает training-процесс через `server/api.py`.
- `run_full_training_cycle()` последовательно вызывает:
  - `calculate_all()`
  - `save_scored_wallets()`
  - `update_cohorts()`
  - `train_exit_model()`
- Ошибки цикла логируются, предыдущие модель/когорты не должны падать вместе с сервером.
- Есть `Sample 10k`, который запускает обучение с `limit=10000`.

Отклонения:

| Требование ТЗ | Текущая реализация | Статус |
|---|---|---|
| Каждый этап логируется | Основные этапы логируются | Соответствует |
| Ошибки не роняют весь цикл | Общий try/except есть | В целом соответствует |
| `SCORED` читает top-50 из `scored_wallets` по Sharpe | API возвращает top wallets `ORDER BY score DESC, volume DESC LIMIT 10` | Не соответствует |
| STABLE/CANDIDATE читаются из `wallet_cohorts` | Да, но по полю `status` | Частично соответствует |
| `EXIT EXAMPLES` отображается таблицей из `exit_examples.json` | React показывает только число `exit examples`; таблицы нет | Не соответствует |
| Кнопка показывает полную цепочку `raw_transactions -> scored_wallets -> wallet_cohorts` | Да | Соответствует |

Что нужно исправить:

- В API добавить отдельный блок/endpoint для `SCORED top-50 ORDER BY sharpe DESC`.
- В React добавить секцию/таблицу `EXIT EXAMPLES`, которая читает `data.exit_examples.examples`.
- В React или API привести поля когорт к терминологии `cohort`, даже если внутри пока используется `status`.
- Показывать в UI метрики модели выхода: examples count, median time, MAE, R2, model type.

## Приоритет исправлений

P0 - влияет на смысл обучения:

- Переделать `cohorts.py` на пороги из ТЗ по `sharpe/win_rate/trade_count/max_drawdown/avg_hold_time`.
- Переделать генерацию exit examples на строгие пары `BUY -> SELL`.
- Добавить train/test split, `MAE`, `R2`, dummy fallback при плохом `R2`.
- Исправить `exit_examples.json` под ожидаемые поля и отобразить таблицу в React.

P1 - влияет на качество метрик:

- Пересчитать `consistency` по месяцам с положительным PnL.
- Уточнить `win_rate/profit_factor` как realized pair metrics, а не side proxy.
- Пересчитать `avg_hold_time` только по закрытым парам.
- Привести schema compatibility: `user_address/cohort` vs `wallet/status`.

P2 - техническая совместимость:

- Заменить `pickle` на `joblib` для `exit_model.pkl`.
- Сделать `SCORED` top-50 по Sharpe, а не top-10 по score/volume.
- Добавить в `best_policy.json` поддержку новых порогов.

## Итог

На текущих данных кнопка `Train Model` действительно запускает пересчет и может заполнять `scored_wallets`, `wallet_cohorts` и файлы модели выхода. Но если ориентироваться строго на описанное ТЗ, текущий результат нельзя считать полноценным автономным обучением: скоринг и когорты дают полезный рабочий сигнал, но модель выхода и политика когорт требуют доработки перед тем, как считать их надежной ML-логикой для копирования крупных кошельков.
