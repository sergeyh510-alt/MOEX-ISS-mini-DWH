# MOEX ISS mini-DWH

**Дневные котировки с Московской биржи: от сырых ответов API до готовых к запросам аналитических витрин.**

[![Apache Airflow 2.7.0](https://img.shields.io/badge/Airflow-2.7.0-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Python 3.10](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

🇷🇺 **Русский (текущий)** · 🇬🇧 [English](README.md)

## Что это и зачем

MOEX ISS отдаёт каждую сделку Московской биржи через бесплатный JSON API -
небольшой и хорошо знакомый набор данных, который удобно провести через
хранилище целиком. Проект забирает дневные котировки из этого API и превращает
их в то, по чему действительно можно делать запросы: чистые дневные цены,
звезду «измерения + факты» и витрины по доходности, волатильности и сравнению
с индексом.

Стек намеренно небольшой: пайплайн оркеструет **Apache Airflow 2.7.0**, данные
хранит **PostgreSQL 15** (в нём же и метаданные Airflow), весь стек поднимается
одной командой через **Docker Compose**, а ETL-код - обычный **Python 3.10**.

Проект закрывает типовые задачи инженера данных:

- загрузка из внешнего API с ретраями и постраничной пагинацией;
- многослойное хранилище `raw -> stg -> core -> mart` с разной степенью
  очистки данных на каждом слое;
- идемпотентность: повторный запуск за тот же период не создаёт дублей;
- звезда из измерений и фактов с суррогатными ключами;
- аналитические витрины (доходность, волатильность, сравнение с индексом);
- проверки качества данных (DQ) и отчёт о свежести;
- оркестрация задачами Airflow, запуск всего стека одной командой.

## Демо

Где бумага сильнее всего обогнала индекс IMOEX (из `mart.index_compare`):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, secid, stock_return_pct, index_return_pct, excess_return_pct
     from mart.index_compare
    order by excess_return_pct desc nulls last limit 3;"
```

```
 date_key | secid | stock_return_pct | index_return_pct | excess_return_pct
----------+-------+------------------+------------------+-------------------
 20240118 | GAZP  |           1.5118 |          -0.2003 |            1.7121
 20240123 | GAZP  |           1.3549 |           0.1132 |            1.2417
 20240117 | LKOH  |           0.7729 |           0.1818 |            0.5911
(3 rows)
```

## Стек

- **Apache Airflow 2.7.0** (`apache/airflow:2.7.0-python3.10`, LocalExecutor) - оркестрация;
- **PostgreSQL 15** - две БД: метаданные Airflow и мини-DWH;
- **Docker Compose** - воспроизводимый локальный стек;
- **Python 3.10** - клиент ISS, ETL-код и CLI-скрипты слоёв.

## Архитектура

```
MOEX ISS API (iss.moex.com)
        |
        v
  raw  -- сырые ответы как есть (ключевые колонки + JSONB со всей строкой)
        |
        v
  stg  -- типизация, проверки качества (is_valid + reject_reason), справочник
        |
        v
  core -- звезда: dim_security, dim_date, fact_daily_prices, fact_daily_index
        |
        v
  mart -- витрины: stock_returns, index_compare, daily_movers
        |
        v
 audit -- проверки качества (dq_run, dq_check); свежесть видна в meta.freshness
```

| Схема   | Назначение |
|---------|------------|
| `raw`   | сырые ответы MOEX ISS API, как пришли (ключевые колонки + `row_json`) |
| `stg`   | типизированные данные, проверки качества, справочник бумаг |
| `core`  | звезда: измерения `dim_*` и факты `fact_*` с суррогатными ключами |
| `mart`  | витрины: доходность бумаг, сравнение с индексом, лидеры дня |
| `meta`  | реестр источников, журнал запусков `load_run`, представление `freshness` |
| `audit` | проверки качества данных: запуски `dq_run` и проверки `dq_check` |

Поток данных: `MOEX ISS API -> raw -> stg -> core -> mart`, затем `audit`
(задача `run_dq`).

Таблицы по слоям:

- `raw.moex_history`, `raw.moex_index_history` - сырые дневные котировки и
  значения индекса, ключ `(boardid, secid, tradedate)` / `(index_code, tradedate)`;
- `stg.stock_history`, `stg.index_history` - те же данные после типизации, с
  флагами `is_valid`/`reject_reason`; `stg.securities` - справочник бумаг (SCD1);
- `core.dim_security` (ключ `security_key`, IDENTITY), `core.dim_date`
  (ключ `date_key` = `YYYYMMDD`), `core.fact_daily_prices` (ключ
  `security_key, date_key`, расчёт `prev_close`/`close_change`/`close_change_pct`),
  `core.fact_daily_index` (ключ `index_code, date_key`);
- `mart.stock_returns` (`daily_return_pct`, `gap_pct`, `amplitude_pct`, `ma_5`,
  `ma_20`, `volatility_5_pct`, `volatility_20_pct`, `cumulative_return_pct`),
  `mart.index_compare` (`stock/index/excess_return_pct`), `mart.daily_movers`
  (`return_rank`, `is_best_of_day`, `is_worst_of_day`);
- `meta.source`, `meta.load_run`, представление `meta.freshness`;
- `audit.dq_run`, `audit.dq_check`.

Соглашения по метрикам (одинаковы в коде и тестах):

- скользящие метрики считаются только при полном окне наблюдений, иначе `NULL`;
- волатильность - выборочное стандартное отклонение (`n - 1`) дневных
  доходностей окна, в процентах;
- накопленная доходность считается от первой доступной цены закрытия бумаги
  (первый день даёт `0`);
- «нет данных» и «нулевое изменение» - разные вещи: при нехватке данных или
  нулевом знаменателе метрика равна `NULL`, а не `0`.

## Что сейчас в хранилище

Проверено на реальной истории за **2024-01-01 .. 2026-10-10** (бумаги `SBER`,
`GAZP`, `LKOH`, борда `TQBR`, индекс `IMOEX`).

| Слой  | Таблица | Строк |
|-------|---------|-------|
| raw   | `moex_history` | 2118 |
| raw   | `moex_index_history` | 706 |
| stg   | `stock_history` / `index_history` / `securities` | 2118 / 706 / 3 |
| core  | `dim_security` / `dim_date` | 3 / 999 |
| core  | `fact_daily_prices` / `fact_daily_index` | 2118 / 706 |
| mart  | `stock_returns` / `index_compare` / `daily_movers` | 2118 / 2115 / 2115 |
| audit | `dq_run` / `dq_check` | проверки: 29, статус `passed` |

`dim_date` - календарь за период построения core (999 дней, включая выходные);
`ma_20` и `volatility_20_pct` заполнены в 2061 строке `mart.stock_returns`
(нужно 20 предыдущих торговых дней). В `index_compare` и `daily_movers` на 3
строки меньше, чем в `stock_returns`: первый торговый день периода не имеет
предыдущей цены, поэтому у него нет ни дневной доходности, ни сравнения.

Средние метрики по бумагам за весь период (по `mart.stock_returns`):

| Бумага | Средняя дневная доходность | СКО дневной доходности |
|--------|---------------------------:|-----------------------:|
| GAZP   | -0.043% | 2.124 |
| LKOH   | +0.001% | 1.809 |
| SBER   | +0.015% | 1.393 |

## Быстрый старт

Требуется Docker Desktop (Docker Compose v2). Из чистого клона:

```bash
copy .env.example .env     # Windows PowerShell; в bash: cp .env.example .env
docker compose up -d
```

Первый запуск собирает образ Airflow (может занять несколько минут). Дальше:

- Airflow UI: http://localhost:8081 - логин и пароль берутся из `.env`
  (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`; в `.env.example` по умолчанию
  `admin` / `admin`);
- БД мини-DWH: `localhost:5433`, база `moex_dwh`, пользователь `dwh`
  (порт `DWH_PG_HOST_PORT`);
- проверить, что сервисы живы: `docker compose ps` (у `airflow-webserver` и
  `airflow-scheduler` статус `healthy`).

> Порты, UI и админ задаются в `.env`. Если 8080/5434 заняты другим стеком,
> поменяйте `AIRFLOW_WEB_PORT` / `AIRFLOW_PG_HOST_PORT` (в этом проекте локально
> используются 8081 и 5436). Файл `.env` в репозиторий не попадает.

Остановить стек:

```bash
docker compose down        # данные сохраняются в томах
docker compose down -v     # удаляет также тома с данными БД и логи
```

## Загрузка истории

Историю удобнее заливать скриптами слоёв, а не backfill'ом DAG по одному дню:
скрипт принимает весь период сразу и делает один проход (несколько
HTTP-страниц ISS + пачки `UPSERT`), тогда как backfill создаёт отдельный
DAG-run на каждый день - это сотни задач Airflow с накладными расходами. Оба
пути идемпотентны, результат один и тот же.

Порядок слоёв важен: каждый следующий читает предыдущий. Запускать из
контейнера `airflow-scheduler` (там есть `psycopg2` и доступ к DWH).

```bash
# 1. raw: загрузка котировок и индекса из MOEX ISS
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/load_raw.py \
    --from 2024-01-01 --to 2026-10-10 \
    --secids SBER,GAZP,LKOH --board TQBR --index IMOEX

# 2. stg: типизация, проверки качества, справочник бумаг
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/build_stg.py --from 2024-01-01 --to 2026-10-10

# 3. core: звезда (измерения и факты)
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/build_core.py --from 2024-01-01 --to 2026-10-10

# 4. mart: витрины
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/build_mart.py --from 2024-01-01 --to 2026-10-10

# 5. audit: проверки качества
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/run_dq.py --from 2024-01-01 --to 2026-10-10
```

> Период берите с запасом в 1-2 года: `ma_20` и `volatility_20_pct` заполняются
> только при наличии 20 предыдущих торговых дней. `build_core` строит
> `dim_date` за календарный период `--from/--to`, а `build_mart` считает
> скользящие метрики по всей истории `core`, но пишет строки только за период -
> поэтому `--from/--to` должен покрывать период фактов.

## Пайплайн (DAG) и ручной запуск

DAG `moex_dwh_daily` (ежедневно, слой за слоем, одна задача на слой):

1. `load_raw`   - загрузка котировок из MOEX ISS в `raw` (`scripts/load_raw.py`);
2. `build_stg`  - типизация и проверки `raw -> stg` (`scripts/build_stg.py`);
3. `build_core` - звезда `stg -> core` (`scripts/build_core.py`);
4. `build_mart` - витрины `core -> mart` (`scripts/build_mart.py`);
5. `run_dq`     - проверки качества данных (`scripts/run_dq.py`). Падение
   проверки останавливает DAG (см. `DECISIONS.md`).

DAG рассчитан на один день (`{{ ds }}`). Снять с паузы и запустить целиком:

```bash
docker compose exec airflow-scheduler airflow dags unpause moex_dwh_daily
docker compose exec airflow-scheduler airflow dags trigger moex_dwh_daily
```

Посмотреть состояние задач и убедиться, что все пять прошли успешно:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d moex_dwh_daily
```

Граф задач виден в UI: http://localhost:8081/dags/moex_dwh_daily/grid.

Чтобы прогнать цепочку за произвольный период, либо вызовите скрипты слоёв
вручную (раздел «Загрузка истории»), либо запустите DAG бэкфиллом (по DAG-run
на каждый день):

```bash
docker compose exec airflow-scheduler airflow dags backfill moex_dwh_daily \
    --start-date 2024-01-09 --end-date 2024-01-23
```

## Примеры запросов к витринам

Доходность и метрики одной бумаги:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, close, daily_return_pct, ma_5, ma_20,
          volatility_5_pct, volatility_20_pct, cumulative_return_pct
     from mart.stock_returns where secid = 'SBER' order by date_key desc limit 10;"
```

Средняя дневная доходность и СКО по бумагам за весь период (даёт числа GAZP
-0.043 / 2.124, LKOH +0.001 / 1.809, SBER +0.015 / 1.393 из таблицы выше):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select secid,
          round(avg(daily_return_pct), 3)         as avg_daily_return_pct,
          round(stddev_samp(daily_return_pct), 3) as stddev_daily_return_pct,
          count(*) as days
     from mart.stock_returns
    where daily_return_pct is not null
    group by secid order by secid;"
```

Лучшие дни по доходности (сортировка по метрике):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, secid, round(daily_return_pct, 2) as ret_pct
     from mart.stock_returns
    where daily_return_pct is not null
    order by daily_return_pct desc limit 10;"
```

Где бумага обогнала индекс IMOEX:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, secid, round(stock_return_pct, 2) as stock,
          round(index_return_pct, 2) as index, round(excess_return_pct, 2) as excess
     from mart.index_compare
    order by excess_return_pct desc nulls last limit 10;"
```

Самые волатильные бумаги периода (там, где окно 20 дней уже есть):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select secid, count(*) as days,
          round(min(volatility_20_pct), 3) as min_vol,
          round(max(volatility_20_pct), 3) as max_vol,
          round(avg(volatility_20_pct), 3) as avg_vol
     from mart.stock_returns
    where volatility_20_pct is not null
    group by secid order by avg_vol desc;"
```

Лучшая и худшая бумага каждого торгового дня:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key,
          string_agg(secid, ', ') filter (where is_best_of_day)  as best,
          string_agg(secid, ', ') filter (where is_worst_of_day) as worst
     from mart.daily_movers group by date_key order by date_key desc limit 10;"
```

Больше запросов (окна, джойны со звездой, накопленная доходность) - в
`docs/queries.md`.

## Проверки качества (DQ) и свежесть

```bash
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/run_dq.py --from 2024-01-01 --to 2026-10-10
```

Скрипт печатает отчёт и пишет результаты в `audit.dq_run` / `audit.dq_check`.
Если упала хотя бы одна проверка, скрипт возвращает код 1 (в DAG это падение
задачи `run_dq`). Выполняется 29 проверок: дубли натуральных ключей,
незаполненные обязательные поля, неположительные цены, отбракованные строки
stg и причина отбраковки, `prev_close` только в первый день бумаги,
согласованность цепочки `raw -> stg -> core -> mart`, свежесть слоёв.

Отчёт DQ:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select status, count(*) from audit.dq_check group by status;"
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select layer, check_name, status, issues_count, max_allowed
     from audit.dq_check order by dq_check_id;"
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select dq_run_id, date_from, date_to, status, checks_total,
          checks_passed, checks_failed, finished_at
     from audit.dq_run order by dq_run_id desc limit 5;"
```

Свежесть и последний успешный прогон по слоям:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select * from meta.freshness order by target_table;"
```

Журнал загрузок (кто, когда, сколько строк, сколько отбраковано):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select target_table, status, rows_loaded, rows_rejected, started_at, finished_at
     from meta.load_run order by started_at desc limit 10;"
```

Порог свежести по умолчанию - 720 часов (30 суток); переопределяется
переменной `DQ_FRESHNESS_MAX_HOURS`.

## Тесты

Оффлайн-юнит-тесты (не требуют сети и БД): клиент ISS, разбор строк, правила
stg, правила core, метрики mart, правила DQ.

```bash
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
```

Необязательная проверка доступности MOEX ISS (не входит в пайплайн и не
блокирует запуск):

```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/check_moex.py
```

## Структура репозитория

```
dags/       DAG'и Airflow (moex_dwh_daily)
include/    ETL-код: config, db, sql_init, batching, parsing, клиент ISS,
            загрузчик raw, правила и билдеры stg/core/mart, правила и раннер DQ
scripts/    точки входа для ручного запуска (load_raw, build_stg, build_core,
            build_mart, run_dq, check_moex)
sql/        DDL слоёв (00_schemas, 10_raw, 20_stg, 30_core, 40_mart, 50_meta,
            60_audit)
docs/       описание эндпоинтов MOEX ISS и примеры запросов
tests/      оффлайн-тесты (pytest)
Dockerfile                 образ Airflow
requirements-airflow.txt   Python-зависимости образа
docker-compose.yml         локальный стек сервисов
.env.example               образец переменных окружения (сам .env не в git)
DECISIONS.md               журнал технических решений
```

## Что не сделано / следующие шаги

Честный список того, чего в проекте пока нет:

- **объёмы и капитализация** (`VALUE`, `VOLUME` лежат в `raw`/`stg`, но в
  витрины не вынесены) - витрины пока про цены и доходность;
- **больше бумаг и борд**: сейчас три бумаги на борде `TQBR` и один индекс;
  справочник бумаг строится из истории торгов, отдельного эндпоинта нет;
- **BI-слой**: витрины смотрятся через `psql`, дашбордов (Grafana / Metabase)
  поверх `mart` нет;
- **CI**: автотесты и линт не гоняются автоматически, только вручную;
- **инкрементальность**: DAG ежедневный, отдельного механизма «догрузить
  только новые дни» без перезапуска за период нет;
- **ретраи DQ и алерты**: о падении проверки узнаём по статусу задачи Airflow,
  внешних уведомлений (Slack/почта) нет.

## Данные

Источник: ISS API Московской биржи (https://iss.moex.com). Используемые
эндпоинты и формат ответа описаны в `docs/moex_endpoints.md`.