# MOEX ISS mini-DWH

**Русская версия: [README_ru.md](README_ru.md)**

A Data Engineer portfolio project: a small data warehouse (DWH) built on open
data from the Moscow Exchange (MOEX ISS API). It shows the full data path from
a raw API response to analytics marts on a small but honest stack:
Airflow + PostgreSQL + Docker Compose + Python.

## What it is and why

Daily exchange quotes are a convenient learning domain: the data is open, the
format is known, and the metrics (return, volatility, comparison with an index)
need no financial background to understand.

The project covers typical data-engineering tasks:

- loading from an external API with retries and paging;
- a layered warehouse `raw -> stg -> core -> mart` with a different level of
  data cleaning on each layer;
- idempotency: re-running for the same period never creates duplicates;
- a star schema of dimensions and facts with surrogate keys;
- analytics marts (return, volatility, comparison with an index);
- data-quality (DQ) checks and a freshness report;
- orchestration with Airflow tasks and a one-command local stack.

## Tech stack

- **Apache Airflow 2.7.0** (`apache/airflow:2.7.0-python3.10`, LocalExecutor) - orchestration;
- **PostgreSQL 15** - two databases: Airflow metadata and the mini-DWH;
- **Docker Compose** - a reproducible local stack;
- **Python 3.10** - the ISS client, the ETL code and the per-layer CLI scripts.

## Architecture

```
MOEX ISS API (iss.moex.com)
        |
        v
  raw  -- raw responses as received (key columns + JSONB with the whole row)
        |
        v
  stg  -- typing, quality checks (is_valid + reject_reason), securities catalog
        |
        v
  core -- star: dim_security, dim_date, fact_daily_prices, fact_daily_index
        |
        v
  mart -- marts: stock_returns, index_compare, daily_movers
        |
        v
 audit -- data-quality checks (dq_run, dq_check); freshness in meta.freshness
```

| Schema  | Purpose |
|---------|---------|
| `raw`   | raw MOEX ISS API responses as received (key columns + `row_json`) |
| `stg`   | typed data, quality checks, securities catalog |
| `core`  | star: `dim_*` dimensions and `fact_*` facts with surrogate keys |
| `mart`  | marts: stock returns, comparison with the index, daily movers |
| `meta`  | source registry, `load_run` journal, `freshness` view |
| `audit` | data-quality checks: `dq_run` runs and `dq_check` checks |

Data flow: `MOEX ISS API -> raw -> stg -> core -> mart`, then `audit`
(the `run_dq` task).

Tables by layer:

- `raw.moex_history`, `raw.moex_index_history` - raw daily quotes and index
  values, key `(boardid, secid, tradedate)` / `(index_code, tradedate)`;
- `stg.stock_history`, `stg.index_history` - the same data after typing, with
  `is_valid`/`reject_reason` flags; `stg.securities` - securities catalog (SCD1);
- `core.dim_security` (key `security_key`, IDENTITY), `core.dim_date`
  (key `date_key` = `YYYYMMDD`), `core.fact_daily_prices` (key
  `security_key, date_key`, computes `prev_close`/`close_change`/`close_change_pct`),
  `core.fact_daily_index` (key `index_code, date_key`);
- `mart.stock_returns` (`daily_return_pct`, `gap_pct`, `amplitude_pct`, `ma_5`,
  `ma_20`, `volatility_5_pct`, `volatility_20_pct`, `cumulative_return_pct`),
  `mart.index_compare` (`stock/index/excess_return_pct`), `mart.daily_movers`
  (`return_rank`, `is_best_of_day`, `is_worst_of_day`);
- `meta.source`, `meta.load_run`, the `meta.freshness` view;
- `audit.dq_run`, `audit.dq_check`.

Metric conventions (the same in code and tests):

- rolling metrics are computed only with a full observation window, otherwise `NULL`;
- volatility is the sample standard deviation (`n - 1`) of daily returns inside
  the window, in percent;
- cumulative return is measured from the first available close of a stock
  (the first day is `0`);
- "no data" and "zero change" are different things: when data is missing or the
  denominator is zero the metric is `NULL`, not `0`.

## What is in the warehouse today

Verified on real history for **2024-01-01 .. 2026-10-10** (stocks `SBER`,
`GAZP`, `LKOH`, board `TQBR`, index `IMOEX`).

| Layer | Table | Rows |
|-------|-------|------|
| raw   | `moex_history` | 2118 |
| raw   | `moex_index_history` | 706 |
| stg   | `stock_history` / `index_history` / `securities` | 2118 / 706 / 3 |
| core  | `dim_security` / `dim_date` | 3 / 999 |
| core  | `fact_daily_prices` / `fact_daily_index` | 2118 / 706 |
| mart  | `stock_returns` / `index_compare` / `daily_movers` | 2118 / 2115 / 2115 |
| audit | `dq_run` / `dq_check` | checks: 29, status `passed` |

`dim_date` is a calendar for the period core was built for (999 days, weekends
included); `ma_20` and `volatility_20_pct` are filled in 2061 rows of
`mart.stock_returns` (20 previous trading days are required). `index_compare`
and `daily_movers` have 3 rows fewer than `stock_returns`: the first trading day
of the period has no previous price, so it has neither a daily return nor a
comparison.

Average metrics per stock over the whole period (from `mart.stock_returns`):

| Stock | Avg daily return | Std dev of daily return |
|-------|-----------------:|------------------------:|
| GAZP  | -0.043% | 2.124 |
| LKOH  | +0.001% | 1.809 |
| SBER  | +0.015% | 1.393 |

## Quick start

Docker Desktop (Docker Compose v2) is required. From a clean clone:

```bash
copy .env.example .env     # Windows PowerShell; in bash: cp .env.example .env
docker compose up -d
```

The first run builds the Airflow image (may take a few minutes). Then:

- Airflow UI: http://localhost:8081 - the login and password come from `.env`
  (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`; `.env.example` defaults to
  `admin` / `admin`);
- mini-DWH: `localhost:5433`, database `moex_dwh`, user `dwh`
  (host port `DWH_PG_HOST_PORT`);
- make sure the services are alive: `docker compose ps` (`airflow-webserver`
  and `airflow-scheduler` should be `healthy`).

> Ports, UI and admin come from `.env`. If 8080/5434 are taken by another stack,
> change `AIRFLOW_WEB_PORT` / `AIRFLOW_PG_HOST_PORT` (this project uses 8081 and
> 5436 locally). The `.env` file is never committed.

Stop the stack:

```bash
docker compose down        # keeps data in volumes
docker compose down -v     # also removes DB volumes and logs
```

## Loading history

It is better to load history with the per-layer scripts than with a per-day DAG
backfill: a script takes the whole period at once and makes a single pass (a few
ISS HTTP pages + batches of `UPSERT`), while a backfill creates a separate
DAG run per day - that is hundreds of Airflow tasks with overhead. Both paths are
idempotent and produce the same result.

The layer order matters: each one reads the previous. Run them from the
`airflow-scheduler` container (it has `psycopg2` and access to the DWH).

```bash
# 1. raw: load quotes and the index from MOEX ISS
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/load_raw.py \
    --from 2024-01-01 --to 2026-10-10 \
    --secids SBER,GAZP,LKOH --board TQBR --index IMOEX

# 2. stg: typing, quality checks, securities catalog
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/build_stg.py --from 2024-01-01 --to 2026-10-10

# 3. core: the star (dimensions and facts)
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/build_core.py --from 2024-01-01 --to 2026-10-10

# 4. mart: marts
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/build_mart.py --from 2024-01-01 --to 2026-10-10

# 5. audit: data-quality checks
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/run_dq.py --from 2024-01-01 --to 2026-10-10
```

> Pick a period with 1-2 years of slack: `ma_20` and `volatility_20_pct` are
> filled only when 20 previous trading days exist. `build_core` builds `dim_date`
> for the calendar period `--from/--to`, while `build_mart` computes rolling
> metrics over the whole history of `core` but writes rows only for the period -
> so `--from/--to` must cover the period of the facts.

## Pipeline (DAG) and manual runs

The `moex_dwh_daily` DAG (daily, layer by layer, one task per layer):

1. `load_raw`   - load quotes from MOEX ISS into `raw` (`scripts/load_raw.py`);
2. `build_stg`  - typing and checks `raw -> stg` (`scripts/build_stg.py`);
3. `build_core` - the star `stg -> core` (`scripts/build_core.py`);
4. `build_mart` - marts `core -> mart` (`scripts/build_mart.py`);
5. `run_dq`     - data-quality checks (`scripts/run_dq.py`). A failed check stops
   the DAG (see `DECISIONS.md`).

The DAG is designed for a single day (`{{ ds }}`). Unpause and trigger it:

```bash
docker compose exec airflow-scheduler airflow dags unpause moex_dwh_daily
docker compose exec airflow-scheduler airflow dags trigger moex_dwh_daily
```

Check the task states and confirm that all five succeeded:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d moex_dwh_daily
```

The task graph is also visible in the UI:
http://localhost:8081/dags/moex_dwh_daily/grid.

To run the chain for an arbitrary period, either call the layer scripts manually
(see "Loading history") or run the DAG as a backfill (one DAG run per day):

```bash
docker compose exec airflow-scheduler airflow dags backfill moex_dwh_daily \
    --start-date 2024-01-09 --end-date 2024-01-23
```

## SQL examples against the marts

Return and metrics of one stock:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, close, daily_return_pct, ma_5, ma_20,
          volatility_5_pct, volatility_20_pct, cumulative_return_pct
     from mart.stock_returns where secid = 'SBER' order by date_key desc limit 10;"
```

Average daily return and standard deviation per stock over the whole period
(this yields GAZP -0.043 / 2.124, LKOH +0.001 / 1.809, SBER +0.015 / 1.393 from
the table above):

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

Best days by return (sorted by the metric):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, secid, round(daily_return_pct, 2) as ret_pct
     from mart.stock_returns
    where daily_return_pct is not null
    order by daily_return_pct desc limit 10;"
```

Where a stock beat the IMOEX index:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key, secid, round(stock_return_pct, 2) as stock,
          round(index_return_pct, 2) as index, round(excess_return_pct, 2) as excess
     from mart.index_compare
    order by excess_return_pct desc nulls last limit 10;"
```

The most volatile stocks of the period (where the 20-day window already exists):

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

The best and the worst stock of each trading day:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select date_key,
          string_agg(secid, ', ') filter (where is_best_of_day)  as best,
          string_agg(secid, ', ') filter (where is_worst_of_day) as worst
     from mart.daily_movers group by date_key order by date_key desc limit 10;"
```

More queries (windows, joins with the star, cumulative return) are in
`docs/queries.md`.

## Data-quality checks (DQ) and freshness

```bash
docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/run_dq.py --from 2024-01-01 --to 2026-10-10
```

The script prints a report and writes results to `audit.dq_run` / `audit.dq_check`.
If at least one check fails, the script returns exit code 1 (in the DAG this is a
failed `run_dq` task). It runs 29 checks: duplicate natural keys, missing required
fields, non-positive prices, rejected stg rows and their reject reason,
`prev_close` only on the first day of a stock, consistency of the
`raw -> stg -> core -> mart` chain, and layer freshness.

DQ report:

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

Freshness and the last successful run per layer:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select * from meta.freshness order by target_table;"
```

Load journal (who, when, how many rows, how many rejected):

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c \
  "select target_table, status, rows_loaded, rows_rejected, started_at, finished_at
     from meta.load_run order by started_at desc limit 10;"
```

The freshness threshold defaults to 720 hours (30 days); it is overridden by the
`DQ_FRESHNESS_MAX_HOURS` variable.

## Tests

Offline unit tests (no network and no DB needed): the ISS client, row parsing,
the stg rules, the core rules, the mart metrics, the DQ rules.

```bash
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
```

An optional MOEX ISS availability check (not part of the pipeline and does not
block startup):

```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/check_moex.py
```

## Repository structure

```
dags/       Airflow DAGs (moex_dwh_daily)
include/    ETL code: config, db, sql_init, batching, parsing, ISS client,
            raw loader, stg/core/mart rules and builders, DQ rules and runner
scripts/    entry points for manual runs (load_raw, build_stg, build_core,
            build_mart, run_dq, check_moex)
sql/        layer DDL (00_schemas, 10_raw, 20_stg, 30_core, 40_mart, 50_meta,
            60_audit)
docs/       MOEX ISS endpoint notes and query examples
tests/      offline tests (pytest)
Dockerfile                 Airflow image
requirements-airflow.txt   Python dependencies of the image
docker-compose.yml         local service stack
.env.example               sample environment variables (the .env itself is not in git)
DECISIONS.md               engineering decision log
```

## What is not done / next steps

An honest list of what the project does not have yet:

- **volume and market cap** (`VALUE`, `VOLUME` live in `raw`/`stg` but are not
  exposed in the marts) - the marts are about prices and returns so far;
- **more stocks and boards**: currently three stocks on board `TQBR` and one
  index; the securities catalog is built from trade history, there is no
  dedicated endpoint;
- **BI layer**: the marts are read through `psql`; there are no dashboards
  (Grafana / Metabase) on top of `mart`;
- **CI**: tests and lint are not run automatically, only manually;
- **incrementality**: the DAG is daily; there is no mechanism to "load only new
  days" without rerunning a period;
- **DQ retries and alerts**: a failed check is noticed through the Airflow task
  status; there are no external notifications (Slack/email).

## Data

Source: Moscow Exchange ISS API (https://iss.moex.com). The endpoints used and
the response format are described in `docs/moex_endpoints.md`.