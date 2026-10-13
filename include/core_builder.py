"""Построение слоя core из stg (звезда: измерения + факты).

Порядок построения важен:
  1. dim_security - измерение бумаг (нужны суррогатные ключи для факта);
  2. dim_date     - календарь дат за период (нужны ключи дат для факта);
  3. fact_daily_prices / fact_daily_index - факты, ссылающиеся на измерения.

Идемпотентность: измерения обновляются upsert'ом по естественному ключу
(суррогатный ключ выдаётся один раз и не меняется), факты - upsert'ом по
составному ключу ``(security_key, date_key)`` / ``(index_code, date_key)``.
Повторное построение за тот же период не создаёт дублей и не двигает ключи.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from include import db
from include.batching import (
    build_upsert_query,
    count_returned,
    deduplicate_rows,
    key_indexes,
)
from include.core_rules import (
    DIM_DATE_COLUMNS,
    DIM_SECURITY_COLUMNS,
    FACT_INDEX_COLUMNS,
    FACT_PRICE_COLUMNS,
    build_date_dim_rows,
    compute_close_change,
    deduplicate_fact_rows,
    map_security_keys,
    to_date_key,
)

DEFAULT_SOURCE_CODE = "moex_iss"
FACT_PRICE_KEY: tuple[str, ...] = ("security_key", "date_key")
# Выражение «в этот календарный день были торги»: используется в двух местах
# при пересчёте признака is_trading_day, поэтому вынесено в константу.
IS_TRADING_DAY_EXPR: str = """
    EXISTS (
        SELECT 1 FROM stg.stock_history s
         WHERE s.tradedate = d.calendar_date AND s.is_valid
    )
    OR EXISTS (
        SELECT 1 FROM stg.index_history i
         WHERE i.tradedate = d.calendar_date AND i.is_valid
    )
"""

FACT_INDEX_KEY: tuple[str, ...] = ("index_code", "date_key")

FACT_PRICE_UPDATE_COLUMNS: tuple[str, ...] = (
    "open",
    "low",
    "high",
    "close",
    "waprice",
    "value",
    "volume",
    "numtrades",
    "prev_close",
    "close_change",
    "close_change_pct",
)
FACT_INDEX_UPDATE_COLUMNS: tuple[str, ...] = (
    "open",
    "low",
    "high",
    "close",
    "value",
    "prev_close",
    "close_change",
    "close_change_pct",
)


@dataclass
class CoreResult:
    """Итог построения одной таблицы core."""

    target_table: str
    rows_read: int
    rows_written: int


def _to_tuple(fields: Mapping[str, Any], columns: Sequence[str]) -> tuple[Any, ...]:
    """Разложить словарь полей в кортеж значений по порядку колонок."""
    values: list[Any] = []
    for column in columns:
        if column == "source_code":
            values.append(DEFAULT_SOURCE_CODE)
        else:
            values.append(fields.get(column))
    return tuple(values)


def _upsert(
    conn: PgConnection,
    table: str,
    columns: Sequence[str],
    key: Sequence[str],
    update_columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    batch_size: int = 500,
) -> int:
    """Вставить/обновить строки и вернуть число затронутых записей."""
    if not rows:
        return 0
    unique_rows = deduplicate_rows(rows, key_indexes(columns, key))
    query = build_upsert_query(table, columns, key, update_columns)
    written = 0
    for offset in range(0, len(unique_rows), batch_size):
        page = unique_rows[offset : offset + batch_size]
        with conn.cursor() as cur:
            result = execute_values(cur, query, page, page_size=batch_size, fetch=True)
            written += count_returned(result)
    return written


def build_dim_security(conn: PgConnection) -> int:
    """Построить ``core.dim_security`` из stg (SCD1, upsert по естественному ключу).

    Суррогатный ключ ``security_key`` выдаётся Postgres один раз при первой
    вставке и сохраняется при последующих обновлениях, поэтому ссылки фактов
    на измерение стабильны.

    Args:
        conn: соединение с БД мини-DWH.

    Returns:
        Число вставленных и обновлённых строк измерения.
    """
    aggregate_rows = db.fetch_all(
        conn,
        """
        SELECT secid, boardid,
               min(shortname) AS shortname,
               min(tradedate) AS first_tradedate,
               max(tradedate) AS last_tradedate
          FROM stg.stock_history
         WHERE is_valid
         GROUP BY secid, boardid
        """,
    )
    prepared = [
        _to_tuple(dict(zip(DIM_SECURITY_COLUMNS, row)), DIM_SECURITY_COLUMNS)
        for row in aggregate_rows
    ]
    return _upsert(
        conn,
        "core.dim_security",
        DIM_SECURITY_COLUMNS,
        ("secid", "boardid"),
        ("shortname", "first_tradedate", "last_tradedate"),
        prepared,
    )


def build_dim_date(conn: PgConnection, date_from: str, date_to: str) -> int:
    """Построить ``core.dim_date`` за календарный период и обновить признак торгов.

    Календарные строки вставляются один раз (``ON CONFLICT DO NOTHING``), затем
    признак ``is_trading_day`` пересчитывается по данным stg за период. Guard
    ``IS DISTINCT FROM`` не даёт повторному запуску бессмысленно менять строки.

    Args:
        conn: соединение с БД мини-DWH.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.

    Returns:
        Число вставленных календарных строк (без учёта уже существовавших).
    """
    dim_rows = build_date_dim_rows(
        date.fromisoformat(date_from), date.fromisoformat(date_to)
    )
    prepared = [_to_tuple(row, DIM_DATE_COLUMNS) for row in dim_rows]
    query = (
        f"INSERT INTO core.dim_date ({', '.join(DIM_DATE_COLUMNS)}) VALUES %s "
        "ON CONFLICT (date_key) DO NOTHING RETURNING 1"
    )
    inserted = 0
    with conn.cursor() as cur:
        result = execute_values(cur, query, prepared, page_size=500, fetch=True)
        inserted = count_returned(result)

    update_query = (
        "UPDATE core.dim_date AS d "
        f"SET is_trading_day = ({IS_TRADING_DAY_EXPR}), updated_at = now() "
        "WHERE d.calendar_date BETWEEN %s AND %s "
        "AND d.is_trading_day IS DISTINCT FROM (" + IS_TRADING_DAY_EXPR + ")"
    )
    with conn.cursor() as cur:
        cur.execute(update_query, (date_from, date_to))
    return inserted


def _security_key_map(conn: PgConnection) -> dict[tuple[str, str], int]:
    """Вернуть отображение ``(secid, boardid) -> security_key``."""
    rows = db.fetch_all(conn, "SELECT secid, boardid, security_key FROM core.dim_security")
    return {(str(row[0]), str(row[1])): int(row[2]) for row in rows}


def build_fact_daily_prices(
    conn: PgConnection, date_from: str, date_to: str, batch_size: int = 500
) -> CoreResult:
    """Построить ``core.fact_daily_prices`` из stg за период (идемпотентно).

    Берутся только валидные строки stg. Изменение цены считается к закрытию
    предыдущего торгового дня (``LAG`` по всей истории stg, поэтому первая дата
    периода тоже получает корректный ``prev_close``).

    Args:
        conn: соединение с БД мини-DWH.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        batch_size: размер пачки вставки.

    Returns:
        Итог: сколько строк прочитано и записано.
    """
    source_rows = db.fetch_all(
        conn,
        """
        WITH src AS (
            SELECT secid, boardid, tradedate, open, low, high, close,
                   waprice, value, volume, numtrades,
                   LAG(close) OVER (
                       PARTITION BY secid, boardid ORDER BY tradedate
                   ) AS prev_close
              FROM stg.stock_history
             WHERE is_valid
        )
        SELECT secid, boardid, tradedate, open, low, high, close,
               waprice, value, volume, numtrades, prev_close
          FROM src
         WHERE tradedate BETWEEN %s AND %s
         ORDER BY secid, boardid, tradedate
        """,
        (date_from, date_to),
    )
    source_columns = (
        "secid",
        "boardid",
        "tradedate",
        "open",
        "low",
        "high",
        "close",
        "waprice",
        "value",
        "volume",
        "numtrades",
        "prev_close",
    )
    records = [dict(zip(source_columns, row)) for row in source_rows]

    key_map = _security_key_map(conn)
    mapped = map_security_keys(records, key_map)

    prepared: list[dict[str, Any]] = []
    for record in mapped:
        change, change_pct = compute_close_change(
            record["close"], record["prev_close"]
        )
        prepared.append(
            {
                "security_key": record["security_key"],
                "date_key": to_date_key(record["tradedate"]),
                "open": record["open"],
                "low": record["low"],
                "high": record["high"],
                "close": record["close"],
                "waprice": record["waprice"],
                "value": record["value"],
                "volume": record["volume"],
                "numtrades": record["numtrades"],
                "prev_close": record["prev_close"],
                "close_change": change,
                "close_change_pct": change_pct,
            }
        )

    unique = deduplicate_fact_rows(prepared, FACT_PRICE_KEY)
    rows = [_to_tuple(row, FACT_PRICE_COLUMNS) for row in unique]
    written = _upsert(
        conn,
        "core.fact_daily_prices",
        FACT_PRICE_COLUMNS,
        FACT_PRICE_KEY,
        FACT_PRICE_UPDATE_COLUMNS,
        rows,
        batch_size,
    )
    return CoreResult("core.fact_daily_prices", len(source_rows), written)


def build_fact_daily_index(
    conn: PgConnection, date_from: str, date_to: str, batch_size: int = 500
) -> CoreResult:
    """Построить ``core.fact_daily_index`` из stg за период (идемпотентно).

    Args:
        conn: соединение с БД мини-DWH.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        batch_size: размер пачки вставки.

    Returns:
        Итог: сколько строк прочитано и записано.
    """
    source_rows = db.fetch_all(
        conn,
        """
        WITH src AS (
            SELECT index_code, tradedate, open, low, high, close, value,
                   LAG(close) OVER (
                       PARTITION BY index_code ORDER BY tradedate
                   ) AS prev_close
              FROM stg.index_history
             WHERE is_valid
        )
        SELECT index_code, tradedate, open, low, high, close, value, prev_close
          FROM src
         WHERE tradedate BETWEEN %s AND %s
         ORDER BY index_code, tradedate
        """,
        (date_from, date_to),
    )
    source_columns = (
        "index_code",
        "tradedate",
        "open",
        "low",
        "high",
        "close",
        "value",
        "prev_close",
    )
    prepared: list[dict[str, Any]] = []
    for row in source_rows:
        record = dict(zip(source_columns, row))
        change, change_pct = compute_close_change(
            record["close"], record["prev_close"]
        )
        prepared.append(
            {
                "index_code": record["index_code"],
                "date_key": to_date_key(record["tradedate"]),
                "open": record["open"],
                "low": record["low"],
                "high": record["high"],
                "close": record["close"],
                "value": record["value"],
                "prev_close": record["prev_close"],
                "close_change": change,
                "close_change_pct": change_pct,
            }
        )

    unique = deduplicate_fact_rows(prepared, FACT_INDEX_KEY)
    rows = [_to_tuple(row, FACT_INDEX_COLUMNS) for row in unique]
    written = _upsert(
        conn,
        "core.fact_daily_index",
        FACT_INDEX_COLUMNS,
        FACT_INDEX_KEY,
        FACT_INDEX_UPDATE_COLUMNS,
        rows,
        batch_size,
    )
    return CoreResult("core.fact_daily_index", len(source_rows), written)


__all__ = [
    "CoreResult",
    "FACT_INDEX_KEY",
    "FACT_PRICE_KEY",
    "build_dim_date",
    "build_dim_security",
    "build_fact_daily_index",
    "build_fact_daily_prices",
]