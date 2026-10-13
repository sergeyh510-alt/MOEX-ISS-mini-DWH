"""Построение слоя stg из raw (типизация, валидация, справочник бумаг).

Идемпотентность: вставка идёт через ``INSERT ... ON CONFLICT DO UPDATE`` по
натуральному ключу, поэтому повторное построение за тот же период не создаёт
дублей, а обновляет значения и ``updated_at``.

Читаем raw и пишем stg в одной транзакции; число строк возвращаем в виде
``TransformResult``, который CLI/DAG пишет в ``meta.load_run``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from include import db
from include.batching import (
    build_upsert_query,
    count_returned,
    deduplicate_rows,
    key_indexes,
)
from include.stg_rules import (
    INDEX_STG_COLUMNS,
    INDEX_STG_KEY,
    STOCK_STG_COLUMNS,
    STOCK_STG_KEY,
    to_stg_index_row,
    to_stg_security_row,
    to_stg_stock_row,
)

DEFAULT_SOURCE_CODE = "moex_iss"

# Колонки, которые обновляются при повторном построении (ключ не трогаем).
STOCK_SOURCE_COLUMNS: tuple[str, ...] = (
    "boardid",
    "secid",
    "tradedate",
    "shortname",
    "numtrades",
    "value",
    "open",
    "low",
    "high",
    "close",
    "waprice",
    "volume",
)
INDEX_SOURCE_COLUMNS: tuple[str, ...] = (
    "index_code",
    "tradedate",
    "shortname",
    "open",
    "low",
    "high",
    "close",
    "value",
)


@dataclass
class TransformResult:
    """Итог построения одной таблицы stg."""

    target_table: str
    date_from: str
    date_to: str
    fetched_rows: int
    valid_rows: int
    invalid_rows: int
    written_rows: int


def _to_insert_tuple(
    fields: dict[str, Any], columns: Sequence[str]
) -> tuple[Any, ...]:
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
    """Вставить/обновить строки и вернуть число затронутых записей.

    Args:
        conn: соединение с БД.
        table: полное имя таблицы, например ``stg.stock_history``.
        columns: полный список колонок вставки.
        key: колонки натурального ключа (по ним ищем конфликт).
        update_columns: колонки, которые перезаписываем при конфликте.
        rows: подготовленные кортежи значений.
        batch_size: размер пачки.

    Returns:
        Число вставленных и обновлённых строк (``RETURNING 1``).
    """
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


def build_stock_history(
    conn: PgConnection, date_from: str, date_to: str, batch_size: int = 500
) -> TransformResult:
    """Построить ``stg.stock_history`` из raw за период (идемпотентно).

    Args:
        conn: соединение с БД мини-DWH.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        batch_size: размер пачки вставки.

    Returns:
        Счётчики: сколько строк прочитано, валидных, отбракованных и записано.
    """
    source_rows = db.fetch_all(
        conn,
        """
        SELECT boardid, secid, tradedate, shortname, numtrades, value,
               open, low, high, close, waprice, volume
          FROM raw.moex_history
         WHERE tradedate BETWEEN %s AND %s
        """,
        (date_from, date_to),
    )
    source_columns = STOCK_SOURCE_COLUMNS
    prepared: list[Sequence[Any]] = []
    valid = 0
    invalid = 0
    for row in source_rows:
        fields = to_stg_stock_row(dict(zip(source_columns, row)))
        if fields["is_valid"]:
            valid += 1
        else:
            invalid += 1
        prepared.append(_to_insert_tuple(fields, STOCK_STG_COLUMNS))

    written = _upsert(
        conn,
        "stg.stock_history",
        STOCK_STG_COLUMNS,
        STOCK_STG_KEY,
        STOCK_SOURCE_COLUMNS,
        prepared,
        batch_size,
    )
    return TransformResult(
        "stg.stock_history", date_from, date_to,
        len(source_rows), valid, invalid, written,
    )


def build_index_history(
    conn: PgConnection, date_from: str, date_to: str, batch_size: int = 500
) -> TransformResult:
    """Построить ``stg.index_history`` из raw за период (идемпотентно).

    Args:
        conn: соединение с БД мини-DWH.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        batch_size: размер пачки вставки.

    Returns:
        Счётчики: сколько строк прочитано, валидных, отбракованных и записано.
    """
    source_rows = db.fetch_all(
        conn,
        """
        SELECT index_code, tradedate, shortname, open, low, high, close, value
          FROM raw.moex_index_history
         WHERE tradedate BETWEEN %s AND %s
        """,
        (date_from, date_to),
    )
    prepared: list[Sequence[Any]] = []
    valid = 0
    invalid = 0
    for row in source_rows:
        fields = to_stg_index_row(dict(zip(INDEX_SOURCE_COLUMNS, row)))
        if fields["is_valid"]:
            valid += 1
        else:
            invalid += 1
        prepared.append(_to_insert_tuple(fields, INDEX_STG_COLUMNS))

    written = _upsert(
        conn,
        "stg.index_history",
        INDEX_STG_COLUMNS,
        INDEX_STG_KEY,
        INDEX_SOURCE_COLUMNS,
        prepared,
        batch_size,
    )
    return TransformResult(
        "stg.index_history", date_from, date_to,
        len(source_rows), valid, invalid, written,
    )


def build_securities(conn: PgConnection, batch_size: int = 500) -> int:
    """Обновить справочник ``stg.securities`` (SCD1) из всего raw.

    Справочник строится по данным торгов: одна строка на (secid, boardid)
    с самой ранней и самой поздней датой торгов. Повторный вызов обновляет
    значения по ключу, дублей не появляется.

    Args:
        conn: соединение с БД мини-DWH.
        batch_size: размер пачки вставки.

    Returns:
        Число вставленных и обновлённых строк справочника.
    """
    aggregate_rows = db.fetch_all(
        conn,
        """
        SELECT secid, boardid,
               min(shortname) AS shortname,
               min(tradedate) AS first_tradedate,
               max(tradedate) AS last_tradedate
          FROM raw.moex_history
         GROUP BY secid, boardid
        """,
    )
    columns = (
        "secid",
        "boardid",
        "shortname",
        "first_tradedate",
        "last_tradedate",
    )
    prepared = [
        _to_insert_tuple(to_stg_security_row(dict(zip(columns, row))), columns)
        for row in aggregate_rows
    ]
    return _upsert(
        conn,
        "stg.securities",
        columns,
        ("secid", "boardid"),
        ("shortname", "first_tradedate", "last_tradedate"),
        prepared,
        batch_size,
    )


__all__ = [
    "INDEX_SOURCE_COLUMNS",
    "STOCK_SOURCE_COLUMNS",
    "TransformResult",
    "build_index_history",
    "build_securities",
    "build_stock_history",
]