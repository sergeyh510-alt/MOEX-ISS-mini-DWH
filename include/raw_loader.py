"""Загрузка сырых данных MOEX ISS в схему raw.

Модуль не зависит от Airflow: вызывается из CLI-скрипта scripts/load_raw.py
и (позже) из DAG. Идемпотентность обеспечивает ``INSERT ... ON CONFLICT
DO NOTHING`` по натуральному ключу: повторная загрузка тех же строк не создаёт
дублей, а число новых строк считаем по ``RETURNING``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json, execute_values

from include import db
from include.batching import (
    build_insert_query,
    count_returned,
    deduplicate_rows,
    key_indexes,
)
from include.parsing import build_index_history_row, build_stock_history_row
from include.sql_init import apply_sql_init, sql_dir

DEFAULT_SOURCE_CODE = "moex_iss"

STOCK_HISTORY_COLUMNS: tuple[str, ...] = (
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
    "row_json",
    "source_code",
)
STOCK_HISTORY_KEY: tuple[str, ...] = ("boardid", "secid", "tradedate")

INDEX_HISTORY_COLUMNS: tuple[str, ...] = (
    "index_code",
    "tradedate",
    "shortname",
    "open",
    "low",
    "high",
    "close",
    "value",
    "row_json",
    "source_code",
)
INDEX_HISTORY_KEY: tuple[str, ...] = ("index_code", "tradedate")




@dataclass
class LoadResult:
    """Итог одной загрузки в raw."""

    target_table: str
    date_from: str
    date_to: str
    fetched_rows: int
    inserted_rows: int


def to_insert_row(fields: dict[str, Any], columns: Sequence[str]) -> tuple[Any, ...]:
    """Разложить словарь полей в кортеж значений по порядку колонок.

    Поле ``row_json`` оборачивается в ``Json`` (исходная строка ответа ISS).
    """
    values: list[Any] = []
    for column in columns:
        if column == "source_code":
            values.append(DEFAULT_SOURCE_CODE)
        elif column == "row_json":
            values.append(Json(fields["row_json"]))
        else:
            values.append(fields[column])
    return tuple(values)


def insert_rows(
    conn: PgConnection,
    table: str,
    columns: Sequence[str],
    key: Sequence[str],
    rows: Sequence[Sequence[Any]],
    batch_size: int = 500,
) -> int:
    """Вставить строки в raw и вернуть число реально вставленных записей."""
    if not rows:
        return 0

    unique_rows = deduplicate_rows(rows, key_indexes(columns, key))
    query = build_insert_query(table, columns, key)
    inserted = 0
    for offset in range(0, len(unique_rows), batch_size):
        page = unique_rows[offset : offset + batch_size]
        with conn.cursor() as cur:
            result = execute_values(cur, query, page, page_size=batch_size, fetch=True)
            inserted += count_returned(result)
    return inserted


def load_stock_history(
    conn: PgConnection,
    rows: Iterable[dict[str, Any]],
    date_from: str,
    date_to: str,
    batch_size: int = 500,
) -> LoadResult:
    """Загрузить дневную историю акций в ``raw.moex_history`` (идемпотентно).

    Args:
        conn: соединение с БД мини-DWH.
        rows: строки ответа ISS.
        date_from: начало периода ``YYYY-MM-DD`` (для журнала).
        date_to: конец периода ``YYYY-MM-DD`` (для журнала).
        batch_size: размер пачки вставки.

    Returns:
        Итог загрузки: сколько строк получено и сколько реально вставлено.
    """
    fetched = 0
    prepared: list[Sequence[Any]] = []
    for row in rows:
        fetched += 1
        fields = build_stock_history_row(row)
        if fields is not None:
            fields["row_json"] = row
            prepared.append(to_insert_row(fields, STOCK_HISTORY_COLUMNS))

    inserted = insert_rows(
        conn,
        "raw.moex_history",
        STOCK_HISTORY_COLUMNS,
        STOCK_HISTORY_KEY,
        prepared,
        batch_size,
    )
    return LoadResult("raw.moex_history", date_from, date_to, fetched, inserted)


def load_index_history(
    conn: PgConnection,
    rows: Iterable[dict[str, Any]],
    index_code: str,
    date_from: str,
    date_to: str,
    batch_size: int = 500,
) -> LoadResult:
    """Загрузить дневную историю индекса в ``raw.moex_index_history``.

    Args:
        conn: соединение с БД мини-DWH.
        rows: строки ответа ISS.
        index_code: код индекса, например ``IMOEX``.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        batch_size: размер пачки вставки.

    Returns:
        Итог загрузки: сколько строк получено и сколько реально вставлено.
    """
    fetched = 0
    prepared: list[Sequence[Any]] = []
    for row in rows:
        fetched += 1
        fields = build_index_history_row(row, index_code)
        if fields is not None:
            fields["row_json"] = row
            prepared.append(to_insert_row(fields, INDEX_HISTORY_COLUMNS))

    inserted = insert_rows(
        conn,
        "raw.moex_index_history",
        INDEX_HISTORY_COLUMNS,
        INDEX_HISTORY_KEY,
        prepared,
        batch_size,
    )
    return LoadResult("raw.moex_index_history", date_from, date_to, fetched, inserted)


__all__ = [
    "DEFAULT_SOURCE_CODE",
    "INDEX_HISTORY_COLUMNS",
    "INDEX_HISTORY_KEY",
    "LoadResult",
    "STOCK_HISTORY_COLUMNS",
    "STOCK_HISTORY_KEY",
    "apply_sql_init",
    "count_returned",
    "deduplicate_rows",
    "insert_rows",
    "load_index_history",
    "load_stock_history",
    "sql_dir",
    "to_insert_row",
]