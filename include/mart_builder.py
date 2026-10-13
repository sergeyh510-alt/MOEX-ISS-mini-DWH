"""Построение слоя mart из core (витрины для аналитики).

Все витрины строятся из ``core.*`` и пересчитываются upsert'ом по своим
естественным ключам, поэтому повторный запуск за тот же период не создаёт
дублей. Расчёты метрик - в include/mart_rules.py (чистые функции).

Порядок построения: stock_returns -> index_compare -> daily_movers
(daily_movers ранжирует значения, посчитанные в stock_returns).
"""

from __future__ import annotations

from dataclasses import dataclass
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
from include.core_rules import to_date_key
from include.mart_rules import (
    DAILY_MOVERS_COLUMNS,
    INDEX_COMPARE_COLUMNS,
    STOCK_RETURNS_COLUMNS,
    compute_excess_return,
    compute_stock_metrics,
    rank_returns,
)

STOCK_RETURNS_KEY: tuple[str, ...] = ("security_key", "date_key")
INDEX_COMPARE_KEY: tuple[str, ...] = ("index_code", "security_key", "date_key")
DAILY_MOVERS_KEY: tuple[str, ...] = ("date_key", "security_key")

STOCK_RETURNS_UPDATE: tuple[str, ...] = (
    "close",
    "prev_close",
    "daily_return_pct",
    "gap_pct",
    "amplitude_pct",
    "ma_5",
    "ma_20",
    "volatility_5_pct",
    "volatility_20_pct",
    "cumulative_return_pct",
)
INDEX_COMPARE_UPDATE: tuple[str, ...] = (
    "stock_return_pct",
    "index_return_pct",
    "excess_return_pct",
)
DAILY_MOVERS_UPDATE: tuple[str, ...] = (
    "daily_return_pct",
    "return_rank",
    "is_best_of_day",
    "is_worst_of_day",
)


@dataclass
class MartResult:
    """Итог построения одной витрины."""

    target_table: str
    rows_read: int
    rows_written: int


def _upsert(
    conn: PgConnection,
    table: str,
    columns: Sequence[str],
    key: Sequence[str],
    update_columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    batch_size: int = 500,
) -> int:
    """Вставить/обновить строки витрины и вернуть число затронутых записей."""
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


def _to_tuple(fields: Mapping[str, Any], columns: Sequence[str]) -> tuple[Any, ...]:
    """Разложить словарь полей в кортеж значений по порядку колонок."""
    return tuple(fields.get(column) for column in columns)


def build_stock_returns(
    conn: PgConnection, date_from: str, date_to: str, batch_size: int = 500
) -> MartResult:
    """Построить ``mart.stock_returns`` (доходность и производные метрики).

    Метрики считаются по всей доступной истории core (чтобы ``prev_close``
    и скользящие окна были корректны на первой дате периода), а записываются
    только строки периода.

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
        SELECT f.security_key, f.date_key, s.secid, s.boardid,
               f.open, f.high, f.low, f.close, f.prev_close
          FROM core.fact_daily_prices f
          JOIN core.dim_security s USING (security_key)
         ORDER BY f.security_key, f.date_key
        """,
    )
    columns = (
        "security_key",
        "date_key",
        "secid",
        "boardid",
        "open",
        "high",
        "low",
        "close",
        "prev_close",
    )
    records = [dict(zip(columns, row)) for row in source_rows]

    # Группируем по бумаге, сохраняя порядок по возрастанию даты.
    series_by_security: dict[int, list[dict[str, Any]]] = {}
    for record in records:
        series_by_security.setdefault(int(record["security_key"]), []).append(record)

    prepared: list[dict[str, Any]] = []
    for security_key, series in series_by_security.items():
        metrics = compute_stock_metrics(series)
        for record, metric in zip(series, metrics):
            date_key = int(record["date_key"])
            if not (to_date_key(date_from) <= date_key <= to_date_key(date_to)):
                continue
            prepared.append(
                {
                    "security_key": security_key,
                    "date_key": date_key,
                    "secid": record["secid"],
                    "boardid": record["boardid"],
                    **metric,
                }
            )

    rows = [_to_tuple(row, STOCK_RETURNS_COLUMNS) for row in prepared]
    written = _upsert(
        conn,
        "mart.stock_returns",
        STOCK_RETURNS_COLUMNS,
        STOCK_RETURNS_KEY,
        STOCK_RETURNS_UPDATE,
        rows,
        batch_size,
    )
    return MartResult("mart.stock_returns", len(source_rows), written)


def build_index_compare(
    conn: PgConnection,
    index_code: str,
    date_from: str,
    date_to: str,
    batch_size: int = 500,
) -> MartResult:
    """Построить ``mart.index_compare`` (бумага против индекса за тот же день).

    В витрину попадают только дни, за которые сравнение вычислимо: соединение
    идёт по ``date_key`` (INNER JOIN, поэтому дни без индекса или без бумаги
    отсекаются), плюс отсекаются дни с неизвестной доходностью (первый торговый
    день без ``prev_close``) - сравнивать NULL с NULL не имеет смысла.

    Args:
        conn: соединение с БД мини-DWH.
        index_code: код индекса для сравнения, например ``IMOEX``.
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        batch_size: размер пачки вставки.

    Returns:
        Итог: сколько строк прочитано и записано.
    """
    source_rows = db.fetch_all(
        conn,
        """
        SELECT i.index_code, r.security_key, r.date_key, r.secid,
               r.daily_return_pct AS stock_return_pct,
               i.close_change_pct AS index_return_pct
          FROM mart.stock_returns r
          JOIN core.fact_daily_index i
            ON i.date_key = r.date_key
           AND i.index_code = %s
         WHERE r.date_key BETWEEN %s AND %s
           AND r.daily_return_pct IS NOT NULL
           AND i.close_change_pct IS NOT NULL
         ORDER BY r.date_key, r.security_key
        """,
        (index_code, to_date_key(date_from), to_date_key(date_to)),
    )
    columns = (
        "index_code",
        "security_key",
        "date_key",
        "secid",
        "stock_return_pct",
        "index_return_pct",
    )
    prepared: list[dict[str, Any]] = []
    for row in source_rows:
        record = dict(zip(columns, row))
        prepared.append(
            {
                "index_code": record["index_code"],
                "security_key": int(record["security_key"]),
                "date_key": int(record["date_key"]),
                "secid": record["secid"],
                "stock_return_pct": record["stock_return_pct"],
                "index_return_pct": record["index_return_pct"],
                "excess_return_pct": compute_excess_return(
                    record["stock_return_pct"], record["index_return_pct"]
                ),
            }
        )

    rows = [_to_tuple(row, INDEX_COMPARE_COLUMNS) for row in prepared]
    written = _upsert(
        conn,
        "mart.index_compare",
        INDEX_COMPARE_COLUMNS,
        INDEX_COMPARE_KEY,
        INDEX_COMPARE_UPDATE,
        rows,
        batch_size,
    )
    return MartResult("mart.index_compare", len(source_rows), written)


def build_daily_movers(
    conn: PgConnection, date_from: str, date_to: str, batch_size: int = 500
) -> MartResult:
    """Построить ``mart.daily_movers`` (лучшие и худшие бумаги дня).

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
        SELECT date_key, security_key, secid, daily_return_pct
          FROM mart.stock_returns
         WHERE date_key BETWEEN %s AND %s
         ORDER BY date_key, security_key
        """,
        (to_date_key(date_from), to_date_key(date_to)),
    )
    columns = ("date_key", "security_key", "secid", "daily_return_pct")
    records = [dict(zip(columns, row)) for row in source_rows]

    rows_by_day: dict[int, list[dict[str, Any]]] = {}
    for record in records:
        rows_by_day.setdefault(int(record["date_key"]), []).append(record)

    prepared: list[dict[str, Any]] = []
    for date_key, day_rows in rows_by_day.items():
        for entry in rank_returns(day_rows):
            prepared.append(
                {
                    "date_key": date_key,
                    "security_key": int(entry["security_key"]),
                    "secid": entry["secid"],
                    "daily_return_pct": entry["daily_return_pct"],
                    "return_rank": entry["return_rank"],
                    "is_best_of_day": entry["is_best_of_day"],
                    "is_worst_of_day": entry["is_worst_of_day"],
                }
            )

    rows = [_to_tuple(row, DAILY_MOVERS_COLUMNS) for row in prepared]
    written = _upsert(
        conn,
        "mart.daily_movers",
        DAILY_MOVERS_COLUMNS,
        DAILY_MOVERS_KEY,
        DAILY_MOVERS_UPDATE,
        rows,
        batch_size,
    )
    return MartResult("mart.daily_movers", len(source_rows), written)



__all__ = [
    "DAILY_MOVERS_KEY",
    "INDEX_COMPARE_KEY",
    "MartResult",
    "STOCK_RETURNS_KEY",
    "build_daily_movers",
    "build_index_compare",
    "build_stock_returns",
]