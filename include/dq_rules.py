"""Правила слоя audit: определения проверок и оценка их результата.

Модуль не зависит от psycopg2: здесь только SQL-тексты проверок (чистые
данные) и логика вывода статуса. Выполнение проверок и запись результатов в
БД - в include/dq_checks.py. Благодаря этому реестр проверок и граничные
случаи оценки покрываются оффлайн-юнит-тестами.

Каждая проверка возвращает одно число - сколько проблемных строк (или единиц
проблемы) она нашла. Проверка проходит, если это число не превышает порог
``max_allowed``. Запросы параметризованы периодом: ``%(date_from)s`` и
``%(date_to)s`` (даты), ``%(date_key_from)s`` и ``%(date_key_to)s``
(целые ключи дат в формате YYYYMMDD) - это named-параметры psycopg2.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Sequence

DqCheckStatus = Literal["passed", "failed", "error"]
DqRunStatus = Literal["passed", "failed"]

STATUS_PASSED: DqCheckStatus = "passed"
STATUS_FAILED: DqCheckStatus = "failed"
STATUS_ERROR: DqCheckStatus = "error"

# Порог свежести по умолчанию - 30 суток. Значение намеренно щедрое: проект
# запускается вручную и бэкфиллом, поэтому данные за старые периоды не должны
# ломать проверку. Порог переопределяется переменной DQ_FRESHNESS_MAX_HOURS.
DEFAULT_FRESHNESS_MAX_AGE_HOURS = 720


@dataclass(frozen=True)
class CheckSpec:
    """Определение проверки: что считаем и какой порог считаем нормой."""

    check_name: str
    layer: str
    target_table: str
    description: str
    sql: str
    max_allowed: int = 0


@dataclass(frozen=True)
class DqResult:
    """Результат выполненной проверки."""

    check_name: str
    layer: str
    target_table: str
    description: str
    issues_count: int
    max_allowed: int
    status: DqCheckStatus
    details: str | None = None


def evaluate_status(issues_count: int, max_allowed: int = 0) -> DqCheckStatus:
    """Определить статус проверки по числу проблем и порогу.

    Args:
        issues_count: сколько проблемных строк (или единиц проблемы) найдено.
        max_allowed: допустимое число проблем (по умолчанию 0).

    Returns:
        ``passed``, если ``issues_count`` не превышает порог, иначе ``failed``.
    """
    return STATUS_PASSED if issues_count <= max_allowed else STATUS_FAILED


def summarize_run(results: Sequence[DqResult]) -> tuple[int, int, int]:
    """Собрать сводку запуска: (всего проверок, прошло, упало).

    Упавшей считается проверка со статусом ``failed`` или ``error``.
    """
    failed = sum(1 for result in results if result.status != STATUS_PASSED)
    return len(results), len(results) - failed, failed


def run_status(checks_failed: int) -> DqRunStatus:
    """Статус запуска проверок: любая упавшая проверка делает запуск неуспешным."""
    return STATUS_PASSED if checks_failed == 0 else STATUS_FAILED


def is_stale(
    last_loaded_at: datetime | None, now: datetime, max_age_hours: float
) -> bool:
    """Устарели ли данные: нет отметки времени или она старше порога.

    Граница включительна: ровно ``max_age_hours`` назад - ещё не устарело.

    Args:
        last_loaded_at: момент последней загрузки (``None``, если данных нет).
        now: текущий момент для сравнения.
        max_age_hours: допустимый возраст данных в часах.

    Returns:
        ``True``, если данных нет или они старше порога.
    """
    if last_loaded_at is None:
        return True
    return now - last_loaded_at > timedelta(hours=max_age_hours)


def _duplicate_key_sql(select_dup: str) -> str:
    """SQL, считающий лишние строки в группах-дублях натурального ключа."""
    return (
        "SELECT coalesce(sum(cnt - 1), 0) FROM ("
        f"{select_dup} HAVING count(*) > 1) dup"
    )


# Проверки, ограниченные запрошенным периодом (или глобальные для измерений).
_PERIOD_CHECKS: tuple[CheckSpec, ...] = (
    # --- raw: сырые данные ---
    CheckSpec(
        check_name="raw_history_duplicate_key",
        layer="raw",
        target_table="raw.moex_history",
        description="Нет дублей натурального ключа (boardid, secid, tradedate)",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM raw.moex_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
             GROUP BY boardid, secid, tradedate
            """
        ),
    ),
    CheckSpec(
        check_name="raw_history_missing_close",
        layer="raw",
        target_table="raw.moex_history",
        description="Заполнена обязательная цена закрытия",
        sql="""
            SELECT count(*) FROM raw.moex_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND close IS NULL
        """,
    ),
    CheckSpec(
        check_name="raw_history_non_positive_price",
        layer="raw",
        target_table="raw.moex_history",
        description="Цены положительные (close, low, high)",
        sql="""
            SELECT count(*) FROM raw.moex_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND (close <= 0 OR low <= 0 OR high <= 0)
        """,
    ),
    CheckSpec(
        check_name="raw_index_history_duplicate_key",
        layer="raw",
        target_table="raw.moex_index_history",
        description="Нет дублей натурального ключа (index_code, tradedate)",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM raw.moex_index_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
             GROUP BY index_code, tradedate
            """
        ),
    ),
    CheckSpec(
        check_name="raw_index_history_missing_close",
        layer="raw",
        target_table="raw.moex_index_history",
        description="Заполнено обязательное значение индекса",
        sql="""
            SELECT count(*) FROM raw.moex_index_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND close IS NULL
        """,
    ),
    # --- stg: типизированные данные ---
    CheckSpec(
        check_name="stg_stock_duplicate_key",
        layer="stg",
        target_table="stg.stock_history",
        description="Нет дублей натурального ключа (boardid, secid, tradedate)",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM stg.stock_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
             GROUP BY boardid, secid, tradedate
            """
        ),
    ),
    CheckSpec(
        check_name="stg_stock_rejected_rows",
        layer="stg",
        target_table="stg.stock_history",
        description="Нет строк, отбракованных проверками качества",
        sql="""
            SELECT count(*) FROM stg.stock_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND NOT is_valid
        """,
    ),
    CheckSpec(
        check_name="stg_stock_rejected_without_reason",
        layer="stg",
        target_table="stg.stock_history",
        description="У отбракованных строк указана причина отбраковки",
        sql="""
            SELECT count(*) FROM stg.stock_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND NOT is_valid
               AND (reject_reason IS NULL OR reject_reason = '')
        """,
    ),
    CheckSpec(
        check_name="stg_stock_missing_required_field",
        layer="stg",
        target_table="stg.stock_history",
        description="У валидных строк заполнены open/high/low/close",
        sql="""
            SELECT count(*) FROM stg.stock_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND is_valid
               AND (close IS NULL OR open IS NULL OR high IS NULL OR low IS NULL)
        """,
    ),
    CheckSpec(
        check_name="stg_stock_non_positive_price",
        layer="stg",
        target_table="stg.stock_history",
        description="У валидных строк цены положительные",
        sql="""
            SELECT count(*) FROM stg.stock_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND is_valid
               AND (close <= 0 OR low <= 0 OR high <= 0)
        """,
    ),
    CheckSpec(
        check_name="stg_index_missing_close",
        layer="stg",
        target_table="stg.index_history",
        description="У валидных строк индекса заполнено значение",
        sql="""
            SELECT count(*) FROM stg.index_history
             WHERE tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND is_valid
               AND close IS NULL
        """,
    ),
    # --- core: звезда ---
    CheckSpec(
        check_name="core_dim_security_duplicate_key",
        layer="core",
        target_table="core.dim_security",
        description="Нет дублей натурального ключа (secid, boardid)",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM core.dim_security
             GROUP BY secid, boardid
            """
        ),
    ),
    CheckSpec(
        check_name="core_fact_prices_duplicate_key",
        layer="core",
        target_table="core.fact_daily_prices",
        description="Нет дублей ключа (security_key, date_key) за период",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM core.fact_daily_prices
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
             GROUP BY security_key, date_key
            """
        ),
    ),
    CheckSpec(
        check_name="core_fact_prices_non_positive_price",
        layer="core",
        target_table="core.fact_daily_prices",
        description="Цены факта положительные (close, low, high)",
        sql="""
            SELECT count(*) FROM core.fact_daily_prices
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
               AND (close <= 0 OR low <= 0 OR high <= 0)
        """,
    ),
    CheckSpec(
        check_name="core_fact_prices_prev_close_gap",
        layer="core",
        target_table="core.fact_daily_prices",
        description="prev_close пуст только в первый торговый день бумаги",
        sql="""
            SELECT count(*) FROM core.fact_daily_prices f
             WHERE f.date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
               AND f.prev_close IS NULL
               AND f.date_key > (
                   SELECT min(f2.date_key) FROM core.fact_daily_prices f2
                    WHERE f2.security_key = f.security_key
               )
        """,
    ),
    CheckSpec(
        check_name="core_fact_index_duplicate_key",
        layer="core",
        target_table="core.fact_daily_index",
        description="Нет дублей ключа (index_code, date_key) за период",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM core.fact_daily_index
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
             GROUP BY index_code, date_key
            """
        ),
    ),
    CheckSpec(
        check_name="core_dim_date_key_mismatch",
        layer="core",
        target_table="core.dim_date",
        description="date_key совпадает с календарной датой (YYYYMMDD)",
        sql="""
            SELECT count(*) FROM core.dim_date
             WHERE calendar_date BETWEEN %(date_from)s AND %(date_to)s
               AND date_key <> to_char(calendar_date, 'YYYYMMDD')::integer
        """,
    ),
    # --- mart: витрины ---
    CheckSpec(
        check_name="mart_stock_returns_duplicate_key",
        layer="mart",
        target_table="mart.stock_returns",
        description="Нет дублей ключа (security_key, date_key) за период",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM mart.stock_returns
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
             GROUP BY security_key, date_key
            """
        ),
    ),
    CheckSpec(
        check_name="mart_stock_returns_non_positive_close",
        layer="mart",
        target_table="mart.stock_returns",
        description="Цена закрытия в витрине положительная",
        sql="""
            SELECT count(*) FROM mart.stock_returns
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
               AND close <= 0
        """,
    ),
    CheckSpec(
        check_name="mart_index_compare_duplicate_key",
        layer="mart",
        target_table="mart.index_compare",
        description="Нет дублей ключа (index_code, security_key, date_key)",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM mart.index_compare
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
             GROUP BY index_code, security_key, date_key
            """
        ),
    ),
    CheckSpec(
        check_name="mart_daily_movers_duplicate_key",
        layer="mart",
        target_table="mart.daily_movers",
        description="Нет дублей ключа (date_key, security_key)",
        sql=_duplicate_key_sql(
            """
            SELECT count(*) AS cnt FROM mart.daily_movers
             WHERE date_key BETWEEN %(date_key_from)s AND %(date_key_to)s
             GROUP BY date_key, security_key
            """
        ),
    ),
    # --- согласованность цепочки raw -> stg -> core -> mart ---
    CheckSpec(
        check_name="chain_stg_missing_from_raw",
        layer="stg",
        target_table="raw.moex_history -> stg.stock_history",
        description="Каждой строке raw за период соответствует строка stg",
        sql="""
            SELECT count(*) FROM raw.moex_history r
             WHERE r.tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND NOT EXISTS (
                   SELECT 1 FROM stg.stock_history s
                    WHERE s.boardid = r.boardid
                      AND s.secid = r.secid
                      AND s.tradedate = r.tradedate
               )
        """,
    ),
    CheckSpec(
        check_name="chain_core_prices_missing_from_stg",
        layer="core",
        target_table="stg.stock_history -> core.fact_daily_prices",
        description="Каждой валидной строке stg за период соответствует факт core",
        sql="""
            SELECT count(*) FROM stg.stock_history s
             JOIN core.dim_security d
               ON d.secid = s.secid AND d.boardid = s.boardid
             WHERE s.is_valid
               AND s.tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND NOT EXISTS (
                   SELECT 1 FROM core.fact_daily_prices f
                    JOIN core.dim_date dd ON dd.date_key = f.date_key
                    WHERE f.security_key = d.security_key
                      AND dd.calendar_date = s.tradedate
               )
        """,
    ),
    CheckSpec(
        check_name="chain_core_index_missing_from_stg",
        layer="core",
        target_table="stg.index_history -> core.fact_daily_index",
        description="Каждой валидной строке индекса stg соответствует факт core",
        sql="""
            SELECT count(*) FROM stg.index_history i
             WHERE i.is_valid
               AND i.tradedate BETWEEN %(date_from)s AND %(date_to)s
               AND NOT EXISTS (
                   SELECT 1 FROM core.fact_daily_index f
                    JOIN core.dim_date dd ON dd.date_key = f.date_key
                    WHERE f.index_code = i.index_code
                      AND dd.calendar_date = i.tradedate
               )
        """,
    ),
    CheckSpec(
        check_name="chain_mart_missing_from_core",
        layer="mart",
        target_table="core.fact_daily_prices -> mart.stock_returns",
        description="Каждому факту core за период соответствует строка витрины",
        sql="""
            SELECT count(*) FROM core.fact_daily_prices f
             JOIN core.dim_date dd ON dd.date_key = f.date_key
             WHERE dd.calendar_date BETWEEN %(date_from)s AND %(date_to)s
               AND NOT EXISTS (
                   SELECT 1 FROM mart.stock_returns m
                    WHERE m.security_key = f.security_key
                      AND m.date_key = f.date_key
               )
        """,
    ),
)

# Таблицы, для которых проверяется свежесть: (слой, таблица).
_FRESHNESS_TABLES: tuple[tuple[str, str], ...] = (
    ("raw", "raw.moex_history"),
    ("stg", "stg.stock_history"),
    ("core", "core.fact_daily_prices"),
    ("mart", "mart.stock_returns"),
)


def _freshness_sql(table: str) -> str:
    """SQL проверки свежести: сколько часов назад таблица пополнялась."""
    return (
        "SELECT coalesce(ceil(extract(epoch FROM (now() - max(loaded_at))) / 3600), "
        f"1000000)::bigint FROM {table}"
    )


def build_check_specs(
    freshness_max_age_hours: int = DEFAULT_FRESHNESS_MAX_AGE_HOURS,
) -> tuple[CheckSpec, ...]:
    """Собрать полный список проверок: период и свежесть.

    Args:
        freshness_max_age_hours: допустимый возраст последней загрузки слоя.

    Returns:
        Кортеж определений проверок в порядке выполнения.
    """
    freshness = tuple(
        CheckSpec(
            check_name=f"freshness_{layer}",
            layer=layer,
            target_table=table,
            description=(
                f"Данные {table} загружались не позже {freshness_max_age_hours} ч назад"
            ),
            sql=_freshness_sql(table),
            max_allowed=freshness_max_age_hours,
        )
        for layer, table in _FRESHNESS_TABLES
    )
    return _PERIOD_CHECKS + freshness


__all__ = [
    "CheckSpec",
    "DEFAULT_FRESHNESS_MAX_AGE_HOURS",
    "DqResult",
    "STATUS_ERROR",
    "STATUS_FAILED",
    "STATUS_PASSED",
    "build_check_specs",
    "evaluate_status",
    "is_stale",
    "run_status",
    "summarize_run",
]