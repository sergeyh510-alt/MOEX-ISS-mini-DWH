"""Выполнение проверок качества данных (DQ) и запись результатов в audit.

Модуль берёт SQL-определения из include/dq_rules.py, выполняет их против
мини-DWH и складывает результаты в audit.dq_run / audit.dq_check. Логика
вывода статуса тоже в dq_rules (чистые функции); здесь - только доступ к БД.

Ошибка самой проверки (например, опечатка в SQL) не роняет весь запуск: такая
проверка получает статус ``error`` и записывается с текстом ошибки.
"""

from __future__ import annotations

from dataclasses import dataclass

from psycopg2.extensions import connection as PgConnection

from include import db
from include.dq_rules import (
    CheckSpec,
    DqResult,
    STATUS_ERROR,
    build_check_specs,
    evaluate_status,
    run_status,
    summarize_run,
)
from include.core_rules import to_date_key

DEFAULT_SOURCE_CODE = "moex_iss"


@dataclass
class DqRunResult:
    """Итог запуска проверок качества."""

    dq_run_id: int
    date_from: str
    date_to: str
    status: str
    checks_total: int
    checks_passed: int
    checks_failed: int
    results: list[DqResult]


def _period_params(date_from: str, date_to: str) -> dict[str, object]:
    """Named-параметры для SQL-проверок (даты и их целые ключи)."""
    return {
        "date_from": date_from,
        "date_to": date_to,
        "date_key_from": to_date_key(date_from),
        "date_key_to": to_date_key(date_to),
    }


def _run_check(
    conn: PgConnection, spec: CheckSpec, params: dict[str, object]
) -> DqResult:
    """Выполнить одну проверку и вернуть её результат."""
    try:
        issues_count = int(db.fetch_scalar(conn, spec.sql, params) or 0)
    except Exception as error:  # noqa: BLE001 - фиксируем любую ошибку проверки
        return DqResult(
            check_name=spec.check_name,
            layer=spec.layer,
            target_table=spec.target_table,
            description=spec.description,
            issues_count=0,
            max_allowed=spec.max_allowed,
            status=STATUS_ERROR,
            details=f"{type(error).__name__}: {error}",
        )
    status = evaluate_status(issues_count, spec.max_allowed)
    return DqResult(
        check_name=spec.check_name,
        layer=spec.layer,
        target_table=spec.target_table,
        description=spec.description,
        issues_count=issues_count,
        max_allowed=spec.max_allowed,
        status=status,
    )


def _start_dq_run(
    conn: PgConnection, date_from: str, date_to: str, checks_total: int
) -> int:
    """Открыть запись запуска проверок со статусом ``running``."""
    query = """
        INSERT INTO audit.dq_run (date_from, date_to, status, checks_total)
        VALUES (%s, %s, 'running', %s)
        RETURNING dq_run_id
    """
    return int(db.fetch_scalar(conn, query, (date_from, date_to, checks_total)))


def _save_check(conn: PgConnection, dq_run_id: int, result: DqResult) -> None:
    """Записать результат одной проверки в audit.dq_check."""
    query = """
        INSERT INTO audit.dq_check (
            dq_run_id, check_name, layer, target_table, description,
            status, issues_count, max_allowed, details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        dq_run_id,
        result.check_name,
        result.layer,
        result.target_table,
        result.description,
        result.status,
        result.issues_count,
        result.max_allowed,
        result.details,
    )
    with conn.cursor() as cur:
        cur.execute(query, params)


def _finish_dq_run(
    conn: PgConnection,
    dq_run_id: int,
    status: str,
    checks_passed: int,
    checks_failed: int,
) -> None:
    """Закрыть запись запуска проверок: итоговый статус и счётчики."""
    query = """
        UPDATE audit.dq_run
           SET status = %s,
               checks_passed = %s,
               checks_failed = %s,
               finished_at = now()
         WHERE dq_run_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (status, checks_passed, checks_failed, dq_run_id))


def run_all_checks(
    date_from: str, date_to: str, freshness_max_age_hours: int | None = None
) -> DqRunResult:
    """Прогнать все проверки за период и записать результаты в audit.

    Args:
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        freshness_max_age_hours: порог свежести в часах; ``None`` - значение
            по умолчанию из dq_rules.

    Returns:
        Итог запуска: id запуска, статус, счётчики и результаты проверок.
    """
    if freshness_max_age_hours is None:
        specs = build_check_specs()
    else:
        specs = build_check_specs(freshness_max_age_hours)
    params = _period_params(date_from, date_to)

    with db.transaction() as conn:
        dq_run_id = _start_dq_run(conn, date_from, date_to, len(specs))

    results: list[DqResult] = []
    with db.transaction() as conn:
        for spec in specs:
            result = _run_check(conn, spec, params)
            _save_check(conn, dq_run_id, result)
            results.append(result)

    checks_total, checks_passed, checks_failed = summarize_run(results)
    status = run_status(checks_failed)
    with db.transaction() as conn:
        _finish_dq_run(conn, dq_run_id, status, checks_passed, checks_failed)

    return DqRunResult(
        dq_run_id=dq_run_id,
        date_from=date_from,
        date_to=date_to,
        status=status,
        checks_total=checks_total,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        results=results,
    )


__all__ = ["DqRunResult", "run_all_checks"]