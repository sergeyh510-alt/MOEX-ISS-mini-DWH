"""CLI-запуск проверок качества данных (DQ) за период.

Запуск из контейнера Airflow (там есть psycopg2 и доступ к DWH):

    docker compose exec airflow-scheduler \
        python /opt/airflow/scripts/run_dq.py --from 2024-01-09 --to 2024-01-23

Отчёт печатается в stdout, результаты пишутся в audit.dq_run / audit.dq_check.
Если хотя бы одна проверка упала, скрипт возвращает код 1 - в DAG это падение
задачи ``run_dq``, и пайплайн останавливается.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

# Позволяет запускать скрипт как из контейнера (/opt/airflow), так и локально.
sys.path.insert(0, "/opt/airflow")

from include import db, dq_checks, sql_init
from include.dq_rules import DEFAULT_FRESHNESS_MAX_AGE_HOURS

FRESHNESS_ENV = "DQ_FRESHNESS_MAX_HOURS"


def _iso_date(value: str) -> str:
    """Проверить, что аргумент является датой в формате ``YYYY-MM-DD``."""
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Ожидалась дата YYYY-MM-DD, получено: {value}"
        ) from error


def _freshness_hours() -> int:
    """Прочитать порог свежести из окружения (по умолчанию - из dq_rules)."""
    raw = os.getenv(FRESHNESS_ENV)
    if raw in (None, ""):
        return DEFAULT_FRESHNESS_MAX_AGE_HOURS
    try:
        return int(raw)
    except ValueError:
        print(
            f"  {FRESHNESS_ENV}={raw!r} не число, беру "
            f"{DEFAULT_FRESHNESS_MAX_AGE_HOURS}",
            file=sys.stderr,
        )
        return DEFAULT_FRESHNESS_MAX_AGE_HOURS


def build_parser() -> argparse.ArgumentParser:
    """Собрать парсер аргументов командной строки."""
    parser = argparse.ArgumentParser(description="Проверки качества данных за период")
    parser.add_argument("--from", dest="date_from", required=True, type=_iso_date)
    parser.add_argument("--to", dest="date_to", required=True, type=_iso_date)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Точка входа CLI: прогнать DQ за период и вернуть код выхода."""
    args = build_parser().parse_args(argv)
    print(f"Проверки качества данных за {args.date_from}..{args.date_to}")

    with db.transaction() as conn:
        sql_init.apply_sql_init(conn)

    result = dq_checks.run_all_checks(
        args.date_from, args.date_to, _freshness_hours()
    )

    print(f"Запуск #{result.dq_run_id}: статус {result.status}")
    for check in result.results:
        mark = "OK  " if check.status == "passed" else "FAIL"
        line = (
            f"  [{mark}] {check.layer:<4} {check.check_name:<40} "
            f"проблем: {check.issues_count} (порог {check.max_allowed})"
        )
        if check.details:
            line += f" | {check.details}"
        print(line)

    print(
        f"Всего проверок: {result.checks_total}, "
        f"прошло: {result.checks_passed}, упало: {result.checks_failed}"
    )
    return 0 if result.checks_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())