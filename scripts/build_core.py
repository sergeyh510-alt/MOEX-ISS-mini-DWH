"""CLI-построение слоя core из stg (без Airflow).

Запуск из контейнера Airflow (там есть psycopg2 и доступ к DWH):

    docker compose exec airflow-scheduler \
        python /opt/airflow/scripts/build_core.py --from 2024-01-09 --to 2024-01-23

Повторный запуск за тот же период безопасен: измерения обновляются upsert'ом
(суррогатные ключи не меняются), факты - upsert'ом по составному ключу.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Callable

# Позволяет запускать скрипт как из контейнера (/opt/airflow), так и локально.
sys.path.insert(0, "/opt/airflow")

from include import audit, core_builder, db, sql_init
from include.core_builder import CoreResult

SOURCE_CODE = "moex_iss"
CoreTransform = Callable[[db.PgConnection], CoreResult]


def _iso_date(value: str) -> str:
    """Проверить, что аргумент является датой в формате ``YYYY-MM-DD``."""
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Ожидалась дата YYYY-MM-DD, получено: {value}"
        ) from error


def build_parser() -> argparse.ArgumentParser:
    """Собрать парсер аргументов командной строки."""
    parser = argparse.ArgumentParser(description="Построение слоя core из stg")
    parser.add_argument("--from", dest="date_from", required=True, type=_iso_date)
    parser.add_argument("--to", dest="date_to", required=True, type=_iso_date)
    return parser


def run_transform(
    target_table: str, date_from: str, date_to: str, transform: CoreTransform
) -> CoreResult:
    """Выполнить одну трансформацию core и записать результат в meta.load_run.

    Args:
        target_table: имя таблицы core (для журнала).
        date_from: начало периода ``YYYY-MM-DD``.
        date_to: конец периода ``YYYY-MM-DD``.
        transform: функция, принимающая соединение и возвращающая итог.

    Returns:
        Итог трансформации.
    """
    with db.transaction() as conn:
        load_run_id = audit.start_load_run(
            conn,
            source_code=SOURCE_CODE,
            target_table=target_table,
            date_from=date_from,
            date_to=date_to,
        )
    try:
        with db.transaction() as conn:
            result = transform(conn)
            audit.finish_load_run(conn, load_run_id, result.rows_written, "success")
    except Exception as error:
        with db.transaction() as conn:
            audit.finish_load_run(conn, load_run_id, 0, "failed", str(error))
        print(f"  {target_table}: ОШИБКА {error}", file=sys.stderr)
        raise

    print(
        f"  {target_table}: прочитано {result.rows_read}, "
        f"записано {result.rows_written}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """Точка входа CLI: построить core за период и записать журнал."""
    args = build_parser().parse_args(argv)
    print(f"Построение core за {args.date_from}..{args.date_to}")

    with db.transaction() as conn:
        sql_init.apply_sql_init(conn)

    run_transform(
        "core.dim_security",
        args.date_from,
        args.date_to,
        lambda conn: CoreResult(
            "core.dim_security", 0, core_builder.build_dim_security(conn)
        ),
    )
    run_transform(
        "core.dim_date",
        args.date_from,
        args.date_to,
        lambda conn: CoreResult(
            "core.dim_date", 0, core_builder.build_dim_date(conn, args.date_from, args.date_to)
        ),
    )
    run_transform(
        "core.fact_daily_prices",
        args.date_from,
        args.date_to,
        lambda conn: core_builder.build_fact_daily_prices(
            conn, args.date_from, args.date_to
        ),
    )
    run_transform(
        "core.fact_daily_index",
        args.date_from,
        args.date_to,
        lambda conn: core_builder.build_fact_daily_index(
            conn, args.date_from, args.date_to
        ),
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())