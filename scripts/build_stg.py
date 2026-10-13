"""CLI-построение слоя stg из raw (без Airflow).

Запуск из контейнера Airflow (там есть psycopg2 и доступ к DWH):

    docker compose exec airflow-scheduler \
        python /opt/airflow/scripts/build_stg.py --from 2024-01-09 --to 2024-01-23

Повторный запуск за тот же период безопасен: используется upsert по
натуральному ключу, дублей не появляется.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Callable

# Позволяет запускать скрипт как из контейнера (/opt/airflow), так и локально.
sys.path.insert(0, "/opt/airflow")

from include import audit, db, sql_init, stg_builder
from include.stg_builder import TransformResult

SOURCE_CODE = "moex_iss"
Transform = Callable[[db.PgConnection], TransformResult]


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
    parser = argparse.ArgumentParser(description="Построение слоя stg из raw")
    parser.add_argument("--from", dest="date_from", required=True, type=_iso_date)
    parser.add_argument("--to", dest="date_to", required=True, type=_iso_date)
    parser.add_argument(
        "--skip-securities",
        action="store_true",
        help="не обновлять справочник stg.securities",
    )
    return parser


def run_transform(
    target_table: str,
    date_from: str,
    date_to: str,
    transform: Transform,
) -> TransformResult:
    """Выполнить одну трансформацию и записать результат в meta.load_run.

    Args:
        target_table: имя таблицы stg (для журнала).
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
            audit.finish_load_run(
                conn,
                load_run_id,
                result.written_rows,
                "success",
                rows_rejected=result.invalid_rows,
            )
    except Exception as error:
        with db.transaction() as conn:
            audit.finish_load_run(conn, load_run_id, 0, "failed", str(error))
        print(f"  {target_table}: ОШИБКА {error}", file=sys.stderr)
        raise

    print(
        f"  {target_table}: прочитано {result.fetched_rows}, "
        f"валидных {result.valid_rows}, отбраковано {result.invalid_rows}, "
        f"записано {result.written_rows}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """Точка входа CLI: построить stg за период и записать журнал."""
    args = build_parser().parse_args(argv)
    print(f"Построение stg за {args.date_from}..{args.date_to}")

    with db.transaction() as conn:
        sql_init.apply_sql_init(conn)

    run_transform(
        "stg.stock_history",
        args.date_from,
        args.date_to,
        lambda conn: stg_builder.build_stock_history(conn, args.date_from, args.date_to),
    )
    run_transform(
        "stg.index_history",
        args.date_from,
        args.date_to,
        lambda conn: stg_builder.build_index_history(conn, args.date_from, args.date_to),
    )

    if not args.skip_securities:
        with db.transaction() as conn:
            load_run_id = audit.start_load_run(
                conn,
                source_code=SOURCE_CODE,
                target_table="stg.securities",
                date_from=args.date_from,
                date_to=args.date_to,
            )
        with db.transaction() as conn:
            rows = stg_builder.build_securities(conn)
            audit.finish_load_run(conn, load_run_id, rows, "success")
        print(f"  stg.securities: записано {rows}")

    return 0


if __name__ == "__main__":
    sys.exit(main())