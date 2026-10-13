"""CLI-построение слоя mart из core (без Airflow).

Запуск из контейнера Airflow (там есть psycopg2 и доступ к DWH):

    docker compose exec airflow-scheduler \
        python /opt/airflow/scripts/build_mart.py --from 2024-01-09 --to 2024-01-23

Повторный запуск за тот же период безопасен: витрины обновляются upsert'ом
по естественным ключам, дублей не появляется.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Callable

# Позволяет запускать скрипт как из контейнера (/opt/airflow), так и локально.
sys.path.insert(0, "/opt/airflow")

from include import audit, db, mart_builder, sql_init
from include.mart_builder import MartResult

SOURCE_CODE = "moex_iss"
DEFAULT_INDEX = "IMOEX"
MartTransform = Callable[[db.PgConnection], MartResult]


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
    parser = argparse.ArgumentParser(description="Построение слоя mart из core")
    parser.add_argument("--from", dest="date_from", required=True, type=_iso_date)
    parser.add_argument("--to", dest="date_to", required=True, type=_iso_date)
    parser.add_argument(
        "--index",
        dest="index_code",
        default=DEFAULT_INDEX,
        help="индекс для витрины сравнения (по умолчанию IMOEX)",
    )
    return parser


def run_transform(
    target_table: str, date_from: str, date_to: str, transform: MartTransform
) -> MartResult:
    """Выполнить одну трансформацию mart и записать результат в meta.load_run.

    Args:
        target_table: имя витрины (для журнала).
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
    """Точка входа CLI: построить mart за период и записать журнал."""
    args = build_parser().parse_args(argv)
    print(f"Построение mart за {args.date_from}..{args.date_to}")

    with db.transaction() as conn:
        sql_init.apply_sql_init(conn)

    run_transform(
        "mart.stock_returns",
        args.date_from,
        args.date_to,
        lambda conn: mart_builder.build_stock_returns(
            conn, args.date_from, args.date_to
        ),
    )
    run_transform(
        "mart.index_compare",
        args.date_from,
        args.date_to,
        lambda conn: mart_builder.build_index_compare(
            conn, args.index_code, args.date_from, args.date_to
        ),
    )
    run_transform(
        "mart.daily_movers",
        args.date_from,
        args.date_to,
        lambda conn: mart_builder.build_daily_movers(
            conn, args.date_from, args.date_to
        ),
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())