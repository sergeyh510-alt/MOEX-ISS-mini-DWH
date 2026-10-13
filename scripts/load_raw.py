"""CLI-загрузка сырых данных MOEX ISS в схему raw.

Запуск без Airflow, из контейнера Airflow (там есть psycopg2 и доступ к DWH):

    docker compose run --rm airflow-scheduler \
        python /opt/airflow/scripts/load_raw.py --from 2024-01-09 --to 2024-01-23 \
        --secids SBER,GAZP,LKOH --board TQBR --index IMOEX

Повторный запуск за тот же период безопасен: вставка идемпотентна
(ON CONFLICT DO NOTHING по натуральному ключу).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

# Позволяет запускать скрипт как из контейнера (/opt/airflow), так и локально.
sys.path.insert(0, "/opt/airflow")

from include import audit, db, raw_loader
from include.config import get_moex_config
from include.moex_client import MoexIssClient

DEFAULT_SECIDS: tuple[str, ...] = ("SBER", "GAZP", "LKOH")
DEFAULT_BOARD = "TQBR"
DEFAULT_INDEX = "IMOEX"
SOURCE_CODE = "moex_iss"


def _parse_secids(value: str) -> list[str]:
    """Разобрать список тикеров из строки ``SBER,GAZP``."""
    return [item.strip().upper() for item in value.split(",") if item.strip()]


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
    parser = argparse.ArgumentParser(description="Загрузка raw-данных MOEX ISS")
    parser.add_argument("--from", dest="date_from", required=True, type=_iso_date)
    parser.add_argument("--to", dest="date_to", required=True, type=_iso_date)
    parser.add_argument("--secids", default=",".join(DEFAULT_SECIDS))
    parser.add_argument("--board", default=DEFAULT_BOARD)
    parser.add_argument("--index", dest="index_code", default=DEFAULT_INDEX)
    parser.add_argument(
        "--skip-index", action="store_true", help="не загружать историю индекса"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Точка входа CLI: загрузить raw-данные за период и записать журнал."""
    args = build_parser().parse_args(argv)
    secids = _parse_secids(args.secids)
    print(
        f"Загрузка MOEX ISS: {args.date_from}..{args.date_to}, "
        f"борда {args.board}, бумаги {secids}"
    )

    moex_config = get_moex_config()
    client = MoexIssClient(moex_config)

    with db.transaction() as conn:
        raw_loader.apply_sql_init(conn)

    total_fetched = 0
    total_inserted = 0

    for secid in secids:
        with db.transaction() as conn:
            load_run_id = audit.start_load_run(
                conn,
                source_code=SOURCE_CODE,
                target_table="raw.moex_history",
                date_from=args.date_from,
                date_to=args.date_to,
            )
        try:
            rows = client.fetch_stock_history(
                secid, args.board, args.date_from, args.date_to
            )
            with db.transaction() as conn:
                result = raw_loader.load_stock_history(
                    conn, rows, args.date_from, args.date_to
                )
                audit.finish_load_run(
                    conn, load_run_id, result.inserted_rows, "success"
                )
        except Exception as error:  # журналируем сбой и продолжаем с другими
            with db.transaction() as conn:
                audit.finish_load_run(
                    conn, load_run_id, 0, "failed", str(error)
                )
            print(f"  {secid}: ОШИБКА {error}", file=sys.stderr)
            continue

        total_fetched += result.fetched_rows
        total_inserted += result.inserted_rows
        print(
            f"  raw.moex_history {secid}: получено {result.fetched_rows}, "
            f"вставлено {result.inserted_rows}"
        )

    if not args.skip_index:
        with db.transaction() as conn:
            load_run_id = audit.start_load_run(
                conn,
                source_code=SOURCE_CODE,
                target_table="raw.moex_index_history",
                date_from=args.date_from,
                date_to=args.date_to,
            )
        try:
            index_rows = client.fetch_index_history(
                args.index_code, args.date_from, args.date_to
            )
            with db.transaction() as conn:
                index_result = raw_loader.load_index_history(
                    conn, index_rows, args.index_code, args.date_from, args.date_to
                )
                audit.finish_load_run(
                    conn, load_run_id, index_result.inserted_rows, "success"
                )
            total_fetched += index_result.fetched_rows
            total_inserted += index_result.inserted_rows
            print(
                f"  raw.moex_index_history {args.index_code}: "
                f"получено {index_result.fetched_rows}, "
                f"вставлено {index_result.inserted_rows}"
            )
        except Exception as error:
            with db.transaction() as conn:
                audit.finish_load_run(conn, load_run_id, 0, "failed", str(error))
            print(f"  {args.index_code}: ОШИБКА {error}", file=sys.stderr)

    print(f"Итого: получено {total_fetched}, вставлено {total_inserted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())