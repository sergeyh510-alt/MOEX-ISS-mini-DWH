"""Чистые утилиты пачечной вставки (без зависимостей от БД).

Вынесены из include/raw_loader.py, чтобы их можно было тестировать оффлайн,
без psycopg2 и без соединения с базой.
"""

from __future__ import annotations

from typing import Any, Sequence


def key_indexes(columns: Sequence[str], key: Sequence[str]) -> list[int]:
    """Индексы колонок бизнес-ключа внутри списка колонок вставки."""
    return [columns.index(name) for name in key]


def deduplicate_rows(
    rows: Sequence[Sequence[Any]], key_positions: Sequence[int]
) -> list[Sequence[Any]]:
    """Убрать повторы по бизнес-ключу внутри одной пачки вставки.

    Postgres не допускает ``ON CONFLICT DO NOTHING`` для строк, которые
    конфликтуют между собой внутри одного ``INSERT``, поэтому дубли снимаем
    заранее. Порядок строк сохраняется.
    """
    seen: set[tuple[Any, ...]] = set()
    unique_rows: list[Sequence[Any]] = []
    for row in rows:
        row_key = tuple(row[index] for index in key_positions)
        if row_key in seen:
            continue
        seen.add(row_key)
        unique_rows.append(row)
    return unique_rows


def count_returned(result: Any) -> int:
    """Посчитать строки, вставленные запросом с ``RETURNING``.

    ``execute_values`` с ``fetch=True`` возвращает список кортежей, если все
    строки поместились в одну страницу, и список списков кортежей - если
    запрос был разбит на несколько страниц.
    """
    if not result:
        return 0
    if isinstance(result[0], tuple):
        return len(result)
    return sum(len(page) for page in result)


def build_insert_query(
    table: str, columns: "Sequence[str]", key: "Sequence[str]"
) -> str:
    """Собрать идемпотентный ``INSERT ... ON CONFLICT DO NOTHING``.

    Args:
        table: полное имя таблицы.
        columns: список колонок вставки.
        key: колонки натурального ключа.

    Returns:
        SQL с ``RETURNING 1`` (по нему считаем вставленные строки).
    """
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(key)}) DO NOTHING RETURNING 1"
    )


def build_upsert_query(
    table: str,
    columns: "Sequence[str]",
    key: "Sequence[str]",
    update_columns: "Sequence[str]",
) -> str:
    """Собрать идемпотентный ``INSERT ... ON CONFLICT DO UPDATE``.

    При конфликте перезаписываются ``update_columns`` и ``updated_at``.

    Args:
        table: полное имя таблицы.
        columns: список колонок вставки.
        key: колонки натурального ключа.
        update_columns: колонки, которые перезаписываем при конфликте.

    Returns:
        SQL с ``RETURNING 1`` (по нему считаем затронутые строки).
    """
    assignments = ", ".join(f"{name} = EXCLUDED.{name}" for name in update_columns)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET "
        f"{assignments}, updated_at = now() RETURNING 1"
    )