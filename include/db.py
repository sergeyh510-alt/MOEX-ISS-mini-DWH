"""Подключение к БД мини-DWH и мелкие утилиты выполнения SQL."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json, execute_values

from include.config import DwhConfig, get_dwh_config


def connect(config: DwhConfig | None = None) -> PgConnection:
    """Открыть соединение с БД мини-DWH."""
    cfg = config or get_dwh_config()
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


@contextmanager
def transaction(config: DwhConfig | None = None) -> Iterator[PgConnection]:
    """Контекстный менеджер: соединение + коммит/откат транзакции."""
    conn = connect(config)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(
    conn: PgConnection, query: str, params: Sequence[Any] | None = None
) -> list[tuple[Any, ...]]:
    """Выполнить запрос и вернуть все строки результата."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_scalar(
    conn: PgConnection, query: str, params: Sequence[Any] | None = None
) -> Any:
    """Выполнить запрос и вернуть первое значение первой строки."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return None if row is None else row[0]


def execute_script(conn: PgConnection, sql_text: str) -> None:
    """Выполнить SQL-скрипт целиком (один вызов курсора)."""
    with conn.cursor() as cur:
        cur.execute(sql_text)


__all__ = [
    "Json",
    "PgConnection",
    "connect",
    "execute_script",
    "execute_values",
    "fetch_all",
    "fetch_scalar",
    "transaction",
]