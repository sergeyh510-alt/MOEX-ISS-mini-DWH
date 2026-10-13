"""Идемпотентное применение DDL-скриптов DWH.

Вынесено отдельно от raw_loader: перед запуском любой задачи нужно гарантировать,
что объекты слоёв существуют. На чистом томе скрипты применяет
``docker-entrypoint-initdb.d``, а на уже существующем томе - этот модуль.

Скрипты упорядочены по номеру (00, 10, 20, ...), чтобы зависимости
(схемы -> таблицы) соблюдались.
"""

from __future__ import annotations

from pathlib import Path

from psycopg2.extensions import connection as PgConnection

from include import db

SQL_DIR = Path("/opt/airflow/sql")
LOCAL_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Порядок применения важен: сначала схемы, затем таблицы слоёв.
INIT_SCRIPTS: tuple[str, ...] = (
    "00_schemas.sql",
    "10_raw.sql",
    "20_stg.sql",
    "30_core.sql",
    "40_mart.sql",
    "50_meta.sql",
    "60_audit.sql",
)


def sql_dir() -> Path:
    """Каталог SQL-скриптов (сначала путь в контейнере Airflow)."""
    return SQL_DIR if SQL_DIR.is_dir() else LOCAL_SQL_DIR


def apply_sql_init(conn: PgConnection) -> None:
    """Идемпотентно применить DDL-скрипты DWH.

    Args:
        conn: соединение с БД мини-DWH.
    """
    for script_name in INIT_SCRIPTS:
        script_path = sql_dir() / script_name
        db.execute_script(conn, script_path.read_text(encoding="utf-8"))


__all__ = ["INIT_SCRIPTS", "apply_sql_init", "sql_dir"]