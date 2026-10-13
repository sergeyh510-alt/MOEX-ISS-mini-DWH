"""Запись в журнал запусков загрузок (схема meta)."""

from __future__ import annotations

from typing import Any

from psycopg2.extensions import connection as PgConnection

from include import db


def start_load_run(
    conn: PgConnection,
    source_code: str,
    target_table: str,
    date_from: str | None,
    date_to: str | None,
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
) -> int:
    """Открыть запись журнала со статусом ``running`` и вернуть её id."""
    query = """
        INSERT INTO meta.load_run (
            source_code, dag_id, task_id, run_id,
            target_table, date_from, date_to, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'running')
        RETURNING load_run_id
    """
    params: tuple[Any, ...] = (
        source_code,
        dag_id,
        task_id,
        run_id,
        target_table,
        date_from,
        date_to,
    )
    return int(db.fetch_scalar(conn, query, params))


def finish_load_run(
    conn: PgConnection,
    load_run_id: int,
    rows_loaded: int,
    status: str = "success",
    error_message: str | None = None,
    rows_rejected: int = 0,
) -> None:
    """Закрыть запись журнала: статус, число строк, время завершения."""
    query = """
        UPDATE meta.load_run
           SET status = %s,
               rows_loaded = %s,
               rows_rejected = %s,
               error_message = %s,
               finished_at = now()
         WHERE load_run_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(
            query, (status, rows_loaded, rows_rejected, error_message, load_run_id)
        )