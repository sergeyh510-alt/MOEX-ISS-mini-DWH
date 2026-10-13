"""DAG мини-DWH на данных MOEX ISS: ежедневная загрузка и трансформация.

Задачи разделены по слоям и не смешиваются:
  1. load_raw   - загрузка сырых данных MOEX ISS в слой raw (scripts/load_raw.py);
  2. build_stg  - типизация и проверки raw -> stg (scripts/build_stg.py);
  3. build_core - звезда stg -> core: измерения и факты (scripts/build_core.py);
  4. build_mart - витрины core -> mart (scripts/build_mart.py);
  5. run_dq     - проверки качества данных mart/core/stg/raw (scripts/run_dq.py).

Задача run_dq завершает пайплайн и возвращает ненулевой код при упавшей
проверке: падение DQ останавливает DAG, а результаты видны в audit.dq_check.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_SECIDS = "SBER,GAZP,LKOH"
DEFAULT_BOARD = "TQBR"
DEFAULT_INDEX = "IMOEX"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="moex_dwh_daily",
    description="Загрузка MOEX ISS (raw), построение stg, core, mart и DQ за дату запуска",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["moex", "raw", "stg", "core", "mart", "audit"],
) as dag:
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=(
            "python /opt/airflow/scripts/load_raw.py "
            "--from {{ ds }} --to {{ ds }} "
            f"--secids {DEFAULT_SECIDS} --board {DEFAULT_BOARD} "
            f"--index {DEFAULT_INDEX}"
        ),
    )

    build_stg = BashOperator(
        task_id="build_stg",
        bash_command=(
            "python /opt/airflow/scripts/build_stg.py "
            "--from {{ ds }} --to {{ ds }}"
        ),
    )

    build_core = BashOperator(
        task_id="build_core",
        bash_command=(
            "python /opt/airflow/scripts/build_core.py "
            "--from {{ ds }} --to {{ ds }}"
        ),
    )

    build_mart = BashOperator(
        task_id="build_mart",
        bash_command=(
            "python /opt/airflow/scripts/build_mart.py "
            "--from {{ ds }} --to {{ ds }} "
            f"--index {DEFAULT_INDEX}"
        ),
    )

    run_dq = BashOperator(
        task_id="run_dq",
        bash_command=(
            "python /opt/airflow/scripts/run_dq.py "
            "--from {{ ds }} --to {{ ds }}"
        ),
    )

    load_raw >> build_stg >> build_core >> build_mart >> run_dq