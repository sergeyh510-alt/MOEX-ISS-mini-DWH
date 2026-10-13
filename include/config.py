"""Настройки проекта, читаемые из переменных окружения.

Значения по умолчанию рассчитаны на локальный docker compose: имена хостов
совпадают с именами сервисов из docker-compose.yml.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DwhConfig:
    """Параметры подключения к БД мини-DWH."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        """Строка подключения для psycopg2/SQLAlchemy."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class MoexConfig:
    """Параметры клиента MOEX ISS API."""

    base_url: str
    page_size: int
    timeout_seconds: float
    max_retries: int
    backoff_factor: float

    @property
    def history_columns(self) -> list[str]:
        """Поля, которые выносим в отдельные колонки raw (если они есть)."""
        return [
            "TRADEDATE",
            "SECID",
            "SHORTNAME",
            "BOARDID",
            "NUMTRADES",
            "VALUE",
            "OPEN",
            "LOW",
            "HIGH",
            "CLOSE",
            "WAPRICE",
            "VOLUME",
        ]


def _env_int(name: str, default: int) -> int:
    """Прочитать целочисленную переменную окружения."""
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    """Прочитать вещественную переменную окружения."""
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


def get_dwh_config() -> DwhConfig:
    """Собрать параметры подключения к мини-DWH из переменных окружения."""
    return DwhConfig(
        host=os.getenv("DWH_DB_HOST", "localhost"),
        port=_env_int("DWH_DB_PORT", 5433),
        database=os.getenv("DWH_DB_NAME", "moex_dwh"),
        user=os.getenv("DWH_DB_USER", "dwh"),
        password=os.getenv("DWH_DB_PASSWORD", "dwh"),
    )


def get_moex_config() -> MoexConfig:
    """Собрать параметры клиента MOEX ISS из переменных окружения."""
    return MoexConfig(
        base_url=os.getenv("MOEX_ISS_BASE_URL", "https://iss.moex.com"),
        page_size=_env_int("MOEX_ISS_PAGE_SIZE", 100),
        timeout_seconds=_env_float("MOEX_ISS_TIMEOUT", 30.0),
        max_retries=_env_int("MOEX_ISS_MAX_RETRIES", 5),
        backoff_factor=_env_float("MOEX_ISS_BACKOFF_FACTOR", 0.5),
    )