"""Чистый разбор строк ответа MOEX ISS (без зависимостей от БД и сети).

Здесь нет импортов psycopg2/requests, поэтому модуль легко тестируется
оффлайн. Преобразование в параметры вставки делает include/raw_loader.py.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    """Привести значение ISS к ``Decimal`` (``None``, если значения нет)."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def to_int(value: Any) -> int | None:
    """Привести значение ISS к ``int`` (``None``, если значения нет)."""
    if value is None or value == "":
        return None
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def to_text(value: Any) -> str | None:
    """Привести значение ISS к непустой строке."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_tradedate(value: Any) -> date | None:
    """Разобрать дату торгов ISS в формате ``YYYY-MM-DD``."""
    text = to_text(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def build_stock_history_row(row: dict[str, Any]) -> dict[str, Any] | None:
    """Разобрать строку истории акций в набор полей ``raw.moex_history``.

    Args:
        row: строка ответа ISS (``{колонка: значение}``).

    Returns:
        Словарь с полями целевой таблицы или ``None``, если у строки нет
        натурального ключа (BOARDID, SECID, TRADEDATE) - такие строки не грузим.
    """
    boardid = to_text(row.get("BOARDID"))
    secid = to_text(row.get("SECID"))
    tradedate = to_tradedate(row.get("TRADEDATE"))
    if not boardid or not secid or tradedate is None:
        return None
    return {
        "boardid": boardid,
        "secid": secid,
        "tradedate": tradedate,
        "shortname": to_text(row.get("SHORTNAME")),
        "numtrades": to_int(row.get("NUMTRADES")),
        "value": to_decimal(row.get("VALUE")),
        "open": to_decimal(row.get("OPEN")),
        "low": to_decimal(row.get("LOW")),
        "high": to_decimal(row.get("HIGH")),
        "close": to_decimal(row.get("CLOSE")),
        "waprice": to_decimal(row.get("WAPRICE")),
        "volume": to_int(row.get("VOLUME")),
    }


def build_index_history_row(
    row: dict[str, Any], index_code: str
) -> dict[str, Any] | None:
    """Разобрать строку истории индекса в поля ``raw.moex_index_history``.

    Args:
        row: строка ответа ISS.
        index_code: код индекса, например ``IMOEX``.

    Returns:
        Словарь с полями целевой таблицы или ``None`` без даты торгов.
    """
    tradedate = to_tradedate(row.get("TRADEDATE"))
    if tradedate is None:
        return None
    return {
        "index_code": index_code,
        "tradedate": tradedate,
        "shortname": to_text(row.get("SHORTNAME")),
        "open": to_decimal(row.get("OPEN")),
        "low": to_decimal(row.get("LOW")),
        "high": to_decimal(row.get("HIGH")),
        "close": to_decimal(row.get("CLOSE")),
        "value": to_decimal(row.get("VALUE")),
    }