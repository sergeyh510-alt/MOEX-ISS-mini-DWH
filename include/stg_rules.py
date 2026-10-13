"""Правила трансформации raw -> stg (чистые функции, без БД).

Здесь нет импортов psycopg2/requests, поэтому логика типизации и проверок
покрывается офлайн-юнит-тестами. Запись в БД делает include/stg_builder.py.

Правила качества выбраны явно (это внутренние правила проекта, а не требования
MOEX ISS); строка не удаляется, а помечается ``is_valid = false`` и причиной
в ``reject_reason`` - решение о фильтрации принимает слой core.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from include.parsing import to_decimal, to_int, to_text

# Имена причин отбраковки (пишутся в stg.*.reject_reason через "; ").
REASON_PRICES_MISSING = "отсутствуют цены open/low/high/close"
REASON_PRICE_NOT_POSITIVE = "цена не положительна"
REASON_HIGH_LT_LOW = "high < low"
REASON_HIGH_LT_OPEN_CLOSE = "high < max(open, close)"
REASON_LOW_GT_OPEN_CLOSE = "low > min(open, close)"
REASON_NUMTRADES_NEGATIVE = "numtrades < 0"
REASON_VOLUME_NEGATIVE = "volume < 0"

PRICE_FIELDS: tuple[str, ...] = ("open", "low", "high", "close")

STOCK_STG_COLUMNS: tuple[str, ...] = (
    "boardid",
    "secid",
    "tradedate",
    "shortname",
    "numtrades",
    "value",
    "open",
    "low",
    "high",
    "close",
    "waprice",
    "volume",
    "is_valid",
    "reject_reason",
    "source_code",
)
STOCK_STG_KEY: tuple[str, ...] = ("boardid", "secid", "tradedate")

INDEX_STG_COLUMNS: tuple[str, ...] = (
    "index_code",
    "tradedate",
    "shortname",
    "open",
    "low",
    "high",
    "close",
    "value",
    "is_valid",
    "reject_reason",
    "source_code",
)
INDEX_STG_KEY: tuple[str, ...] = ("index_code", "tradedate")


def validate_prices(
    open_: Decimal | None,
    low: Decimal | None,
    high: Decimal | None,
    close: Decimal | None,
) -> list[str]:
    """Проверить согласованность цен и вернуть список причин отбраковки.

    Args:
        open_: цена открытия.
        low: минимальная цена.
        high: максимальная цена.
        close: цена закрытия.

    Returns:
        Список причин; пустой список означает, что цены корректны.
    """
    reasons: list[str] = []
    prices = {"open": open_, "low": low, "high": high, "close": close}
    if any(value is None for value in prices.values()):
        return [REASON_PRICES_MISSING]

    if any(value <= 0 for value in prices.values()):  # type: ignore[operator]
        reasons.append(REASON_PRICE_NOT_POSITIVE)

    if high < low:  # type: ignore[operator]
        reasons.append(REASON_HIGH_LT_LOW)
    if high < max(open_, close):  # type: ignore[operator,type-var]
        reasons.append(REASON_HIGH_LT_OPEN_CLOSE)
    if low > min(open_, close):  # type: ignore[operator,type-var]
        reasons.append(REASON_LOW_GT_OPEN_CLOSE)
    return reasons


def validate_counts(numtrades: int | None, volume: int | None) -> list[str]:
    """Проверить неотрицательность счётчиков (если они присутствуют)."""
    reasons: list[str] = []
    if numtrades is not None and numtrades < 0:
        reasons.append(REASON_NUMTRADES_NEGATIVE)
    if volume is not None and volume < 0:
        reasons.append(REASON_VOLUME_NEGATIVE)
    return reasons


def to_stg_stock_row(row: dict[str, Any]) -> dict[str, Any]:
    """Преобразовать строку ``raw.moex_history`` в набор полей stg.

    Args:
        row: строка raw (типизированные колонки, без jsonb).

    Returns:
        Словарь с полями ``stg.stock_history``, включая ``is_valid`` и
        ``reject_reason``.
    """
    open_ = to_decimal(row.get("open"))
    low = to_decimal(row.get("low"))
    high = to_decimal(row.get("high"))
    close = to_decimal(row.get("close"))
    numtrades = to_int(row.get("numtrades"))
    volume = to_int(row.get("volume"))

    reasons = validate_prices(open_, low, high, close)
    reasons += validate_counts(numtrades, volume)

    return {
        "boardid": to_text(row.get("boardid")),
        "secid": to_text(row.get("secid")),
        "tradedate": to_date(row.get("tradedate")),
        "shortname": to_text(row.get("shortname")),
        "numtrades": numtrades,
        "value": to_decimal(row.get("value")),
        "open": open_,
        "low": low,
        "high": high,
        "close": close,
        "waprice": to_decimal(row.get("waprice")),
        "volume": volume,
        "is_valid": not reasons,
        "reject_reason": "; ".join(reasons) or None,
    }


def to_stg_index_row(row: dict[str, Any]) -> dict[str, Any]:
    """Преобразовать строку ``raw.moex_index_history`` в набор полей stg."""
    open_ = to_decimal(row.get("open"))
    low = to_decimal(row.get("low"))
    high = to_decimal(row.get("high"))
    close = to_decimal(row.get("close"))

    reasons = validate_prices(open_, low, high, close)

    return {
        "index_code": to_text(row.get("index_code")),
        "tradedate": to_date(row.get("tradedate")),
        "shortname": to_text(row.get("shortname")),
        "open": open_,
        "low": low,
        "high": high,
        "close": close,
        "value": to_decimal(row.get("value")),
        "is_valid": not reasons,
        "reject_reason": "; ".join(reasons) or None,
    }


def to_stg_security_row(row: dict[str, Any]) -> dict[str, Any]:
    """Преобразовать агрегат из raw в строку справочника ``stg.securities``."""
    return {
        "secid": to_text(row.get("secid")),
        "boardid": to_text(row.get("boardid")),
        "shortname": to_text(row.get("shortname")),
        "first_tradedate": to_date(row.get("first_tradedate")),
        "last_tradedate": to_date(row.get("last_tradedate")),
    }


def to_date(value: Any) -> date | None:
    """Привести значение к ``date`` (принимает ``date`` или строку ISO)."""
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None