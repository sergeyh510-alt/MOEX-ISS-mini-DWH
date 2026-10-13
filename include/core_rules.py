"""Правила построения слоя core (чистые функции, без БД).

Здесь нет импортов psycopg2/requests, поэтому логика измерений, суррогатных
ключей и расчёта изменений цены покрывается офлайн-юнит-тестами. Запись в БД
делает include/core_builder.py.

Измерения - SCD1: суррогатный ключ выдаёт Postgres (IDENTITY) один раз, при
повторном построении строка обновляется по естественному ключу и ключ не
меняется. Поэтому связь факта с измерением стабильна между запусками.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence

# Русские сокращения дней недели; порядок совпадает с date.weekday() (Пн = 0).
DAY_NAMES: tuple[str, ...] = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")

# Точность процента изменения цены.
PCT_QUANT = Decimal("0.0001")

DIM_DATE_COLUMNS: tuple[str, ...] = (
    "date_key",
    "calendar_date",
    "year",
    "quarter",
    "month",
    "day",
    "iso_week",
    "day_of_week",
    "day_name",
    "is_weekend",
    "is_trading_day",
)

DIM_SECURITY_COLUMNS: tuple[str, ...] = (
    "secid",
    "boardid",
    "shortname",
    "first_tradedate",
    "last_tradedate",
)

FACT_PRICE_COLUMNS: tuple[str, ...] = (
    "security_key",
    "date_key",
    "open",
    "low",
    "high",
    "close",
    "waprice",
    "value",
    "volume",
    "numtrades",
    "prev_close",
    "close_change",
    "close_change_pct",
    "source_code",
)

FACT_INDEX_COLUMNS: tuple[str, ...] = (
    "index_code",
    "date_key",
    "open",
    "low",
    "high",
    "close",
    "value",
    "prev_close",
    "close_change",
    "close_change_pct",
    "source_code",
)


def to_date_key(value: Any) -> int | None:
    """Преобразовать дату в целочисленный ключ ``YYYYMMDD``.

    Args:
        value: ``date``, ``datetime`` или строка ISO.

    Returns:
        Ключ вида 20240110 или ``None``, если значение не является датой.
    """
    if value is None or value == "":
        return None
    if isinstance(value, date):
        calendar_date = value
    else:
        try:
            calendar_date = date.fromisoformat(str(value))
        except ValueError:
            return None
    return calendar_date.year * 10000 + calendar_date.month * 100 + calendar_date.day


def build_date_dim_row(calendar_date: date, is_trading_day: bool = False) -> dict[str, Any]:
    """Собрать строку измерения даты для одного календарного дня.

    Args:
        calendar_date: календарная дата.
        is_trading_day: признак того, что в этот день были торги.

    Returns:
        Словарь с полями ``core.dim_date``.
    """
    iso_year, iso_week, _ = calendar_date.isocalendar()
    quarter = (calendar_date.month - 1) // 3 + 1
    return {
        "date_key": to_date_key(calendar_date),
        "calendar_date": calendar_date,
        "year": calendar_date.year,
        "quarter": quarter,
        "month": calendar_date.month,
        "day": calendar_date.day,
        "iso_week": iso_week,
        "day_of_week": calendar_date.weekday() + 1,
        "day_name": DAY_NAMES[calendar_date.weekday()],
        "is_weekend": calendar_date.weekday() >= 5,
        "is_trading_day": is_trading_day,
    }


def build_date_dim_rows(date_from: date, date_to: date) -> list[dict[str, Any]]:
    """Построить строки измерения даты за календарный период включительно.

    Args:
        date_from: первая дата периода.
        date_to: последняя дата периода.

    Returns:
        Список строк ``core.dim_date`` по возрастанию даты.
    """
    rows: list[dict[str, Any]] = []
    current = date_from
    while current <= date_to:
        rows.append(build_date_dim_row(current))
        current += timedelta(days=1)
    return rows


def to_decimal(value: Any) -> Decimal | None:
    """Привести значение к ``Decimal`` (или ``None``)."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def compute_close_change(
    close: Any, prev_close: Any
) -> tuple[Decimal | None, Decimal | None]:
    """Посчитать абсолютное и относительное изменение цены закрытия.

    Args:
        close: цена закрытия текущего дня.
        prev_close: цена закрытия предыдущего торгового дня.

    Returns:
        Кортеж ``(close_change, close_change_pct)``. Оба элемента ``None``,
        если не хватает данных; процент округляется до 4 знаков.
    """
    current = to_decimal(close)
    previous = to_decimal(prev_close)
    if current is None or previous is None or previous == 0:
        return None, None
    change = current - previous
    pct = (change / previous * 100).quantize(PCT_QUANT, rounding=ROUND_HALF_UP)
    return change, pct


def map_security_keys(
    rows: Iterable[Mapping[str, Any]],
    key_to_surrogate: Mapping[tuple[str, str], int],
) -> list[dict[str, Any]]:
    """Проставить строкам факта суррогатный ключ бумаги по естественному ключу.

    Args:
        rows: строки с полями ``secid`` и ``boardid``.
        key_to_surrogate: отображение ``(secid, boardid) -> security_key``.

    Returns:
        Новые словари с добавленным полем ``security_key``. Строки, для которых
        ключ не найден, пропускаются (их не должно быть при корректной звезде).
    """
    mapped: list[dict[str, Any]] = []
    for row in rows:
        natural_key = (str(row["secid"]), str(row["boardid"]))
        surrogate = key_to_surrogate.get(natural_key)
        if surrogate is None:
            continue
        enriched = dict(row)
        enriched["security_key"] = surrogate
        mapped.append(enriched)
    return mapped


def deduplicate_fact_rows(
    rows: Sequence[Mapping[str, Any]], key: Sequence[str] = ("security_key", "date_key")
) -> list[Mapping[str, Any]]:
    """Убрать повторы факта по составному ключу, сохранив первый.

    Args:
        rows: строки факта.
        key: имена колонок составного ключа.

    Returns:
        Список строк без повторов по ключу.
    """
    seen: set[tuple[Any, ...]] = set()
    unique: list[Mapping[str, Any]] = []
    for row in rows:
        row_key = tuple(row[name] for name in key)
        if row_key in seen:
            continue
        seen.add(row_key)
        unique.append(row)
    return unique