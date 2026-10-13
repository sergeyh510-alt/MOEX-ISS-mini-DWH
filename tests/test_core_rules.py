"""Оффлайн-тесты правил слоя core: измерение даты, ключи, расчёт изменений.

Ни сети, ни БД: проверяются чистые функции из include/core_rules.py.

Запуск:

    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from include import core_rules


def test_to_date_key_handles_formats() -> None:
    """Ключ даты собирается из date, строки ISO; мусор даёт None."""
    assert core_rules.to_date_key(date(2024, 1, 10)) == 20240110
    assert core_rules.to_date_key("2024-12-31") == 20241231
    assert core_rules.to_date_key(None) is None
    assert core_rules.to_date_key("не дата") is None


def test_build_date_dim_row_fields() -> None:
    """Календарная строка содержит год, квартал, неделю, выходной признак."""
    row = core_rules.build_date_dim_row(date(2024, 1, 10), is_trading_day=True)

    assert row["date_key"] == 20240110
    assert row["year"] == 2024
    assert row["quarter"] == 1
    assert row["month"] == 1
    assert row["day"] == 10
    assert row["iso_week"] == 2
    assert row["day_of_week"] == 3
    assert row["day_name"] == "Ср"
    assert row["is_weekend"] is False
    assert row["is_trading_day"] is True


def test_build_date_dim_row_weekend_and_quarter() -> None:
    """Суббота - выходной; декабрь относится к 4 кварталу."""
    saturday = core_rules.build_date_dim_row(date(2024, 1, 13))
    december = core_rules.build_date_dim_row(date(2024, 12, 1))

    assert saturday["is_weekend"] is True
    assert saturday["day_name"] == "Сб"
    assert saturday["day_of_week"] == 6
    assert december["quarter"] == 4


def test_build_date_dim_rows_is_inclusive() -> None:
    """Период включает обе границы и идёт по возрастанию."""
    rows = core_rules.build_date_dim_rows(date(2024, 1, 9), date(2024, 1, 11))

    assert [row["date_key"] for row in rows] == [20240109, 20240110, 20240111]


def test_compute_close_change() -> None:
    """Абсолютное и относительное изменение считаются от prev_close."""
    change, pct = core_rules.compute_close_change(Decimal("110"), Decimal("100"))

    assert change == Decimal("10")
    assert pct == Decimal("10.0000")


def test_compute_close_change_rounding() -> None:
    """Процент округляется до 4 знаков (ROUND_HALF_UP)."""
    change, pct = core_rules.compute_close_change(Decimal("100"), Decimal("3"))

    assert change is not None
    assert pct == Decimal("3233.3333")


def test_compute_close_change_without_prev() -> None:
    """Без предыдущего дня или при нуле изменения не считаются."""
    assert core_rules.compute_close_change(Decimal("100"), None) == (None, None)
    assert core_rules.compute_close_change(Decimal("100"), Decimal("0")) == (None, None)
    assert core_rules.compute_close_change(None, Decimal("100")) == (None, None)


def test_map_security_keys_resolves_surrogate() -> None:
    """Естественный ключ (secid, boardid) превращается в суррогатный."""
    rows = [
        {"secid": "SBER", "boardid": "TQBR", "close": Decimal("280")},
        {"secid": "GAZP", "boardid": "TQBR", "close": Decimal("170")},
    ]
    key_map = {("SBER", "TQBR"): 1, ("GAZP", "TQBR"): 2}

    mapped = core_rules.map_security_keys(rows, key_map)

    assert [row["security_key"] for row in mapped] == [1, 2]
    assert mapped[0]["close"] == Decimal("280")


def test_map_security_keys_skips_unknown() -> None:
    """Строка без измерения в справочнике не попадает в факт."""
    rows = [{"secid": "UNKNOWN", "boardid": "TQBR"}]
    mapped = core_rules.map_security_keys(rows, {("SBER", "TQBR"): 1})

    assert mapped == []


def test_deduplicate_fact_rows_keeps_first() -> None:
    """Повторы факта по составному ключу снимаются, первый побеждает."""
    rows = [
        {"security_key": 1, "date_key": 20240110, "close": Decimal("1")},
        {"security_key": 1, "date_key": 20240111, "close": Decimal("2")},
        {"security_key": 1, "date_key": 20240110, "close": Decimal("3")},
    ]

    unique = core_rules.deduplicate_fact_rows(rows)

    assert len(unique) == 2
    assert unique[0]["close"] == Decimal("1")
    assert unique[1]["close"] == Decimal("2")


def test_fact_columns_contain_surrogate_keys() -> None:
    """Факт ссылается на измерения суррогатными ключами."""
    assert "security_key" in core_rules.FACT_PRICE_COLUMNS
    assert "date_key" in core_rules.FACT_PRICE_COLUMNS
    assert "secid" not in core_rules.FACT_PRICE_COLUMNS
    assert "date_key" in core_rules.DIM_DATE_COLUMNS
    assert "security_key" not in core_rules.DIM_SECURITY_COLUMNS