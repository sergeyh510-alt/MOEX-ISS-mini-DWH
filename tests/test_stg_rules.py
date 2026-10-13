"""Оффлайн-тесты правил трансформации raw -> stg.

Ни сети, ни БД: проверяются чистые функции из include/stg_rules.py и
include/batching.py.

Запуск:

    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from include import batching, stg_rules


def _raw_stock_row(**overrides: object) -> dict[str, object]:
    """Собрать корректную строку raw.moex_history для тестов."""
    row: dict[str, object] = {
        "boardid": "TQBR",
        "secid": "SBER",
        "tradedate": date(2024, 1, 10),
        "shortname": "Сбербанк",
        "numtrades": 12345,
        "value": Decimal("1000000.50"),
        "open": Decimal("280.10"),
        "low": Decimal("278.00"),
        "high": Decimal("285.40"),
        "close": Decimal("284.75"),
        "waprice": Decimal("282.30"),
        "volume": 5000,
    }
    row.update(overrides)
    return row


def test_valid_rows_pass_quality_checks() -> None:
    """Корректная строка не имеет причин отбраковки."""
    fields = stg_rules.to_stg_stock_row(_raw_stock_row())

    assert fields["is_valid"] is True
    assert fields["reject_reason"] is None
    assert fields["tradedate"] == date(2024, 1, 10)
    assert fields["close"] == Decimal("284.75")


def test_missing_prices_are_rejected() -> None:
    """Пропуск любой из OHLC-цен - причина отбраковки."""
    fields = stg_rules.to_stg_stock_row(_raw_stock_row(low=None))

    assert fields["is_valid"] is False
    assert stg_rules.REASON_PRICES_MISSING in fields["reject_reason"]


def test_non_positive_price_is_rejected() -> None:
    """Нулевая или отрицательная цена - причина отбраковки."""
    fields = stg_rules.to_stg_stock_row(_raw_stock_row(close=Decimal("0")))

    assert fields["is_valid"] is False
    assert stg_rules.REASON_PRICE_NOT_POSITIVE in fields["reject_reason"]


def test_inconsistent_high_low_is_rejected() -> None:
    """high < low и выход high/low за пределы open/close отбраковываются."""
    assert stg_rules.REASON_HIGH_LT_LOW in stg_rules.validate_prices(
        Decimal("100"), Decimal("120"), Decimal("110"), Decimal("105")
    )
    assert stg_rules.REASON_HIGH_LT_OPEN_CLOSE in stg_rules.validate_prices(
        Decimal("100"), Decimal("90"), Decimal("95"), Decimal("105")
    )
    assert stg_rules.REASON_LOW_GT_OPEN_CLOSE in stg_rules.validate_prices(
        Decimal("100"), Decimal("101"), Decimal("110"), Decimal("105")
    )


def test_negative_counters_are_rejected() -> None:
    """Отрицательные numtrades/volume отбраковываются, отсутствие - нет."""
    fields = stg_rules.to_stg_stock_row(_raw_stock_row(volume=-1))

    assert fields["is_valid"] is False
    assert stg_rules.REASON_VOLUME_NEGATIVE in fields["reject_reason"]
    assert stg_rules.validate_counts(None, None) == []


def test_multiple_reasons_are_joined() -> None:
    """Несколько нарушений собираются в одну строку причин."""
    fields = stg_rules.to_stg_stock_row(
        _raw_stock_row(close=Decimal("0"), numtrades=-5)
    )

    assert fields["is_valid"] is False
    assert stg_rules.REASON_PRICE_NOT_POSITIVE in fields["reject_reason"]
    assert stg_rules.REASON_NUMTRADES_NEGATIVE in fields["reject_reason"]
    assert "; " in fields["reject_reason"]


def test_index_row_validity() -> None:
    """Правила для индекса те же: пропуск цен и неположительные значения."""
    valid = stg_rules.to_stg_index_row(
        {
            "index_code": "IMOEX",
            "tradedate": date(2024, 1, 10),
            "open": Decimal("3100.5"),
            "low": Decimal("3090.0"),
            "high": Decimal("3125.0"),
            "close": Decimal("3120.0"),
            "value": Decimal("123.45"),
        }
    )
    assert valid["is_valid"] is True

    invalid = stg_rules.to_stg_index_row(
        {
            "index_code": "IMOEX",
            "tradedate": date(2024, 1, 10),
            "open": None,
            "low": Decimal("3090.0"),
            "high": Decimal("3125.0"),
            "close": Decimal("3120.0"),
        }
    )
    assert invalid["is_valid"] is False


def test_security_row_from_aggregate() -> None:
    """Строка справочника собирается из агрегата raw."""
    fields = stg_rules.to_stg_security_row(
        {
            "secid": "SBER",
            "boardid": "TQBR",
            "shortname": "Сбербанк",
            "first_tradedate": date(2024, 1, 9),
            "last_tradedate": date(2024, 1, 23),
        }
    )

    assert fields["secid"] == "SBER"
    assert fields["first_tradedate"] == date(2024, 1, 9)
    assert fields["last_tradedate"] == date(2024, 1, 23)


def test_upsert_query_is_idempotent_by_key() -> None:
    """SQL upsert содержит конфликт по ключу и обновление updated_at."""
    query = batching.build_upsert_query(
        "stg.stock_history",
        ["boardid", "secid", "tradedate", "close"],
        ["boardid", "secid", "tradedate"],
        ["close"],
    )

    assert query.startswith("INSERT INTO stg.stock_history")
    assert "ON CONFLICT (boardid, secid, tradedate) DO UPDATE SET" in query
    assert "close = EXCLUDED.close" in query
    assert "updated_at = now()" in query
    assert query.endswith("RETURNING 1")


def test_insert_query_does_nothing_on_conflict() -> None:
    """SQL вставки raw игнорирует конфликт по ключу."""
    query = batching.build_insert_query(
        "raw.moex_history", ["boardid", "secid", "tradedate"], ["boardid", "secid", "tradedate"]
    )

    assert "ON CONFLICT (boardid, secid, tradedate) DO NOTHING" in query
    assert query.endswith("RETURNING 1")