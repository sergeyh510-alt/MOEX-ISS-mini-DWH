"""Оффлайн-тесты разбора строк ISS и утилит пачечной вставки.

Не требуют ни сети, ни БД: проверяются чистые функции из include/parsing.py
и include/batching.py.

Запуск:

    docker compose run --rm airflow-scheduler python -m pytest /opt/airflow/tests -q
"""

from __future__ import annotations

from datetime import date

from include import batching, parsing

STOCK_ROW: dict[str, object] = {
    "TRADEDATE": "2024-01-10",
    "SECID": "SBER",
    "SHORTNAME": "Сбербанк",
    "BOARDID": "TQBR",
    "NUMTRADES": 12345,
    "VALUE": 1000000.5,
    "OPEN": "280.10",
    "LOW": "278.00",
    "HIGH": "285.40",
    "CLOSE": "284.75",
    "WAPRICE": "282.30",
    "VOLUME": 5000,
}


def test_build_stock_history_row_parses_types() -> None:
    """Разбор строки акций: дата, текст, int и Decimal."""
    fields = parsing.build_stock_history_row(STOCK_ROW)

    assert fields is not None
    assert fields["boardid"] == "TQBR"
    assert fields["secid"] == "SBER"
    assert fields["tradedate"] == date(2024, 1, 10)
    assert fields["shortname"] == "Сбербанк"
    assert fields["numtrades"] == 12345
    assert fields["volume"] == 5000
    assert str(fields["close"]) == "284.75"
    assert str(fields["value"]) == "1000000.5"


def test_build_stock_history_row_requires_natural_key() -> None:
    """Без SECID/BOARDID/TRADEDATE строка не грузится."""
    assert parsing.build_stock_history_row({"SECID": "SBER"}) is None
    assert parsing.build_stock_history_row({**STOCK_ROW, "TRADEDATE": ""}) is None


def test_build_stock_history_row_handles_empty_numbers() -> None:
    """Пустые числовые поля превращаются в None, а не в ошибку."""
    fields = parsing.build_stock_history_row(
        {**STOCK_ROW, "VALUE": "", "CLOSE": None, "VOLUME": ""}
    )

    assert fields is not None
    assert fields["value"] is None
    assert fields["close"] is None
    assert fields["volume"] is None


def test_build_index_history_row() -> None:
    """Разбор строки индекса с подстановкой кода индекса."""
    fields = parsing.build_index_history_row(
        {"TRADEDATE": "2024-01-10", "OPEN": 3100.5, "CLOSE": 3120.0}, "IMOEX"
    )

    assert fields is not None
    assert fields["index_code"] == "IMOEX"
    assert fields["tradedate"] == date(2024, 1, 10)
    assert fields["close"] is not None


def test_deduplicate_rows_keeps_first_occurrence() -> None:
    """Повторы по бизнес-ключу внутри пачки снимаются."""
    rows = [
        ("TQBR", "SBER", "2024-01-10", 1),
        ("TQBR", "SBER", "2024-01-11", 2),
        ("TQBR", "SBER", "2024-01-10", 3),
    ]

    unique = batching.deduplicate_rows(rows, key_positions=[0, 1, 2])

    assert len(unique) == 2
    assert unique[0][3] == 1
    assert unique[1][3] == 2


def test_key_indexes_and_count_returned() -> None:
    """Позиции ключа считаются по колонкам, а RETURNING - по форме результата."""
    assert batching.key_indexes(["boardid", "secid", "tradedate"], ["secid", "boardid"]) == [1, 0]
    assert batching.count_returned([]) == 0
    assert batching.count_returned([(1,), (1,)]) == 2
    assert batching.count_returned([[(1,), (1,)], [(1,)]]) == 3