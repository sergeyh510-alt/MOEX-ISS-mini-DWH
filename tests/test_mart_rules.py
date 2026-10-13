"""Оффлайн-тесты метрик слоя mart (граничные случаи включены).

Ни сети, ни БД: проверяются чистые функции из include/mart_rules.py.

Запуск:

    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
"""

from __future__ import annotations

from decimal import Decimal

from include import mart_rules


def _series(closes: list[float], prev: list[float | None] | None = None) -> list[dict]:
    """Собрать ряд строк одной бумаги (open/high/low/close/prev_close)."""
    rows = []
    for index, close in enumerate(closes):
        previous = None if prev is None else prev[index]
        rows.append(
            {
                "close": Decimal(str(close)),
                "open": Decimal(str(close * 0.995)),
                "high": Decimal(str(close * 1.01)),
                "low": Decimal(str(close * 0.99)),
                "prev_close": None if previous is None else Decimal(str(previous)),
            }
        )
    return rows


def test_pct_change_basic() -> None:
    """Процент изменения считается от базового значения."""
    assert mart_rules.pct_change(110, 100) == Decimal("10.0000")
    assert mart_rules.pct_change(90, 100) == Decimal("-10.0000")


def test_pct_change_division_by_zero_returns_none() -> None:
    """Деление на ноль не выполняется: метрика NULL, а не бесконечность."""
    assert mart_rules.pct_change(100, 0) is None
    assert mart_rules.pct_change(100, Decimal("0.0")) is None


def test_pct_change_missing_values() -> None:
    """Отсутствие любого из значений даёт NULL."""
    assert mart_rules.pct_change(None, 100) is None
    assert mart_rules.pct_change(100, None) is None
    assert mart_rules.pct_change("", 100) is None


def test_daily_return_first_day_without_prev_close() -> None:
    """Первый торговый день без prev_close: доходность NULL, а не 0."""
    assert mart_rules.daily_return_pct(Decimal("280"), None) is None


def test_gap_and_amplitude() -> None:
    """Гэп открытия и размах дня считаются к prev_close."""
    assert mart_rules.gap_pct(Decimal("101"), Decimal("100")) == Decimal("1.0000")
    assert mart_rules.gap_pct(Decimal("101"), None) is None
    assert mart_rules.amplitude_pct(
        Decimal("105"), Decimal("95"), Decimal("100")
    ) == Decimal("10.0000")
    assert mart_rules.amplitude_pct(Decimal("105"), Decimal("95"), None) is None


def test_rolling_mean_fills_window() -> None:
    """Среднее за 5 дней появляется только при полном окне."""
    values = [10, 20, 30, 40, 50, 60]
    result = mart_rules.rolling_mean(values, 5)

    assert result[0] is None and result[3] is None
    assert result[4] == Decimal("30.000000")
    assert result[5] == Decimal("40.000000")


def test_rolling_mean_window_larger_than_data() -> None:
    """Если данных меньше окна, все значения NULL."""
    assert mart_rules.rolling_mean([1, 2, 3], 20) == [None, None, None]


def test_rolling_mean_with_gap_returns_none() -> None:
    """Пропуск внутри окна делает среднее неизвестным."""
    values = [10, None, 30, 40, 50]
    result = mart_rules.rolling_mean(values, 3)

    assert result[2] is None  # окно [10, None, 30] содержит пропуск
    assert result[4] == Decimal("40.000000")  # окно [30, 40, 50] чистое


def test_rolling_stddev_sample_denominator() -> None:
    """Волатильность - выборочное СКО (делитель n - 1)."""
    result = mart_rules.rolling_stddev([1, 2, 3], 3)

    assert result[0] is None and result[1] is None
    assert result[2] == Decimal("1.0000")


def test_rolling_stddev_constant_series_is_zero() -> None:
    """Постоянный ряд даёт нулевую волатильность (не NULL)."""
    result = mart_rules.rolling_stddev([5, 5, 5], 3)

    assert result[2] == Decimal("0.0000")


def test_rolling_stddev_with_gap() -> None:
    """Пропуск в окне обнуляет волатильность в NULL."""
    assert mart_rules.rolling_stddev([1, None, 3], 3)[2] is None


def test_cumulative_return_starts_at_zero() -> None:
    """Накопленная доходность отсчитывается от первой цены и начинается с 0."""
    result = mart_rules.cumulative_return_pct([100, 110, 90])

    assert result[0] == Decimal("0.0000")
    assert result[1] == Decimal("10.0000")
    assert result[2] == Decimal("-10.0000")


def test_cumulative_return_skips_missing_base() -> None:
    """Базой становится первая доступная ненулевая цена."""
    result = mart_rules.cumulative_return_pct([None, 50, 100])

    assert result[0] is None
    assert result[1] == Decimal("0.0000")
    assert result[2] == Decimal("100.0000")


def test_cumulative_return_without_valid_base() -> None:
    """Без базовой цены или при нулевой базе метрика не определена."""
    assert mart_rules.cumulative_return_pct([None, None]) == [None, None]
    assert mart_rules.cumulative_return_pct([0, 10]) == [None, None]
    assert mart_rules.cumulative_return_pct([]) == []


def test_cumulative_return_zero_in_the_middle_is_defined() -> None:
    """Нулевая цена позже базы даёт -100%, а не NULL."""
    result = mart_rules.cumulative_return_pct([100, 0, 50])

    assert result[0] == Decimal("0.0000")
    assert result[1] == Decimal("-100.0000")
    assert result[2] == Decimal("-50.0000")


def test_compute_stock_metrics_short_series() -> None:
    """Короткий ряд: дневные метрики есть, скользящие окна пусты."""
    series = _series([100, 110], prev=[None, 100])
    metrics = mart_rules.compute_stock_metrics(series)

    assert metrics[0]["daily_return_pct"] is None
    assert metrics[1]["daily_return_pct"] == Decimal("10.0000")
    assert metrics[1]["ma_5"] is None
    assert metrics[1]["volatility_5_pct"] is None
    assert metrics[0]["cumulative_return_pct"] == Decimal("0.0000")


def test_compute_stock_metrics_full_series() -> None:
    """На длинном ряду появляются окна 5 и 20, а волатильность ждёт данных."""
    closes = [100 + index for index in range(26)]
    prev = [None] + [Decimal(str(100 + index)) for index in range(25)]
    metrics = mart_rules.compute_stock_metrics(_series(closes, prev))

    assert metrics[4]["ma_5"] is not None
    assert metrics[19]["ma_20"] is not None
    assert metrics[3]["ma_20"] is None
    # Волатильность считается по доходностям: у первого дня prev_close нет,
    # поэтому окно из 20 доходностей закрывается только на 21-м торговом дне.
    assert metrics[19]["volatility_20_pct"] is None
    assert metrics[20]["volatility_20_pct"] is not None


def test_compute_excess_return() -> None:
    """Превышение доходности бумаги над индексом - в процентных пунктах."""
    assert mart_rules.compute_excess_return(
        Decimal("2.5"), Decimal("1.0")
    ) == Decimal("1.5000")
    assert mart_rules.compute_excess_return(Decimal("-1"), Decimal("1")) == Decimal(
        "-2.0000"
    )


def test_compute_excess_return_missing_pair() -> None:
    """Нет пары «бумага - индекс» (или доходности неизвестны) - NULL."""
    assert mart_rules.compute_excess_return(None, Decimal("1.0")) is None
    assert mart_rules.compute_excess_return(Decimal("1.0"), None) is None


def test_rank_returns_orders_and_flags() -> None:
    """Ранг: 1 - лучший, максимум - худший; ключи бумаг сохранены."""
    rows = [
        {"security_key": 1, "secid": "AAA", "daily_return_pct": Decimal("1.0")},
        {"security_key": 2, "secid": "BBB", "daily_return_pct": Decimal("-2.0")},
        {"security_key": 3, "secid": "CCC", "daily_return_pct": Decimal("0.5")},
    ]
    ranked = mart_rules.rank_returns(rows)
    by_secid = {entry["secid"]: entry for entry in ranked}

    assert by_secid["AAA"]["return_rank"] == 1
    assert by_secid["CCC"]["return_rank"] == 2
    assert by_secid["BBB"]["return_rank"] == 3
    assert by_secid["AAA"]["is_best_of_day"] is True
    assert by_secid["BBB"]["is_worst_of_day"] is True
    assert by_secid["CCC"]["is_best_of_day"] is False
    assert by_secid["AAA"]["security_key"] == 1


def test_rank_returns_ties_share_rank() -> None:
    """Равные доходности получают одинаковый ранг и оба флага лучших."""
    rows = [
        {"security_key": 1, "secid": "AAA", "daily_return_pct": Decimal("1.0")},
        {"security_key": 2, "secid": "BBB", "daily_return_pct": Decimal("1.0")},
        {"security_key": 3, "secid": "CCC", "daily_return_pct": Decimal("-1.0")},
    ]
    ranked = mart_rules.rank_returns(rows)
    by_secid = {entry["secid"]: entry for entry in ranked}

    assert by_secid["AAA"]["return_rank"] == 1
    assert by_secid["BBB"]["return_rank"] == 1
    assert by_secid["AAA"]["is_best_of_day"] and by_secid["BBB"]["is_best_of_day"]
    assert by_secid["CCC"]["return_rank"] == 3
    assert by_secid["CCC"]["is_worst_of_day"] is True


def test_rank_returns_skips_rows_without_return() -> None:
    """Строки без доходности (первый день) не участвуют в ранжировании."""
    rows = [
        {"security_key": 1, "secid": "AAA", "daily_return_pct": None},
        {"security_key": 2, "secid": "BBB", "daily_return_pct": Decimal("1.0")},
    ]
    ranked = mart_rules.rank_returns(rows)

    assert len(ranked) == 1
    assert ranked[0]["secid"] == "BBB"


def test_rank_returns_empty_input() -> None:
    """Пустой вход даёт пустой результат, а не ошибку."""
    assert mart_rules.rank_returns([]) == []