"""Правила расчёта метрик слоя mart (чистые функции, без БД).

Здесь нет импортов psycopg2/requests, поэтому все метрики и граничные случаи
(первый торговый день без prev_close, деление на ноль, отсутствие пары
«бумага - индекс») покрываются офлайн-юнит-тестами. Запись в БД делает
include/mart_builder.py.

Соглашения (зафиксированы явно, чтобы метрики не толковались двояко):
  * окно скользящих метрик включает текущий день и считается только при
    полном окне наблюдений, иначе значение NULL;
  * волатильность - выборочное стандартное отклонение (делитель n - 1)
    дневных доходностей внутри окна, в процентах;
  * накопленная доходность считается от первой доступной цены закрытия
    бумаги, поэтому первый день даёт 0;
  * если данных не хватает или знаменатель равен нулю, метрика равна NULL,
    а не 0 - «нет данных» и «нулевое изменение» это разные вещи.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any, Mapping, Sequence

from include.parsing import to_decimal

# Точность процентов и цен.
PCT_QUANT = Decimal("0.0001")
PRICE_QUANT = Decimal("0.000001")
HUNDRED = Decimal("100")

# Окна скользящих метрик (в торговых днях, включая текущий).
SHORT_WINDOW = 5
LONG_WINDOW = 20

STOCK_RETURNS_COLUMNS: tuple[str, ...] = (
    "security_key",
    "date_key",
    "secid",
    "boardid",
    "close",
    "prev_close",
    "daily_return_pct",
    "gap_pct",
    "amplitude_pct",
    "ma_5",
    "ma_20",
    "volatility_5_pct",
    "volatility_20_pct",
    "cumulative_return_pct",
)

INDEX_COMPARE_COLUMNS: tuple[str, ...] = (
    "index_code",
    "security_key",
    "date_key",
    "secid",
    "stock_return_pct",
    "index_return_pct",
    "excess_return_pct",
)

DAILY_MOVERS_COLUMNS: tuple[str, ...] = (
    "date_key",
    "security_key",
    "secid",
    "daily_return_pct",
    "return_rank",
    "is_best_of_day",
    "is_worst_of_day",
)


def pct_change(current: Any, base: Any) -> Decimal | None:
    """Посчитать относительное изменение ``(current - base) / base * 100``.

    Args:
        current: текущее значение.
        base: базовое значение (знаменатель).

    Returns:
        Процент с точностью 4 знака или ``None``, если значений нет либо
        базовое значение равно нулю (деление на ноль невозможно).
    """
    now = to_decimal(current)
    previous = to_decimal(base)
    if now is None or previous is None or previous == 0:
        return None
    return ((now - previous) / previous * HUNDRED).quantize(
        PCT_QUANT, rounding=ROUND_HALF_UP
    )


def daily_return_pct(close: Any, prev_close: Any) -> Decimal | None:
    """Дневная доходность: изменение close к предыдущему закрытию, %."""
    return pct_change(close, prev_close)


def gap_pct(open_price: Any, prev_close: Any) -> Decimal | None:
    """Гэп открытия: отклонение open от предыдущего закрытия, %."""
    return pct_change(open_price, prev_close)


def amplitude_pct(high: Any, low: Any, prev_close: Any) -> Decimal | None:
    """Размах дня (high - low) в процентах от предыдущего закрытия."""
    top = to_decimal(high)
    bottom = to_decimal(low)
    previous = to_decimal(prev_close)
    if top is None or bottom is None or previous is None or previous == 0:
        return None
    return ((top - bottom) / previous * HUNDRED).quantize(
        PCT_QUANT, rounding=ROUND_HALF_UP
    )


def rolling_mean(values: Sequence[Any], window: int) -> list[Decimal | None]:
    """Скользящее среднее по окну (включая текущий элемент).

    Args:
        values: последовательность значений (например, close по дням).
        window: размер окна в элементах.

    Returns:
        Список той же длины. Первые ``window - 1`` элементов - ``None``,
        а также ``None``, если внутри окна есть пропуск.
    """
    result: list[Decimal | None] = []
    numbers = [to_decimal(value) for value in values]
    for index in range(len(numbers)):
        if index + 1 < window:
            result.append(None)
            continue
        chunk = numbers[index - window + 1 : index + 1]
        if any(value is None for value in chunk):
            result.append(None)
            continue
        total = sum(chunk)  # type: ignore[arg-type]
        result.append((total / window).quantize(PRICE_QUANT, rounding=ROUND_HALF_UP))
    return result


def rolling_stddev(values: Sequence[Any], window: int) -> list[Decimal | None]:
    """Выборочное стандартное отклонение по окну (делитель ``n - 1``).

    Args:
        values: последовательность значений (например, дневные доходности).
        window: размер окна в элементах.

    Returns:
        Список той же длины. ``None``, пока окно не заполнено, если внутри
        окна есть пропуск или в окне меньше двух значений.
    """
    result: list[Decimal | None] = []
    numbers = [to_decimal(value) for value in values]
    for index in range(len(numbers)):
        if index + 1 < window:
            result.append(None)
            continue
        chunk = numbers[index - window + 1 : index + 1]
        if any(value is None for value in chunk) or len(chunk) < 2:
            result.append(None)
            continue
        numbers_only = [value for value in chunk if value is not None]
        mean = sum(numbers_only) / len(numbers_only)
        variance = sum((value - mean) ** 2 for value in numbers_only) / (
            len(numbers_only) - 1
        )
        with localcontext() as context:
            context.prec = 28
            deviation = variance.sqrt()
        result.append(deviation.quantize(PCT_QUANT, rounding=ROUND_HALF_UP))
    return result


def cumulative_return_pct(closes: Sequence[Any]) -> list[Decimal | None]:
    """Накопленная доходность от первой доступной цены закрытия, %.

    Args:
        closes: цены закрытия по возрастанию даты.

    Returns:
        Список той же длины. База - первая доступная цена закрытия; базовый день
        даёт ``0``. Если базовой цены нет или она равна нулю (некорректная цена),
        все элементы равны ``None``.
    """
    numbers = [to_decimal(value) for value in closes]
    base: Decimal | None = None
    for value in numbers:
        if value is not None:
            base = value
            break
    if base is None or base == 0:
        return [None] * len(numbers)
    return [
        None
        if value is None
        else ((value - base) / base * HUNDRED).quantize(
            PCT_QUANT, rounding=ROUND_HALF_UP
        )
        for value in numbers
    ]


def compute_stock_metrics(series: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Посчитать метрики витрины ``mart.stock_returns`` для одной бумаги.

    Args:
        series: строки одной бумаги по возрастанию даты; ожидаются поля
            ``close``, ``open``, ``high``, ``low``, ``prev_close``.

    Returns:
        Список словарей метрик в том же порядке, что и входные строки.
    """
    closes = [row.get("close") for row in series]
    returns = [daily_return_pct(row.get("close"), row.get("prev_close")) for row in series]
    ma_short = rolling_mean(closes, SHORT_WINDOW)
    ma_long = rolling_mean(closes, LONG_WINDOW)
    volatility_short = rolling_stddev(returns, SHORT_WINDOW)
    volatility_long = rolling_stddev(returns, LONG_WINDOW)
    cumulative = cumulative_return_pct(closes)

    metrics: list[dict[str, Any]] = []
    for index, row in enumerate(series):
        metrics.append(
            {
                "close": to_decimal(row.get("close")),
                "prev_close": to_decimal(row.get("prev_close")),
                "daily_return_pct": returns[index],
                "gap_pct": gap_pct(row.get("open"), row.get("prev_close")),
                "amplitude_pct": amplitude_pct(
                    row.get("high"), row.get("low"), row.get("prev_close")
                ),
                "ma_5": ma_short[index],
                "ma_20": ma_long[index],
                "volatility_5_pct": volatility_short[index],
                "volatility_20_pct": volatility_long[index],
                "cumulative_return_pct": cumulative[index],
            }
        )
    return metrics


def compute_excess_return(
    stock_return_pct: Any, index_return_pct: Any
) -> Decimal | None:
    """Превышение доходности бумаги над индексом, процентные пункты.

    Args:
        stock_return_pct: дневная доходность бумаги, %.
        index_return_pct: дневная доходность индекса, %.

    Returns:
        Разница в процентных пунктах или ``None``, если хотя бы одна
        доходность неизвестна.
    """
    stock = to_decimal(stock_return_pct)
    index = to_decimal(index_return_pct)
    if stock is None or index is None:
        return None
    return (stock - index).quantize(PCT_QUANT, rounding=ROUND_HALF_UP)


def rank_returns(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Ранжировать бумаги по дневной доходности (1 = лучший результат).

    Ранг считается как «1 + число бумаг со строго большей доходностью»,
    поэтому равные доходности получают одинаковый ранг, а несколько бумаг
    могут одновременно быть лучшими или худшими за день.

    Args:
        rows: строки одного торгового дня с полями ``security_key``, ``secid``
            и ``daily_return_pct``.

    Returns:
        Список словарей с сохранёнными ``security_key``/``secid`` и
        добавленными ``return_rank``, ``is_best_of_day``, ``is_worst_of_day``.
        Строки без доходности пропускаются.
    """
    ranked = [row for row in rows if to_decimal(row.get("daily_return_pct")) is not None]
    if not ranked:
        return []

    values = [to_decimal(row["daily_return_pct"]) for row in ranked]
    max_rank = 0
    entries: list[dict[str, Any]] = []
    for row, value in zip(ranked, values):
        rank = 1 + sum(1 for other in values if other > value)  # type: ignore[operator]
        max_rank = max(max_rank, rank)
        entries.append(
            {
                "security_key": row.get("security_key"),
                "secid": row.get("secid"),
                "daily_return_pct": value,
                "return_rank": rank,
            }
        )

    for entry in entries:
        entry["is_best_of_day"] = entry["return_rank"] == 1
        entry["is_worst_of_day"] = entry["return_rank"] == max_rank
    return entries