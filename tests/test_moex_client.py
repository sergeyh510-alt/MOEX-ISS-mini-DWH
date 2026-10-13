"""Оффлайн-тесты клиента MOEX ISS: пагинация и разбор строк.

Сеть не используется: HTTP-сессия клиента подменяется на фейковую.
Запуск (из контейнера Airflow, где установлены зависимости):

    docker compose run --rm airflow-scheduler python -m pytest /opt/airflow/tests -q
"""

from __future__ import annotations

from typing import Any

from include.config import MoexConfig
from include.moex_client import MoexIssClient

HISTORY_COLUMNS: list[str] = [
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

CURSOR_COLUMNS: list[str] = ["INDEX", "TOTAL", "PAGESIZE"]


def _make_row(tradedate: str, secid: str, close: float) -> list[Any]:
    """Собрать строку ответа ISS в порядке ``HISTORY_COLUMNS``."""
    return [
        tradedate,
        secid,
        "Тест",
        "TQBR",
        10,
        1000.0,
        close - 1,
        close - 2,
        close + 1,
        close,
        close,
        100,
    ]


class FakeResponse:
    """Минимальная заглушка ответа ``requests.Response``."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        """Имитация успешного статуса."""

    def json(self) -> dict[str, Any]:
        """Вернуть JSON-полезную нагрузку."""
        return self._payload


class FakeSession:
    """Фейковая сессия: отдаёт страницы по параметру ``start``.

    Как и реальный ISS, возвращает блок данных и курсор в одном ответе.
    """

    def __init__(self, pages: dict[int, list[list[Any]]], total: int) -> None:
        self._pages = pages
        self._total = total
        self.requests: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float) -> FakeResponse:
        """Отдать одну страницу ответа ISS."""
        self.requests.append({"url": url, "params": dict(params), "timeout": timeout})
        start = int(params.get("start", 0))
        rows = self._pages.get(start, [])
        return FakeResponse(
            {
                "history": {"columns": HISTORY_COLUMNS, "data": rows},
                "history.cursor": {
                    "columns": CURSOR_COLUMNS,
                    "data": [[start, self._total, len(rows)]],
                },
            }
        )


def _client_with(session: FakeSession, page_size: int) -> MoexIssClient:
    """Создать клиент с подменённой HTTP-сессией."""
    config = MoexConfig(
        base_url="https://iss.moex.com",
        page_size=page_size,
        timeout_seconds=5.0,
        max_retries=0,
        backoff_factor=0.1,
    )
    client = MoexIssClient(config)
    client._session = session  # type: ignore[assignment]
    return client


def test_pagination_returns_all_rows() -> None:
    """Пагинация по ``start`` должна вернуть все строки ответа."""
    pages = {
        0: [_make_row("2024-01-09", "SBER", 280.0), _make_row("2024-01-10", "SBER", 281.0)],
        2: [_make_row("2024-01-11", "SBER", 282.0), _make_row("2024-01-12", "SBER", 283.0)],
    }
    session = FakeSession(pages, total=4)
    client = _client_with(session, page_size=2)

    rows = list(client.fetch_stock_history("SBER", "TQBR", "2024-01-09", "2024-01-12"))

    assert len(rows) == 4
    assert rows[0]["SECID"] == "SBER"
    assert rows[-1]["TRADEDATE"] == "2024-01-12"
    assert [request["params"]["start"] for request in session.requests] == [0, 2]


def test_stops_when_page_smaller_than_page_size() -> None:
    """Короткая страница означает конец данных: лишних запросов нет."""
    pages = {0: [_make_row("2024-01-09", "GAZP", 170.0)]}
    session = FakeSession(pages, total=1)
    client = _client_with(session, page_size=2)

    rows = list(client.fetch_stock_history("GAZP", "TQBR", "2024-01-09", "2024-01-09"))

    assert len(rows) == 1
    assert len(session.requests) == 1


def test_extract_rows_unknown_block_raises() -> None:
    """Отсутствие ожидаемого блока в ответе - явная ошибка."""
    from include.moex_client import MoexResponseError

    try:
        MoexIssClient.extract_rows({"other": {}}, "history")
    except MoexResponseError:
        return
    raise AssertionError("ожидалось MoexResponseError")


def test_request_path_and_params() -> None:
    """Путь и параметры запроса должны соответствовать документации ISS."""
    session = FakeSession({0: [_make_row("2024-01-09", "LKOH", 6500.0)]}, total=1)
    client = _client_with(session, page_size=100)

    list(client.fetch_stock_history("LKOH", "TQBR", "2024-01-09", "2024-01-10"))

    request = session.requests[0]
    assert request["url"].endswith(
        "/iss/history/engines/stock/markets/shares/boards/TQBR/securities/LKOH.json"
    )
    assert request["params"]["from"] == "2024-01-09"
    assert request["params"]["till"] == "2024-01-10"
    assert request["params"]["limit"] == 100
    assert request["params"]["iss.meta"] == "off"