"""HTTP-клиент MOEX ISS API.

Документация: https://iss.moex.com/iss/reference/ (раздел «ISS API»).

Используемые эндпоинты дневной истории:

* акции по режиму торгов (борде):
  /iss/history/engines/stock/markets/shares/boards/{board}/securities/{secid}.json
* индекс:
  /iss/history/engines/stock/markets/index/securities/{index_code}.json

Формат ответа ISS для подобных методов:

    {
      "<block>": {"metadata": {...}, "columns": [...], "data": [[...], ...]},
      "<block>.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"],
                         "data": [[<start>, <total>, <page_size>]]}
    }

Оба блока (``history`` и ``history.cursor``) приходят в одном HTTP-ответе,
поэтому страница читается одним запросом. Листание длинных ответов делается
параметром ``start``; размер страницы задаётся ``limit``.
"""

from __future__ import annotations

from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from include.config import MoexConfig

# Коды ответа, при которых имеет смысл повторить запрос.
RETRY_STATUS_CODES: tuple[int, ...] = (429, 500, 502, 503, 504)


class MoexResponseError(RuntimeError):
    """Ответ ISS не содержит ожидаемого блока."""


class MoexIssClient:
    """Клиент MOEX ISS с таймаутами, ретраями и пагинацией."""

    def __init__(self, config: MoexConfig) -> None:
        """Создать клиент по параметрам конфигурации."""
        self._config = config
        self._session = self._build_session(config)

    @staticmethod
    def _build_session(config: MoexConfig) -> requests.Session:
        """Собрать HTTP-сессию с ретраями и экспоненциальным backoff."""
        retry = Retry(
            total=config.max_retries,
            connect=config.max_retries,
            read=config.max_retries,
            status=config.max_retries,
            backoff_factor=config.backoff_factor,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_payload(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """Запросить эндпоинт и вернуть полный JSON-ответ ISS.

        Args:
            path: путь эндпоинта, например ``/iss/history/.../SBER.json``.
            params: query-параметры запроса.

        Returns:
            Разобранный JSON-ответ с блоками данных и курсором.
        """
        url = self._config.base_url.rstrip("/") + path
        request_params = dict(params)
        request_params["iss.meta"] = "off"
        request_params["limit"] = self._config.page_size
        response = self._session.get(
            url, params=request_params, timeout=self._config.timeout_seconds
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def extract_rows(
        payload: dict[str, Any], block: str
    ) -> list[dict[str, Any]]:
        """Превратить блок ``columns``/``data`` в список словарей.

        Args:
            payload: полный JSON-ответ ISS.
            block: имя блока с данными, например ``history``.

        Returns:
            Строки блока как словари ``{колонка: значение}``.

        Raises:
            MoexResponseError: если блока нет в ответе.
        """
        if block not in payload:
            raise MoexResponseError(
                f"В ответе нет блока '{block}' "
                f"(получены: {sorted(payload.keys())})"
            )
        columns: list[str] = payload[block].get("columns", [])
        return [dict(zip(columns, row)) for row in payload[block].get("data", [])]

    @staticmethod
    def extract_total(payload: dict[str, Any], block: str) -> int | None:
        """Взять суммарное число строк из блока ``<block>.cursor``.

        Returns:
            Значение ``TOTAL`` или ``None``, если курсора в ответе нет.
        """
        cursor = payload.get(f"{block}.cursor")
        if not cursor:
            return None
        columns: list[str] = cursor.get("columns", [])
        data: list[list[Any]] = cursor.get("data", [])
        if not data or "TOTAL" not in columns:
            return None
        return int(data[0][columns.index("TOTAL")])

    def _fetch_rows(self, path: str, params: dict[str, Any], block: str) -> Iterator[dict[str, Any]]:
        """Отдать строки блока постранично, листая параметром ``start``.

        Args:
            path: путь эндпоинта ISS.
            params: базовые query-параметры (без ``start``/``limit``).
            block: имя блока с данными.

        Yields:
            Строки блока в виде словарей.
        """
        start = 0
        total: int | None = None
        while True:
            payload = self.fetch_payload(path, {**params, "start": start})
            if total is None:
                total = self.extract_total(payload, block)
            rows = self.extract_rows(payload, block)
            for row in rows:
                yield row

            start += len(rows)
            if not rows or len(rows) < self._config.page_size:
                break
            if total is not None and start >= total:
                break

    def fetch_stock_history(
        self, secid: str, board: str, date_from: str, date_to: str
    ) -> Iterator[dict[str, Any]]:
        """Отдать дневную историю одной бумаги по борде за период.

        Args:
            secid: код бумаги, например ``SBER``.
            board: код режима торгов, например ``TQBR``.
            date_from: начало периода в формате ``YYYY-MM-DD``.
            date_to: конец периода в формате ``YYYY-MM-DD``.

        Yields:
            Строки истории (словари по колонкам ответа ISS).
        """
        path = (
            "/iss/history/engines/stock/markets/shares"
            f"/boards/{board}/securities/{secid}.json"
        )
        yield from self._fetch_rows(path, {"from": date_from, "till": date_to}, "history")

    def fetch_index_history(
        self, index_code: str, date_from: str, date_to: str
    ) -> Iterator[dict[str, Any]]:
        """Отдать дневную историю индекса за период.

        Args:
            index_code: код индекса, например ``IMOEX``.
            date_from: начало периода в формате ``YYYY-MM-DD``.
            date_to: конец периода в формате ``YYYY-MM-DD``.

        Yields:
            Строки истории индекса.
        """
        path = f"/iss/history/engines/stock/markets/index/securities/{index_code}.json"
        yield from self._fetch_rows(path, {"from": date_from, "till": date_to}, "history")