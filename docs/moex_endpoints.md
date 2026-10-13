# Эндпоинты MOEX ISS, используемые в проекте

Источник: ISS API Московской биржи, https://iss.moex.com/iss/reference/

Общий формат ответа ISS (все перечисленные ниже методы):

```json
{
  "<block>": {
    "metadata": { "<колонка>": {"type": "...", "bytes": 0, "max_size": 0} },
    "columns": ["COL1", "COL2", "..."],
    "data": [["значение", 1, "..."], ["...", "...", "..."]]
  },
  "<block>.cursor": {
    "columns": ["INDEX", "TOTAL", "PAGESIZE"],
    "data": [[0, 12345, 100]]
  }
}
```

Листание: параметр `start` (смещение). Размер страницы задаётся `limit`.
Суммарное число строк блока возвращает `<block>.cursor` (поле `TOTAL`).
Параметр `iss.meta=off` убирает служебные блоки `metadata`/`columns` из ответа
(в коде клиента он передаётся всегда).

## 1. Дневная история акций по борде

```
GET /iss/history/engines/stock/markets/shares/boards/{board}/securities/{secid}.json
    ?from=YYYY-MM-DD&till=YYYY-MM-DD&start=0&limit=100&iss.meta=off
```

Пример (проверочный запрос, использован в CLI по умолчанию):

```
https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER.json?from=2024-01-09&till=2024-01-23&iss.meta=off
```

Блок ответа: `history`. Используемые колонки: `TRADEDATE`, `SECID`,
`SHORTNAME`, `BOARDID`, `NUMTRADES`, `VALUE`, `OPEN`, `LOW`, `HIGH`, `CLOSE`,
`WAPRICE`, `VOLUME`. В `raw.moex_history` они вынесены в отдельные поля,
а полная строка сохраняется в `row_json`.

## 2. Дневная история индекса

```
GET /iss/history/engines/stock/markets/index/securities/{index_code}.json
    ?from=YYYY-MM-DD&till=YYYY-MM-DD&start=0&limit=100&iss.meta=off
```

Пример:

```
https://iss.moex.com/iss/history/engines/stock/markets/index/securities/IMOEX.json?from=2024-01-09&till=2024-01-23&iss.meta=off
```

Блок ответа: `history`. В проекте из него берутся `TRADEDATE`, `SHORTNAME`,
`OPEN`, `LOW`, `HIGH`, `CLOSE`, `VALUE` (в `raw.moex_index_history`), полная
строка - в `row_json`.

## Сверка с API

Живая проверка выполняется вручную (не входит в пайплайн):

```bash
# 1. Доступность ISS
docker compose run --rm airflow-scheduler python /opt/airflow/scripts/check_moex.py

# 2. Реальный ответ по акциям: посмотреть имена колонок и число строк
docker compose run --rm airflow-scheduler python -c "
import requests, json
url = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER.json'
r = requests.get(url, params={'from':'2024-01-09','till':'2024-01-23','iss.meta':'off','limit':3}, timeout=30)
r.raise_for_status()
payload = r.json()
print('blocks:', sorted(payload))
print('columns:', payload['history']['columns'])
print('rows on page:', len(payload['history']['data']))
print('cursor:', payload.get('history.cursor'))
"
```

Если набор колонок отличается от перечисленного выше, в код нужно внести
соответствующие поля: клиент выносит в колонки только те значения, которые
находит по имени, а полная строка ответа всегда сохраняется в `row_json`,
поэтому данные не теряются.