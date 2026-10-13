"""Необязательная проверка доступности MOEX ISS API.

Скрипт запускается вручную и НЕ участвует в DAG-пайплайне: недоступность
iss.moex.com не должна блокировать работу Airflow. Возвращает код 0, если
запрос успешен, и 1, если сервис недоступен или ответ не распознан.

Запуск (из корня проекта):
    docker compose run --rm airflow-scheduler python /opt/airflow/scripts/check_moex.py
"""

from __future__ import annotations

import os
import sys

import requests

ISS_BASE_URL: str = os.getenv("MOEX_ISS_BASE_URL", "https://iss.moex.com")
# /iss/index.json - корневой список доступных блоков ISS (самый дешёвый запрос).
ISS_PROBE_PATH: str = "/iss/index.json"
REQUEST_TIMEOUT_SECONDS: float = 15.0


def check_iss_availability(base_url: str, timeout: float) -> bool:
    """Проверить, отвечает ли MOEX ISS и разбирается ли ответ как JSON.

    Args:
        base_url: базовый адрес ISS, например ``https://iss.moex.com``.
        timeout: таймаут HTTP-запроса в секундах.

    Returns:
        ``True``, если сервис ответил HTTP 2xx и валидным JSON, иначе ``False``.
    """
    url = base_url.rstrip("/") + ISS_PROBE_PATH
    params = {"iss.meta": "off"}
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as error:
        print(f"[FAIL] {url}: {error}")
        return False
    except ValueError as error:
        print(f"[FAIL] {url}: ответ не является JSON ({error})")
        return False

    blocks = ", ".join(sorted(payload.keys())) or "<пусто>"
    print(f"[OK] {url}")
    print(f"     получены блоки: {blocks}")
    return True


def main() -> int:
    """Точка входа: запустить проверку и вернуть код выхода."""
    base_url = os.getenv("MOEX_ISS_BASE_URL", ISS_BASE_URL)
    print(f"Проверка доступности MOEX ISS: {base_url}")
    is_available = check_iss_availability(base_url, REQUEST_TIMEOUT_SECONDS)
    if not is_available:
        print("Подсказка: недоступность ISS не влияет на запуск docker compose.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())