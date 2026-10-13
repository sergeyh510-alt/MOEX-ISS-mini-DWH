"""Оффлайн-тесты правил слоя audit (проверки качества и их статусы).

Ни сети, ни БД: проверяются чистые функции и реестр проверок из
include/dq_rules.py.

Запуск:

    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
"""

from __future__ import annotations

from datetime import datetime, timedelta

from include import dq_rules


def test_evaluate_status_within_threshold() -> None:
    """Ноль проблем при нулевом пороге - проверка пройдена."""
    assert dq_rules.evaluate_status(0, 0) == "passed"


def test_evaluate_status_boundary_is_passed() -> None:
    """Ровно на пороге - ещё пройдено (граница включительна)."""
    assert dq_rules.evaluate_status(3, 3) == "passed"


def test_evaluate_status_above_threshold_is_failed() -> None:
    """Превышение порога - проверка упала."""
    assert dq_rules.evaluate_status(1, 0) == "failed"
    assert dq_rules.evaluate_status(4, 3) == "failed"


def _result(name: str, status: str) -> dq_rules.DqResult:
    """Собрать результат проверки для сводки."""
    return dq_rules.DqResult(
        check_name=name,
        layer="raw",
        target_table="raw.moex_history",
        description="тест",
        issues_count=0,
        max_allowed=0,
        status=status,
    )


def test_summarize_run_counts_all_statuses() -> None:
    """Сводка: passed отдельно, failed и error - вместе как упавшие."""
    results = [
        _result("a", "passed"),
        _result("b", "failed"),
        _result("c", "error"),
        _result("d", "passed"),
    ]

    assert dq_rules.summarize_run(results) == (4, 2, 2)


def test_summarize_run_empty() -> None:
    """Пустой список проверок даёт нулевую сводку, а не ошибку."""
    assert dq_rules.summarize_run([]) == (0, 0, 0)


def test_run_status_only_passed_when_no_failures() -> None:
    """Запуск успешен только при нуле упавших проверок."""
    assert dq_rules.run_status(0) == "passed"
    assert dq_rules.run_status(1) == "failed"


def test_is_stale_without_timestamp() -> None:
    """Нет отметки загрузки - данные считаются устаревшими."""
    now = datetime(2024, 1, 23, 12, 0, 0)
    assert dq_rules.is_stale(None, now, 24) is True


def test_is_stale_boundary_not_stale() -> None:
    """Ровно на пороге данные ещё свежие (граница включительна)."""
    now = datetime(2024, 1, 23, 12, 0, 0)
    assert dq_rules.is_stale(now - timedelta(hours=24), now, 24) is False
    assert dq_rules.is_stale(now - timedelta(hours=1), now, 24) is False


def test_is_stale_above_threshold() -> None:
    """Старше порога - данные устарели."""
    now = datetime(2024, 1, 23, 12, 0, 0)
    assert dq_rules.is_stale(now - timedelta(hours=25), now, 24) is True


def test_build_check_specs_contains_core_checks() -> None:
    """Реестр проверок непустой и содержит проверки по каждому слою."""
    specs = dq_rules.build_check_specs()
    layers = {spec.layer for spec in specs}

    assert specs, "реестр проверок пуст"
    assert {"raw", "stg", "core", "mart"} <= layers


def test_build_check_specs_names_are_unique() -> None:
    """Имена проверок уникальны - иначе audit.dq_check не различит их."""
    specs = dq_rules.build_check_specs()
    names = [spec.check_name for spec in specs]

    assert len(names) == len(set(names))


def test_freshness_specs_use_custom_threshold() -> None:
    """Порог свежести попадает в max_allowed и в описание проверок."""
    specs = dq_rules.build_check_specs(freshness_max_age_hours=48)
    freshness = [spec for spec in specs if spec.check_name.startswith("freshness_")]

    assert freshness, "нет проверок свежести"
    assert all(spec.max_allowed == 48 for spec in freshness)
    assert all("48" in spec.description for spec in freshness)


def test_period_checks_use_named_parameters() -> None:
    """SQL проверок периода параметризован, а не склеен со значениями."""
    specs = dq_rules.build_check_specs()
    period_specs = [
        spec for spec in specs if not spec.check_name.startswith("freshness_")
    ]

    assert any("%(date_from)s" in spec.sql for spec in period_specs)
    assert any("%(date_key_from)s" in spec.sql for spec in period_specs)
    # Ни в одном запросе не должно быть значений периода, подставленных строкой.
    assert all("2024" not in spec.sql for spec in period_specs)