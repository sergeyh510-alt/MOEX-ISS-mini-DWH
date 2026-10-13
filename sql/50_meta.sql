-- Слой meta: реестр источников и журнал запусков загрузок.

CREATE TABLE IF NOT EXISTS meta.source (
    source_code text PRIMARY KEY,
    description text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE meta.source IS 'Реестр внешних источников данных';
COMMENT ON COLUMN meta.source.source_code IS 'Короткий код источника, например moex_iss';

INSERT INTO meta.source (source_code, description)
VALUES ('moex_iss', 'Московская биржа, ISS API (iss.moex.com)')
ON CONFLICT (source_code) DO NOTHING;

-- Журнал запусков загрузки: одна строка на запуск задачи.
CREATE TABLE IF NOT EXISTS meta.load_run (
    load_run_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code   text NOT NULL REFERENCES meta.source (source_code),
    dag_id        text,
    task_id       text,
    run_id        text,
    target_table  text NOT NULL,
    date_from     date,
    date_to       date,
    status        text NOT NULL,
    rows_loaded   bigint NOT NULL DEFAULT 0,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    error_message text,
    CONSTRAINT load_run_status_check CHECK (status IN ('running', 'success', 'failed'))
);

COMMENT ON TABLE meta.load_run IS 'Журнал запусков загрузок: статус, объём, время';
COMMENT ON COLUMN meta.load_run.status IS 'running | success | failed';

CREATE INDEX IF NOT EXISTS idx_load_run_started_at ON meta.load_run (started_at DESC);

-- Число строк, отбракованных проверками качества при построении stg.
ALTER TABLE meta.load_run
    ADD COLUMN IF NOT EXISTS rows_rejected bigint NOT NULL DEFAULT 0;

COMMENT ON COLUMN meta.load_run.rows_rejected IS 'Сколько строк не прошло проверки качества (для stg)';

-- Отчёт о свежести и последнем успешном прогоне по таблицам слоёв.
-- Считается по журналу meta.load_run, который заполняют все CLI-скрипты.
CREATE OR REPLACE VIEW meta.freshness AS
SELECT target_table,
       max(finished_at) FILTER (WHERE status = 'success') AS last_success_at,
       max(date_to)     FILTER (WHERE status = 'success') AS last_success_to,
       count(*)         FILTER (WHERE status = 'failed')  AS failed_runs,
       count(*)                                           AS total_runs
  FROM meta.load_run
 GROUP BY target_table;

COMMENT ON VIEW meta.freshness IS 'Свежесть и последний успешный прогон по таблицам слоёв';