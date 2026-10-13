-- Слой audit: результаты проверок качества данных (DQ).
--
-- Одна строка в audit.dq_run - один запуск scripts/run_dq.py за период,
-- одна строка в audit.dq_check - одна проверка внутри запуска.
-- Падение проверки означает статус failed у проверки и у запуска, скрипт
-- возвращает ненулевой код, поэтому задача Airflow падает и пайплайн
-- останавливается.

CREATE TABLE IF NOT EXISTS audit.dq_run (
    dq_run_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_from     date,
    date_to       date,
    status        text NOT NULL,
    checks_total  integer NOT NULL DEFAULT 0,
    checks_passed integer NOT NULL DEFAULT 0,
    checks_failed integer NOT NULL DEFAULT 0,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    CONSTRAINT dq_run_status_check CHECK (status IN ('running', 'passed', 'failed')),
    -- Пока запуск идёт (running), счётчики ещё не сведены; на завершении
    -- статус меняется, и суммы обязаны сойтись с checks_total.
    CONSTRAINT dq_run_counts_check CHECK (
        status = 'running' OR checks_passed + checks_failed = checks_total
    )
);

COMMENT ON TABLE audit.dq_run IS 'Запуски проверок качества данных';
COMMENT ON COLUMN audit.dq_run.status IS 'running | passed | failed';

CREATE INDEX IF NOT EXISTS idx_dq_run_started_at ON audit.dq_run (started_at DESC);

CREATE TABLE IF NOT EXISTS audit.dq_check (
    dq_check_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dq_run_id    bigint NOT NULL REFERENCES audit.dq_run (dq_run_id),
    check_name   text NOT NULL,
    layer        text NOT NULL,
    target_table text NOT NULL,
    description  text NOT NULL,
    status       text NOT NULL,
    issues_count bigint NOT NULL,
    max_allowed  bigint NOT NULL DEFAULT 0,
    details      text,
    checked_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dq_check_status_check CHECK (status IN ('passed', 'failed', 'error'))
);

COMMENT ON TABLE audit.dq_check IS 'Результаты отдельных проверок качества данных';
COMMENT ON COLUMN audit.dq_check.issues_count IS 'Сколько проблемных строк (или единиц проблемы) нашла проверка';
COMMENT ON COLUMN audit.dq_check.max_allowed IS 'Порог: проверка проходит, если issues_count <= max_allowed';
COMMENT ON COLUMN audit.dq_check.status IS 'passed | failed | error (error - проверка не смогла выполниться)';

CREATE INDEX IF NOT EXISTS idx_dq_check_run ON audit.dq_check (dq_run_id);
CREATE INDEX IF NOT EXISTS idx_dq_check_status ON audit.dq_check (status);