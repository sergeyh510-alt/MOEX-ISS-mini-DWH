-- Слой mart: витрины для аналитики (построены из core, идемпотентны).
--
-- Витрины денормализованы (secid/boardid продублированы), чтобы их можно было
-- читать без джойнов - это осознанный выбор для портфолио-витрин.
--
-- Естественные ключи закреплены первичными ключами, запись идёт upsert'ом,
-- поэтому повторное построение за тот же период не создаёт дублей.

CREATE TABLE IF NOT EXISTS mart.stock_returns (
    security_key          bigint        NOT NULL REFERENCES core.dim_security (security_key),
    date_key              integer       NOT NULL REFERENCES core.dim_date (date_key),
    secid                 text          NOT NULL,
    boardid               text          NOT NULL,
    close                 numeric(20, 6) NOT NULL,
    prev_close            numeric(20, 6),
    daily_return_pct      numeric(12, 4),
    gap_pct               numeric(12, 4),
    amplitude_pct         numeric(12, 4),
    ma_5                  numeric(20, 6),
    ma_20                 numeric(20, 6),
    volatility_5_pct      numeric(12, 4),
    volatility_20_pct     numeric(12, 4),
    cumulative_return_pct numeric(12, 4),
    loaded_at             timestamptz   NOT NULL DEFAULT now(),
    updated_at            timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT mart_stock_returns_pkey PRIMARY KEY (security_key, date_key),
    CONSTRAINT mart_stock_returns_close_check CHECK (close > 0)
);

COMMENT ON TABLE mart.stock_returns IS 'Витрина: доходность бумаг и производные метрики';
COMMENT ON COLUMN mart.stock_returns.daily_return_pct IS 'Дневное изменение close к prev_close, %';
COMMENT ON COLUMN mart.stock_returns.gap_pct IS 'Гэп открытия к prev_close, %';
COMMENT ON COLUMN mart.stock_returns.amplitude_pct IS 'Размах дня (high - low) к prev_close, %';
COMMENT ON COLUMN mart.stock_returns.ma_5 IS 'Средняя close за 5 торговых дней (включая текущий)';
COMMENT ON COLUMN mart.stock_returns.ma_20 IS 'Средняя close за 20 торговых дней (NULL, если истории меньше)';
COMMENT ON COLUMN mart.stock_returns.volatility_5_pct IS 'Стандартное отклонение daily_return_pct за 5 дней';
COMMENT ON COLUMN mart.stock_returns.volatility_20_pct IS 'Стандартное отклонение daily_return_pct за 20 дней';
COMMENT ON COLUMN mart.stock_returns.cumulative_return_pct IS 'Накопленная доходность от первой доступной close бумаги, %';

CREATE INDEX IF NOT EXISTS idx_mart_stock_returns_date
    ON mart.stock_returns (date_key);
CREATE INDEX IF NOT EXISTS idx_mart_stock_returns_return
    ON mart.stock_returns (daily_return_pct DESC NULLS LAST);

-- Сравнение бумаги с индексом за тот же торговый день.
CREATE TABLE IF NOT EXISTS mart.index_compare (
    index_code        text          NOT NULL,
    security_key      bigint        NOT NULL REFERENCES core.dim_security (security_key),
    date_key          integer       NOT NULL REFERENCES core.dim_date (date_key),
    secid             text          NOT NULL,
    stock_return_pct  numeric(12, 4),
    index_return_pct  numeric(12, 4),
    excess_return_pct numeric(12, 4),
    loaded_at         timestamptz   NOT NULL DEFAULT now(),
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT mart_index_compare_pkey PRIMARY KEY (index_code, security_key, date_key)
);

COMMENT ON TABLE mart.index_compare IS 'Витрина: сравнение доходности бумаги с индексом за день';
COMMENT ON COLUMN mart.index_compare.excess_return_pct IS 'stock_return_pct - index_return_pct (NULL, если нет пары)';

CREATE INDEX IF NOT EXISTS idx_mart_index_compare_date
    ON mart.index_compare (date_key);

-- Лучшие и худшие бумаги дня (ранжирование внутри торгового дня).
CREATE TABLE IF NOT EXISTS mart.daily_movers (
    date_key         integer       NOT NULL REFERENCES core.dim_date (date_key),
    security_key     bigint        NOT NULL REFERENCES core.dim_security (security_key),
    secid            text          NOT NULL,
    daily_return_pct numeric(12, 4) NOT NULL,
    return_rank      integer       NOT NULL,
    is_best_of_day   boolean       NOT NULL,
    is_worst_of_day  boolean       NOT NULL,
    loaded_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT mart_daily_movers_pkey PRIMARY KEY (date_key, security_key),
    CONSTRAINT mart_daily_movers_rank_check CHECK (return_rank >= 1)
);

COMMENT ON TABLE mart.daily_movers IS 'Витрина: лучшие и худшие бумаги торгового дня';
COMMENT ON COLUMN mart.daily_movers.return_rank IS 'Ранг по daily_return_pct внутри дня (1 = лучший)';