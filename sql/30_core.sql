-- Слой core: звезда из измерений (dim_*) и фактов (fact_*).
--
-- Суррогатные ключи:
--   * dim_security.security_key - суррогатный ключ бумаги (естественный ключ -
--     secid + boardid);
--   * dim_date.date_key - целочисленный ключ даты в формате YYYYMMDD;
--   * факты ссылаются на эти ключи, поэтому джойны по строкам не нужны.
--
-- Измерения хранят SCD1: значение перезаписывается, история версий не ведётся.
-- Суррогатный ключ при обновлении не меняется (одна строка на естественный ключ).

CREATE TABLE IF NOT EXISTS core.dim_security (
    security_key    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    secid           text        NOT NULL,
    boardid         text        NOT NULL,
    shortname       text,
    first_tradedate date,
    last_tradedate  date,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dim_security_natural_key UNIQUE (secid, boardid)
);

COMMENT ON TABLE core.dim_security IS 'Измерение: бумаги (SCD1)';
COMMENT ON COLUMN core.dim_security.security_key IS 'Суррогатный ключ бумаги';
COMMENT ON COLUMN core.dim_security.first_tradedate IS 'Первая дата торгов в stg';
COMMENT ON COLUMN core.dim_security.last_tradedate IS 'Последняя дата торгов в stg';

CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key       integer  PRIMARY KEY,
    calendar_date  date     NOT NULL,
    year           smallint NOT NULL,
    quarter        smallint NOT NULL,
    month          smallint NOT NULL,
    day            smallint NOT NULL,
    iso_week       smallint NOT NULL,
    day_of_week    smallint NOT NULL,
    day_name       text     NOT NULL,
    is_weekend     boolean  NOT NULL,
    is_trading_day boolean  NOT NULL DEFAULT false,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dim_date_calendar_date_key UNIQUE (calendar_date),
    CONSTRAINT dim_date_date_key_range CHECK (date_key BETWEEN 19000101 AND 99991231),
    CONSTRAINT dim_date_month_check CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT dim_date_quarter_check CHECK (quarter BETWEEN 1 AND 4)
);

COMMENT ON TABLE core.dim_date IS 'Измерение: календарь дат с признаком торгового дня';
COMMENT ON COLUMN core.dim_date.date_key IS 'Ключ даты в формате YYYYMMDD';
COMMENT ON COLUMN core.dim_date.is_trading_day IS 'Были ли в этот день данные торгов в stg';

CREATE TABLE IF NOT EXISTS core.fact_daily_prices (
    security_key    bigint        NOT NULL REFERENCES core.dim_security (security_key),
    date_key        integer       NOT NULL REFERENCES core.dim_date (date_key),
    open            numeric(20, 6) NOT NULL,
    low             numeric(20, 6) NOT NULL,
    high            numeric(20, 6) NOT NULL,
    close           numeric(20, 6) NOT NULL,
    waprice         numeric(20, 6),
    value           numeric(20, 2),
    volume          bigint,
    numtrades       bigint,
    prev_close      numeric(20, 6),
    close_change    numeric(20, 6),
    close_change_pct numeric(12, 4),
    source_code     text          NOT NULL DEFAULT 'moex_iss',
    loaded_at       timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT fact_daily_prices_pkey PRIMARY KEY (security_key, date_key),
    CONSTRAINT fact_daily_prices_price_check CHECK (low > 0 AND high >= low AND close > 0)
);

COMMENT ON TABLE core.fact_daily_prices IS 'Факт: дневные цены акций по бумаге и дате';
COMMENT ON COLUMN core.fact_daily_prices.prev_close IS 'Закрытие предыдущего торгового дня (NULL для первой даты истории)';
COMMENT ON COLUMN core.fact_daily_prices.close_change IS 'close - prev_close';
COMMENT ON COLUMN core.fact_daily_prices.close_change_pct IS 'Изменение close к prev_close в процентах';

CREATE INDEX IF NOT EXISTS idx_fact_daily_prices_date ON core.fact_daily_prices (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_daily_prices_security ON core.fact_daily_prices (security_key);

CREATE TABLE IF NOT EXISTS core.fact_daily_index (
    index_code       text          NOT NULL,
    date_key         integer       NOT NULL REFERENCES core.dim_date (date_key),
    open             numeric(20, 6) NOT NULL,
    low              numeric(20, 6) NOT NULL,
    high             numeric(20, 6) NOT NULL,
    close            numeric(20, 6) NOT NULL,
    value            numeric(20, 2),
    prev_close       numeric(20, 6),
    close_change     numeric(20, 6),
    close_change_pct numeric(12, 4),
    source_code      text          NOT NULL DEFAULT 'moex_iss',
    loaded_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT fact_daily_index_pkey PRIMARY KEY (index_code, date_key),
    CONSTRAINT fact_daily_index_price_check CHECK (low > 0 AND high >= low AND close > 0)
);

COMMENT ON TABLE core.fact_daily_index IS 'Факт: дневные значения индексов по коду и дате';
COMMENT ON COLUMN core.fact_daily_index.index_code IS 'Код индекса (вырожденное измерение), например IMOEX';