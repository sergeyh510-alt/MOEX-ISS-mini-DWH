-- Слой stg: типизированные и проверенные данные из raw.
--
-- Отличия от raw:
--   * только типизированные колонки (без jsonb);
--   * натуральные ключи закреплены уникальными ограничениями;
--   * явная валидация: is_valid + reject_reason (строки не удаляем, а
--     помечаем - это аудируемо, а фильтрацию сделает слой core);
--   * loaded_at (первая загрузка) и updated_at (последнее обновление).

CREATE TABLE IF NOT EXISTS stg.stock_history (
    boardid       text        NOT NULL,
    secid         text        NOT NULL,
    tradedate     date        NOT NULL,
    shortname     text,
    numtrades     bigint,
    value         numeric(20, 2),
    open          numeric(20, 6),
    low           numeric(20, 6),
    high          numeric(20, 6),
    close         numeric(20, 6),
    waprice       numeric(20, 6),
    volume        bigint,
    is_valid      boolean     NOT NULL,
    reject_reason text,
    source_code   text        NOT NULL DEFAULT 'moex_iss',
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT stg_stock_history_pkey PRIMARY KEY (boardid, secid, tradedate)
);

COMMENT ON TABLE stg.stock_history IS 'Типизированная дневная история акций';
COMMENT ON COLUMN stg.stock_history.is_valid IS 'Прошла ли строка проверки качества';
COMMENT ON COLUMN stg.stock_history.reject_reason IS 'Причины отбраковки через "; " (пусто, если строка валидна)';
COMMENT ON COLUMN stg.stock_history.loaded_at IS 'Момент первой загрузки строки в stg';
COMMENT ON COLUMN stg.stock_history.updated_at IS 'Момент последнего обновления строки в stg';

CREATE INDEX IF NOT EXISTS idx_stg_stock_history_tradedate
    ON stg.stock_history (tradedate);
CREATE INDEX IF NOT EXISTS idx_stg_stock_history_secid
    ON stg.stock_history (secid);
CREATE INDEX IF NOT EXISTS idx_stg_stock_history_valid
    ON stg.stock_history (is_valid) WHERE is_valid = false;

CREATE TABLE IF NOT EXISTS stg.index_history (
    index_code    text        NOT NULL,
    tradedate     date        NOT NULL,
    shortname     text,
    open          numeric(20, 6),
    low           numeric(20, 6),
    high          numeric(20, 6),
    close         numeric(20, 6),
    value         numeric(20, 2),
    is_valid      boolean     NOT NULL,
    reject_reason text,
    source_code   text        NOT NULL DEFAULT 'moex_iss',
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT stg_index_history_pkey PRIMARY KEY (index_code, tradedate)
);

COMMENT ON TABLE stg.index_history IS 'Типизированная дневная история индексов';
COMMENT ON COLUMN stg.index_history.is_valid IS 'Прошла ли строка проверки качества';

CREATE INDEX IF NOT EXISTS idx_stg_index_history_tradedate
    ON stg.index_history (tradedate);

-- Справочник бумаг (SCD1): одна строка на пару (secid, boardid).
-- Значения перезаписываются при изменении, история версий не хранится.
-- Пока справочник строится из того, что реально есть в raw (история торгов);
-- отдельный эндпоинт справочника бумаг - задача следующих этапов.
CREATE TABLE IF NOT EXISTS stg.securities (
    secid           text        NOT NULL,
    boardid         text        NOT NULL,
    shortname       text,
    first_tradedate date,
    last_tradedate  date,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT stg_securities_pkey PRIMARY KEY (secid, boardid)
);

COMMENT ON TABLE stg.securities IS 'Справочник бумаг из данных торгов (SCD1)';
COMMENT ON COLUMN stg.securities.first_tradedate IS 'Самая ранняя дата торгов, встреченная в raw';
COMMENT ON COLUMN stg.securities.last_tradedate IS 'Самая поздняя дата торгов, встреченная в raw';