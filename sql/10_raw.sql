-- Слой raw: сырые данные дневной истории торгов MOEX ISS.
--
-- Храним явные колонки бизнес-ключа и основных числовых показателей,
-- а полный набор полей ответа - в row_json (jsonb). Это защищает от
-- изменений набора колонок в API: ничего не теряем, ничего не выдумываем.
--
-- Натуральный ключ (boardid, secid, tradedate) закреплён уникальным
-- ограничением, поэтому повторная загрузка за ту же дату не создаёт дублей.

CREATE TABLE IF NOT EXISTS raw.moex_history (
    boardid      text        NOT NULL,
    secid        text        NOT NULL,
    tradedate    date        NOT NULL,
    shortname    text,
    numtrades    bigint,
    value        numeric(20, 2),
    open         numeric(20, 6),
    low          numeric(20, 6),
    high         numeric(20, 6),
    close        numeric(20, 6),
    waprice      numeric(20, 6),
    volume       bigint,
    row_json     jsonb       NOT NULL,
    source_code  text        NOT NULL DEFAULT 'moex_iss',
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT moex_history_pkey PRIMARY KEY (boardid, secid, tradedate)
);

COMMENT ON TABLE raw.moex_history IS 'Сырые дневные котировки акций (MOEX ISS history)';
COMMENT ON COLUMN raw.moex_history.row_json IS 'Полная строка ответа ISS как есть';
COMMENT ON COLUMN raw.moex_history.loaded_at IS 'Момент загрузки строки в raw';

CREATE INDEX IF NOT EXISTS idx_moex_history_tradedate ON raw.moex_history (tradedate);
CREATE INDEX IF NOT EXISTS idx_moex_history_secid ON raw.moex_history (secid);

-- Сырая дневная история индексов (например IMOEX).
CREATE TABLE IF NOT EXISTS raw.moex_index_history (
    index_code  text        NOT NULL,
    tradedate   date        NOT NULL,
    shortname   text,
    open        numeric(20, 6),
    low         numeric(20, 6),
    high        numeric(20, 6),
    close       numeric(20, 6),
    value       numeric(20, 2),
    row_json    jsonb       NOT NULL,
    source_code text        NOT NULL DEFAULT 'moex_iss',
    loaded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT moex_index_history_pkey PRIMARY KEY (index_code, tradedate)
);

COMMENT ON TABLE raw.moex_index_history IS 'Сырая дневная история индексов (MOEX ISS history)';
COMMENT ON COLUMN raw.moex_index_history.row_json IS 'Полная строка ответа ISS как есть';