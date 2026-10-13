-- Базовые схемы мини-DWH.
-- Скрипт идемпотентен и применяется автоматически:
--   * при первой инициализации тома dwh_postgres (docker-entrypoint-initdb.d);
--   * повторно - из DAG'а, чтобы схема существовала и на уже созданном томе.
--
-- Слои:
--   raw   - сырые ответы MOEX ISS API, как пришли;
--   stg   - типизированные и очищенные данные из raw;
--   core  - измерения и факты (звезда);
--   mart  - витрины для аналитики;
--   meta  - реестр источников и состояние загрузок;
--   audit - журналы запусков, задач и проверок качества данных.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA raw   IS 'Сырые данные MOEX ISS (как в API, без преобразований)';
COMMENT ON SCHEMA stg   IS 'Очищенные и типизированные данные';
COMMENT ON SCHEMA core  IS 'Ядро DWH: измерения и факты';
COMMENT ON SCHEMA mart  IS 'Витрины данных для аналитики';
COMMENT ON SCHEMA meta  IS 'Метаданные: источники и состояние загрузок';
COMMENT ON SCHEMA audit IS 'Аудит: запуски, задачи, проверки качества данных';