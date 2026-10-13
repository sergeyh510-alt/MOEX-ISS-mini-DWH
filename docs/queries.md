# Примеры аналитических запросов

Дополнительные запросы к мини-DWH (кроме примеров из README). Все запускаются
из контейнера БД:

```bash
docker compose exec dwh_postgres psql -U dwh -d moex_dwh -c "<запрос>"
```

Точные определения метрик - в `include/mart_rules.py`, структура таблиц - в
`sql/`.

## 1. Ряд одной бумаги со звёздным джойном (читаемые даты)

`mart.stock_returns` хранит `date_key` в формате `YYYYMMDD`; для читаемого
вывода джойним `core.dim_date`.

```sql
select d.calendar_date, s.secid, m.close, m.daily_return_pct,
       m.ma_5, m.ma_20, m.volatility_20_pct
  from mart.stock_returns m
  join core.dim_date d using (date_key)
  join core.dim_security s using (security_key)
 where s.secid = 'SBER'
 order by d.calendar_date desc
 limit 15;
```

## 2. Цена выше или ниже скользящей средней (тренд)

```sql
select s.secid, count(*) as days,
       count(*) filter (where m.close > m.ma_20) as days_above_ma20,
       round(100.0 * count(*) filter (where m.close > m.ma_20) / count(*), 1) as pct_above
  from mart.stock_returns m
  join core.dim_security s using (security_key)
 where m.ma_20 is not null
 group by s.secid order by pct_above desc;
```

## 3. Лучший и худший день каждой бумаги

```sql
select secid,
       (array_agg(date_key order by daily_return_pct desc))[1]  as best_day,
       round(max(daily_return_pct), 2)                          as best_pct,
       (array_agg(date_key order by daily_return_pct asc))[1]   as worst_day,
       round(min(daily_return_pct), 2)                          as worst_pct
  from mart.stock_returns
 where daily_return_pct is not null
 group by secid order by secid;
```

## 4. Бета и корреляция бумаги к индексу

Скользящее сравнение `mart.index_compare`; `corr` и `regr_slope` - встроенные
агрегаты PostgreSQL.

```sql
select secid, count(*) as days,
       round(corr(stock_return_pct, index_return_pct)::numeric, 3)   as corr_to_imoex,
       round(regr_slope(stock_return_pct, index_return_pct)::numeric, 3) as beta
  from mart.index_compare
 where stock_return_pct is not null and index_return_pct is not null
 group by secid order by beta desc;
```

## 5. Догоняющая доходность бумаги против индекса

Сравниваем накопленный рост бумаги (из `mart.stock_returns`) и индекса
(из `core.fact_daily_index`) от начала периода.

```sql
with base as (
  select s.secid, min(m.date_key) as first_key
    from mart.stock_returns m
    join core.dim_security s using (security_key)
   group by s.secid
)
select b.secid,
       round(100.0 * (last_val.close / first_val.close - 1), 2) as stock_cum_pct
  from base b
  join lateral (
    select close from mart.stock_returns m
     where m.security_key = (select security_key from core.dim_security s2
                              where s2.secid = b.secid)
     order by m.date_key asc limit 1
  ) first_val on true
  join lateral (
    select close from mart.stock_returns m
     where m.security_key = (select security_key from core.dim_security s3
                              where s3.secid = b.secid)
     order by m.date_key desc limit 1
  ) last_val on true
 order by stock_cum_pct desc;
```

## 6. Сколько торговых дней по месяцам

Признак `is_trading_day` считает слой core по данным stg.

```sql
select year, month, count(*) as days,
       count(*) filter (where is_trading_day) as trading_days
  from core.dim_date
 group by year, month order by year, month;
```

## 7. Пропуски: торговый день без строки у бумаги

Проверка полноты загрузки - торговые дни календаря, которых нет в факте.

```sql
select s.secid, count(*) as missing_days
  from core.dim_security s
  cross join core.dim_date d
 where d.is_trading_day
   and not exists (
       select 1 from core.fact_daily_prices f
        where f.security_key = s.security_key and f.date_key = d.date_key
   )
 group by s.secid order by missing_days desc;
```

## 8. Распределение дневной доходности (гистограмма)

```sql
select width_bucket(daily_return_pct, -10, 10, 10) as bucket,
       round(min(daily_return_pct), 2) as bucket_from,
       round(max(daily_return_pct), 2) as bucket_to,
       count(*) as days
  from mart.stock_returns
 where daily_return_pct is not null
 group by bucket order by bucket;
```

## 9. Идемпотентность: проверка на дубли (должно быть пусто)

```sql
select 'stock_returns' as tbl, count(*) as dup_groups from (
    select 1 from mart.stock_returns group by security_key, date_key having count(*) > 1
) d
union all
select 'index_compare', count(*) from (
    select 1 from mart.index_compare group by index_code, security_key, date_key having count(*) > 1
) d
union all
select 'daily_movers', count(*) from (
    select 1 from mart.daily_movers group by date_key, security_key having count(*) > 1
) d;
```