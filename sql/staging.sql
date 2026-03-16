PRAGMA foreign_keys = ON;

-- LOAD STAGING TABLES

insert into stg_instruments(ticker, exchange, currency)
select distinct ticker, 'MOEX' as exchange, 'RUB' as currency
from raw_bars_daily
where ticker is not null and length(trim(ticker)) > 0
on conflict(ticker) do nothing;

insert into stg_bars_daily(instrument_id, trade_date, open, high, low, close, volume)
select
    i.instrument_id,
    r.trade_date,
    r.open,
    r.high,
    r.low,
    r.close,
    r.volume
from raw_bars_daily r
join stg_instruments i
    on i.ticker = r.ticker
on conflict(instrument_id, trade_date) do update set
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume;

-- VALIDATION

select
    ticker,
    exchange,
    currency,
    created_at
from stg_instruments
order by ticker;

select
    i.ticker,
    min(s.trade_date) as min_date,
    max(s.trade_date) as max_date,
    count(*) as total_rows
from stg_bars_daily s
join stg_instruments i
    on i.instrument_id = s.instrument_id
group by i.ticker
order by i.ticker;

select
    i.ticker,
    sum(case when s.open is null then 1 else 0 end) as null_open,
    sum(case when s.high is null then 1 else 0 end) as null_high,
    sum(case when s.low is null then 1 else 0 end) as null_low,
    sum(case when s.close is null then 1 else 0 end) as null_close,
    sum(case when s.volume is null then 1 else 0 end) as null_volume
from stg_bars_daily s
join stg_instruments i
    on i.instrument_id = s.instrument_id
group by i.ticker
order by i.ticker;

select
    i.ticker,
    sum(case when s.high < s.low then 1 else 0 end) as bad_high_low,
    sum(case when s.open < s.low or s.open > s.high then 1 else 0 end) as open_outside_range,
    sum(case when s.close < s.low or s.close > s.high then 1 else 0 end) as close_outside_range,
    sum(case when s.volume <= 0 then 1 else 0 end) as bad_volume
from stg_bars_daily s
join stg_instruments i
    on i.instrument_id = s.instrument_id
group by i.ticker
order by i.ticker;

select
    i.ticker,
    s.trade_date,
    count(*) as cnt
from stg_bars_daily s
join stg_instruments i
    on i.instrument_id = s.instrument_id
group by i.ticker, s.trade_date
having count(*) > 1
order by i.ticker, s.trade_date;
