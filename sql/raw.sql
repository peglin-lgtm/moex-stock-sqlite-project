select
    ticker,
    trade_date,
    count(*) as duplicate_count
from raw_bars_daily
group by ticker, trade_date
having count(*) > 1;

select
    sum(case when open is null then 1 else 0 end) as null_open,
    sum(case when high is null then 1 else 0 end) as null_high,
    sum(case when low is null then 1 else 0 end) as null_low,
    sum(case when close is null then 1 else 0 end) as null_close,
    sum(case when volume is null then 1 else 0 end) as null_volume
from raw_bars_daily;

select
    sum(case when high < low then 1 else 0 end) as bad_high_low,
    sum(case when volume <= 0 then 1 else 0 end) as bad_volume
from raw_bars_daily;

select
    ticker,
    min(trade_date) as min_date,
    max(trade_date) as max_date,
    count(*) as total_rows
from raw_bars_daily
group by ticker;
