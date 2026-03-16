PRAGMA foreign_keys = ON;

.headers on
.mode column

select 'Top 10 worst daily returns' as section;

select
    i.ticker,
    m.trade_date,
    m.return * 100
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
order by m.return asc
limit 10;


select 'Top 10 best daily returns' as section;

select
    i.ticker,
    m.trade_date,
    m.return * 100
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
order by m.return desc
limit 10;


select 'Deepest drawdowns' as section;

select
    i.ticker,
    m.trade_date,
    m.drawdown
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
order by m.drawdown asc
limit 10;


select 'Highest volume spikes (volume_ratio_20)' as section;

select
    i.ticker,
    m.trade_date,
    m.volume_ratio_20
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
order by m.volume_ratio_20 desc
limit 10;


select 'Price crossing above MA20' as section;

select
    i.ticker,
    m.trade_date,
    m.close,
    m.ma_20
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
where m.close > m.ma_20
order by m.trade_date desc
limit 10;


select 'Average yearly return' as section;

select
    i.ticker,
    substr(m.trade_date,1,4) as year,
    avg(m.return) as avg_return
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
group by i.ticker, year
order by year;


select 'Yearly volatility' as section;

select
    i.ticker,
    substr(m.trade_date,1,4) as year,
    avg(m.volatility_20) as avg_volatility
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
group by i.ticker, year
order by year;


select 'Largest drawdowns' as section;

select
    i.ticker,
    m.trade_date,
    m.close,
    m.drawdown
from mart_market_metrics m
join stg_instruments i
    on i.instrument_id = m.instrument_id
order by m.drawdown
limit 20;


select 'Extreme daily moves' as section;

select
    sum(case when return > 0.02 then 1 else 0 end) as days_above_2pct,
    sum(case when return < -0.02 then 1 else 0 end) as days_below_minus2pct,
    sum(case when abs(return) > 0.05 then 1 else 0 end) as extreme_moves
from mart_market_metrics;