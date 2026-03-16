PRAGMA foreign_keys = ON;

begin transaction;

delete from mart_market_metrics;
delete from mart_market_data;


-- LOAD mart_market_data

insert into mart_market_data (
    instrument_id,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    prev_close
)
select
    instrument_id,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    lag(close) over (
        partition by instrument_id
        order by trade_date
    ) as prev_close
from stg_bars_daily;


-- LOAD mart_market_metrics

insert into mart_market_metrics (
    instrument_id,
    trade_date,
    close,
    return,
    log_return,
    volatility_20,
    volatility_50,
    cumulative_return,
    ma_20,
    ma_50,
    avg_volume_20,
    volume_ratio_20,
    rolling_max_close,
    drawdown
)
with returns_base as (
    select
        instrument_id,
        trade_date,
        close,
        volume,
        prev_close,
        first_value(close) over (
            partition by instrument_id
            order by trade_date
        ) as first_close,

        case
            when prev_close is not null and prev_close != 0
            then (close - prev_close) / prev_close
        end as return,

        case
            when prev_close is not null and prev_close != 0
            then ln(close / prev_close)
        end as log_return,

        avg(
            case
                when prev_close is not null and prev_close != 0
                then (close - prev_close) / prev_close
            end
        ) over (
            partition by instrument_id
            order by trade_date
            rows between 19 preceding and current row
        ) as avg_return_20,

        avg(
            case
                when prev_close is not null and prev_close != 0
                then ((close - prev_close) / prev_close) * ((close - prev_close) / prev_close)
            end
        ) over (
            partition by instrument_id
            order by trade_date
            rows between 19 preceding and current row
        ) as avg_return_sq_20,

        avg(
            case
                when prev_close is not null and prev_close != 0
                then (close - prev_close) / prev_close
            end
        ) over (
            partition by instrument_id
            order by trade_date
            rows between 49 preceding and current row
        ) as avg_return_50,

        avg(
            case
                when prev_close is not null and prev_close != 0
                then ((close - prev_close) / prev_close) * ((close - prev_close) / prev_close)
            end
        ) over (
            partition by instrument_id
            order by trade_date
            rows between 49 preceding and current row
        ) as avg_return_sq_50,

        avg(close) over (
            partition by instrument_id
            order by trade_date
            rows between 19 preceding and current row
        ) as ma_20,

        avg(close) over (
            partition by instrument_id
            order by trade_date
            rows between 49 preceding and current row
        ) as ma_50,

        avg(volume) over (
            partition by instrument_id
            order by trade_date
            rows between 19 preceding and current row
        ) as avg_volume_20,

        max(close) over (
            partition by instrument_id
            order by trade_date
            rows between unbounded preceding and current row
        ) as rolling_max_close

    from mart_market_data
),
base as (
    select
        instrument_id,
        trade_date,
        close,
        volume,
        prev_close,
        first_close,
        return,
        log_return,
        case
            when avg_return_20 is not null and avg_return_sq_20 is not null
            then sqrt(max(avg_return_sq_20 - avg_return_20 * avg_return_20, 0))
        end as volatility_20,
        case
            when avg_return_50 is not null and avg_return_sq_50 is not null
            then sqrt(max(avg_return_sq_50 - avg_return_50 * avg_return_50, 0))
        end as volatility_50,
        case
            when first_close is not null and first_close != 0
            then close * 1.0 / first_close - 1
        end as cumulative_return,
        ma_20,
        ma_50,
        avg_volume_20,
        rolling_max_close
    from returns_base
)
select
    instrument_id,
    trade_date,
    close,
    return,
    log_return,
    volatility_20,
    volatility_50,
    cumulative_return,
    ma_20,
    ma_50,
    avg_volume_20,

    case
        when avg_volume_20 is not null and avg_volume_20 != 0
        then volume * 1.0 / avg_volume_20
    end as volume_ratio_20,

    rolling_max_close,

    case
        when rolling_max_close is not null and rolling_max_close != 0
        then close * 1.0 / rolling_max_close - 1
    end as drawdown

from base;

-- VALIDATION

select
    count(*) as mart_data_rows
from mart_market_data;

select
    count(*) as mart_metrics_rows
from mart_market_metrics;

select
    min(trade_date) as min_date,
    max(trade_date) as max_date
from mart_market_metrics;

select
    trade_date,
    close,
    prev_close
from mart_market_data
limit 10;

select
    trade_date,
    close,
    return,
    log_return,
    volatility_20,
    volatility_50,
    cumulative_return,
    ma_20,
    ma_50,
    avg_volume_20,
    volume_ratio_20,
    drawdown
from mart_market_metrics
limit 10;

commit;
