PRAGMA foreign_keys = ON;


-- RAW LAYER

create table if not exists raw_bars_daily (
    source text not null default 'moex',
    ticker text not null,
    trade_date text not null,
    open real,
    high real,
    low real,
    close real,
    volume integer,
    ingested_at text not null default (datetime('now')),
    primary key (source, ticker, trade_date)
);

create index if not exists ix_raw_ticker_date
on raw_bars_daily(ticker, trade_date);


-- STAGING LAYER

create table if not exists stg_instruments (
    instrument_id integer primary key,
    ticker text not null unique,
    exchange text not null default 'MOEX',
    currency text not null default 'RUB',
    created_at text not null default (datetime('now'))
);

create table if not exists stg_bars_daily (
    instrument_id integer not null,
    trade_date text not null,
    open real,
    high real,
    low real,
    close real,
    volume integer,
    primary key (instrument_id, trade_date),
    foreign key (instrument_id) references stg_instruments(instrument_id)
);

create index if not exists ix_stg_date
on stg_bars_daily(trade_date);


-- MART LAYER

create table if not exists mart_market_data (
    instrument_id integer not null,
    trade_date text not null,
    open real,
    high real,
    low real,
    close real,
    volume integer,
    prev_close real,
    primary key (instrument_id, trade_date),
    foreign key (instrument_id) references stg_instruments(instrument_id)
);

create index if not exists ix_mart_data_date
on mart_market_data(trade_date);

create table if not exists mart_market_metrics (
    instrument_id integer not null,
    trade_date text not null,
    close real,
    return real,
    log_return real,
    volatility_20 real,
    volatility_50 real,
    cumulative_return real,
    ma_20 real,
    ma_50 real,
    avg_volume_20 real,
    volume_ratio_20 real,
    rolling_max_close real,
    drawdown real,
    primary key (instrument_id, trade_date),
    foreign key (instrument_id) references stg_instruments(instrument_id)
);

create index if not exists ix_mart_metrics_date
on mart_market_metrics(trade_date);
