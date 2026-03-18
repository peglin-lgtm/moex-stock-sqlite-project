# MOEX Stock SQLite Project

Small ETL/BI project for loading MOEX daily bars into SQLite, preparing analytical marts, and analyzing the data in SQL and Power BI.

## Project Goal

The goal of the project is to build a clear local pipeline for MOEX stocks that:

- loads market data into SQLite
- transforms it into a clean analytical structure
- calculates core market metrics
- provides a ready-to-use database for SQL analysis and Power BI

## What the Project Does

The project has three data layers:

- `raw_bars_daily` - raw daily OHLCV data
- `stg_instruments`, `stg_bars_daily` - staging layer
- `mart_market_data`, `mart_market_metrics` - analytical marts

The data source is the MOEX API. Data loading is handled by [`load_moex_to_sqlite.py`](load_moex_to_sqlite.py).

## Project Structure

- [`schema.sql`](schema.sql) - creates tables and indexes
- [`drop_all.sql`](drop_all.sql) - drops all tables
- [`raw.sql`](raw.sql) - raw-layer checks
- [`staging.sql`](staging.sql) - loads the staging layer and validates data
- [`mart.sql`](mart.sql) - builds marts and calculates metrics
- [`analysis.sql`](analysis.sql) - sample analytical SQL queries
- [`load_moex_to_sqlite.py`](load_moex_to_sqlite.py) - loads MOEX data into the raw layer
- [`run_all.ps1`](run_all.ps1) - runs the full pipeline with one command
- [`BI_stock.pbix`](BI_stock.pbix) - Power BI dashboard
- `moex_stock.db` - project SQLite database

## Data Architecture

The project follows the standard `raw -> staging -> mart` pattern.

- `raw` - raw ingestion layer
- `staging` - cleansing and normalization layer
- `mart` - analysis-ready metrics layer for analytics and BI

Data flow:

```text
MOEX API
  -> raw_bars_daily
  -> stg_instruments
  -> stg_bars_daily
  -> mart_market_data
  -> mart_market_metrics
  -> SQL analysis / Power BI dashboard
```

Table purpose:

- `raw_bars_daily` stores source daily OHLCV data
- `stg_instruments` stores the instrument reference list
- `stg_bars_daily` stores quotes linked to `instrument_id`
- `mart_market_data` stores market data and `prev_close`
- `mart_market_metrics` stores calculated analytical indicators

## Metrics in `mart_market_metrics`

The mart calculates:

- `return` - daily return
- `log_return` - logarithmic return `ln(close / prev_close)`
- `volatility_20`, `volatility_50` - rolling volatility of daily returns over 20 and 50 days
- `cumulative_return` - cumulative return since the instrument's first price
- `ma_20`, `ma_50` - moving averages of closing prices
- `avg_volume_20` - average volume over 20 days
- `volume_ratio_20` - ratio of current volume to average 20-day volume
- `rolling_max_close` - cumulative maximum closing price
- `drawdown` - drawdown relative to the historical maximum

## Technologies Used

- `Python 3` - data loading
- `PowerShell` - full pipeline execution
- `SQLite` - local data storage
- `sqlite3.exe` - command-line SQL script execution
- `SQL` - data layer construction and analytics queries
- `aiohttp` - HTTP client for asynchronous data loading
- `aiomoex` - MOEX market data access
- `pandas` - data preparation before writing to SQLite
- `Power BI Desktop` - visualization of the final marts

## Requirements

- Windows PowerShell
- Python 3
- SQLite CLI (`sqlite3.exe` is already included in the project)
- Python packages:

```powershell
py -3 -m pip install aiohttp aiomoex pandas
```

If `py` does not work, you can use:

```powershell
python -m pip install aiohttp aiomoex pandas
```

## Requirements Analysis

What the project should support:

- loading daily data for one or more MOEX tickers
- storing data in a local SQLite database
- separating data into raw, staging, and mart layers
- calculating market metrics for analysis
- allowing marts to be recalculated
- connecting to Power BI without an additional server

Technical requirements covered by the solution:

- simple local execution
- clear SQL file structure
- full project rebuild capability
- suitability for manual data validation
- compatibility with PowerShell, SQLite, and Power BI Desktop

How the project addresses these requirements:

- full rebuild via [`run_all.ps1`](run_all.ps1)
- separate SQL files for layers and processing stages
- Python loader with parameters for database, tickers, and dates
- `mart_market_metrics` mart with key price, volume, and risk metrics
- a set of analytical SQL queries in [`analysis.sql`](analysis.sql)

## Quick Start

Full project build with one command:

```powershell
.\run_all.ps1
```

By default, the script:

- creates a new `moex_stock.db` database or recreates the existing one
- removes old tables with `drop_all.sql`
- executes `schema.sql`
- loads MOEX data for ticker `GAZP` starting from `2010-01-01`
- populates staging and mart

Run with parameters:

```powershell
.\run_all.ps1 -Database moex_stock.db -Tickers "GAZP,SBER,LKOH" -Start "2015-01-01" -End "2026-03-16"
```

If PowerShell blocks script execution:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_all.ps1 -Tickers "GAZP,SBER"
```

## Important

`run_all.ps1` performs a full rebuild and starts with [`drop_all.sql`](drop_all.sql). If the database contains data you need, it will be deleted.

If your goal is not a full rebuild but only adding new tickers to an existing database, you should not use `run_all.ps1`.

To add new tickers, this order is recommended:

1. Run [`load_moex_to_sqlite.py`](load_moex_to_sqlite.py) with the required tickers
2. Execute [`staging.sql`](staging.sql)
3. Execute [`mart.sql`](mart.sql)

## Manual Step-by-Step Run

1. Create tables:

```powershell
.\sqlite3.exe moex_stock.db ".read schema.sql"
```

2. Load raw data:

```powershell
py -3 .\load_moex_to_sqlite.py --db moex_stock.db --tickers "GAZP,SBER" --start "2015-01-01" --end "2026-03-16"
```

3. Build staging:

```powershell
.\sqlite3.exe moex_stock.db ".read staging.sql"
```

4. Build mart:

```powershell
.\sqlite3.exe moex_stock.db ".read mart.sql"
```

5. Run analysis:

```powershell
.\sqlite3.exe moex_stock.db ".read analysis.sql"
```

## `load_moex_to_sqlite.py` Parameters

When running the Python script, you can set parameters manually:

- `--db` - database name or path
- `--tickers` - comma-separated tickers, for example `GAZP,SBER,LKOH`
- `--start` - load start date in `YYYY-MM-DD` format
- `--end` - load end date in `YYYY-MM-DD` format

If `--end` is not specified, the script uses the current date.

Example:

```powershell
py -3 .\load_moex_to_sqlite.py --db my_stock.db --tickers "GAZP,MGNT,SBER" --start "2020-01-01" --end "2026-03-16"
```

If you only need to add new tickers to an existing database, rerun the following after loading raw data:

```powershell
.\sqlite3.exe moex_stock.db ".read staging.sql"
.\sqlite3.exe moex_stock.db ".read mart.sql"
```

## Checks and Analysis

- [`raw.sql`](raw.sql) checks duplicates, gaps, and basic anomalies in raw data
- [`staging.sql`](staging.sql) loads staging and includes validation queries
- [`mart.sql`](mart.sql) recalculates marts
- [`analysis.sql`](analysis.sql) contains ready-to-use queries for returns, volatility, drawdowns, and extreme moves

## Power BI Dashboard

The [`BI_stock.pbix`](BI_stock.pbix) file uses the project's SQLite database as its data source.

The dashboard can be used to view:

- closing price trend by date
- daily `return`
- cumulative `cumulative_return`
- `volatility_20` and `volatility_50`
- `drawdown`
- volume spikes using `volume_ratio_20`

After updating the database:

1. Open `BI_stock.pbix`
2. Click `Refresh`
3. Make sure Power BI is connected to the current `moex_stock.db` file

## Result

The project produces a local SQLite database with marts for:

- SQL analysis
- Power BI visualizations
- evaluating returns, volatility, volume, and drawdowns for MOEX tickers
