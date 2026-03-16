import argparse
import asyncio
import sqlite3
from datetime import date
from pathlib import Path
from typing import Iterable

import aiohttp
import aiomoex
import pandas as pd


DEFAULT_DB_PATH = "moex_stock.db"

# Default loading settings. Can be overridden via CLI arguments.
DEFAULT_TICKERS = ["GAZP"]
DEFAULT_START = "2010-01-01"
DEFAULT_END = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load MOEX daily bars into SQLite raw layer.")
    parser.add_argument(
        "--db",
        type=str,
        default=DEFAULT_DB_PATH,
        help="SQLite database path",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated MOEX tickers, e.g. GAZP,SBER,LKOH",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=DEFAULT_START,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=DEFAULT_END,
        help="End date in YYYY-MM-DD format (default: today)",
    )
    return parser.parse_args()


def parse_tickers(raw_tickers: str) -> list[str]:
    tickers = [t.strip().upper() for t in raw_tickers.split(",") if t.strip()]
    if not tickers:
        raise ValueError("At least one ticker must be provided via --tickers")
    return tickers


def ensure_db_settings(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")


def ensure_schema(conn: sqlite3.Connection) -> None:
    schema_path = Path(__file__).with_name("schema.sql")
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    conn.executescript(schema_path.read_text(encoding="utf-8"))


def upsert_raw_bars(conn: sqlite3.Connection, rows: Iterable[tuple]) -> int:
    """
    rows: (ticker, trade_date, open, high, low, close, volume)
    """
    sql = """
    INSERT INTO raw_bars_daily (ticker, trade_date, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(source, ticker, trade_date) DO UPDATE SET
      open  = excluded.open,
      high  = excluded.high,
      low   = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      ingested_at = datetime('now');
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    return cur.rowcount


async def fetch_daily_candles(session: aiohttp.ClientSession, ticker: str, start: str, end: str | None) -> pd.DataFrame:
    data = await aiomoex.get_market_candles(
        session,
        security=ticker,
        interval=24,
        start=start,
        end=end,
    )
    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["begin"]).dt.strftime("%Y-%m-%d")

    out = df[["trade_date", "open", "high", "low", "close", "volume"]].copy()
    out.insert(0, "ticker", ticker)
    return out


async def main() -> None:
    args = parse_args()
    tickers = parse_tickers(args.tickers)
    db_path = args.db
    start = args.start
    end = args.end or date.today().strftime("%Y-%m-%d")

    async with aiohttp.ClientSession() as session:
        all_frames: list[pd.DataFrame] = []
        for ticker in tickers:
            df = await fetch_daily_candles(session, ticker, start, end)
            print(f"{ticker}: fetched {len(df)} rows")
            all_frames.append(df)

    non_empty_frames = [df for df in all_frames if not df.empty]
    if not non_empty_frames:
        print("No data fetched. Check ticker names and dates.")
        return
    df_all = pd.concat(non_empty_frames, ignore_index=True)

    rows = list(df_all.itertuples(index=False, name=None))

    conn = sqlite3.connect(db_path)
    try:
        ensure_db_settings(conn)
        ensure_schema(conn)
        conn.execute("BEGIN;")
        affected = upsert_raw_bars(conn, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"Upserted rows (affected): {affected}")

    conn = sqlite3.connect(db_path)
    try:
        q = """
        SELECT ticker, MIN(trade_date), MAX(trade_date), COUNT(*)
        FROM raw_bars_daily
        GROUP BY ticker
        ORDER BY ticker;
        """
        print(pd.read_sql_query(q, conn))
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
