from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
import calendar
from pathlib import Path

import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, render_template, request

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "data" / "market.db"
DEFAULT_SETTINGS = {
    "default_start_date": "2010-01-01",
    "default_end_date": "2026-01-15",
    "last_ticker": "AAPL",
}

app = Flask(__name__)


def normalize_ticker(ticker: str) -> str:
    cleaned = (ticker or "").strip().upper()
    if cleaned == "SPX":
        return "^GSPC"
    return cleaned


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (ticker, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            DEFAULT_SETTINGS.items(),
        )


def get_setting(key: str) -> str | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
    return row[0] if row else None


def parse_date_safe(value: str) -> datetime.date | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        try:
            year, month, day = (int(part) for part in value.split("-"))
            last_day = calendar.monthrange(year, month)[1]
            day = min(max(day, 1), last_day)
            return datetime(year, month, day).date()
        except Exception:
            return None


def set_setting(key: str, value: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def fetch_tickers() -> list[str]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM prices ORDER BY ticker"
        ).fetchall()
    return [row[0] for row in rows]


def fetch_from_db(ticker: str, start: str, end: str) -> pd.DataFrame:
    query = """
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE ticker = ?
          AND date >= ?
          AND date <= ?
        ORDER BY date
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=(ticker, start, end))


def fetch_all_from_db(ticker: str) -> pd.DataFrame:
    query = """
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE ticker = ?
        ORDER BY date
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=(ticker,))


def fetch_date_bounds(ticker: str) -> tuple[str | None, str | None]:
    query = """
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM prices
        WHERE ticker = ?
    """
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(query, (ticker,)).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def upsert_prices(ticker: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    records = [
        (
            ticker,
            row["Date"],
            float(coerce_scalar(row["Open"])),
            float(coerce_scalar(row["High"])),
            float(coerce_scalar(row["Low"])),
            float(coerce_scalar(row["Close"])),
            (
                int(coerce_scalar(row["Volume"]))
                if pd.notna(coerce_scalar(row["Volume"]))
                else None
            ),
        )
        for _, row in frame.iterrows()
    ]
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO prices
                (ticker, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )


def download_prices(
    ticker: str, start: str, end: str, interval: str
) -> pd.DataFrame:
    print(
        f"🟢🟢 [download_prices] for {ticker} from {start} to {end} interval={interval}"
    )
    data = yf.download(
        tickers=ticker,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
    )
    if data.empty:
        return data
    if isinstance(data.columns, pd.MultiIndex):
        if ticker in data.columns.get_level_values(-1):
            data = data.xs(ticker, axis=1, level=-1, drop_level=True)
        elif ticker in data.columns.get_level_values(0):
            data = data.xs(ticker, axis=1, level=0, drop_level=True)
    data = data.reset_index()
    data.rename(
        columns={
            "Date": "Date",
            "Open": "Open",
            "High": "High",
            "Low": "Low",
            "Close": "Close",
            "Volume": "Volume",
        },
        inplace=True,
    )
    data["Date"] = pd.to_datetime(data["Date"]).dt.strftime("%Y-%m-%d")
    return data[["Date", "Open", "High", "Low", "Close", "Volume"]]


def coerce_scalar(value):
    if isinstance(value, pd.Series):
        if value.empty:
            return None
        return value.iloc[0]
    return value


def compute_features(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["weekday"] = frame["date"].dt.day_name().str.lower()
    frame["year"] = frame["date"].dt.year
    frame["month"] = frame["date"].dt.month
    frame["week_of_year"] = frame["date"].dt.isocalendar().week.astype(int)
    frame["oc_pct"] = (frame["close"] / frame["open"] - 1.0) * 100.0
    for period in (1, 2, 3):
        frame[f"oo_pct_{period}d"] = frame["open"].pct_change(period) * 100.0
        frame[f"cc_pct_{period}d"] = frame["close"].pct_change(period) * 100.0
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    return frame


def compute_monthly_performance(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    month_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data.dropna(subset=["close"])
    if data.empty:
        return data
    data["year"] = data["date"].dt.year
    data["month"] = data["date"].dt.month
    monthly = data.groupby(["year", "month"], as_index=False)["close"].agg(
        first="first", last="last"
    )
    monthly["perf_pct"] = (monthly["last"] / monthly["first"] - 1.0) * 100.0
    pivot = monthly.pivot(index="month", columns="year", values="perf_pct")
    pivot = pivot.reindex(range(1, 13))
    pivot = pivot.sort_index(axis=1)
    pivot.index.name = "month"
    pivot = pivot.reset_index()
    pivot["month"] = pivot["month"].apply(
        lambda value: month_names[value - 1] if 1 <= value <= 12 else value
    )
    return pivot


@app.route("/")
def index():
    init_db()
    default_start = (
        get_setting("default_start_date")
        or DEFAULT_SETTINGS["default_start_date"]
    )
    default_end = (
        get_setting("default_end_date")
        or DEFAULT_SETTINGS["default_end_date"]
    )
    default_ticker = (
        get_setting("last_ticker") or DEFAULT_SETTINGS["last_ticker"]
    )
    tickers = fetch_tickers()
    if not tickers:
        tickers = ["^GSPC"]
    if default_ticker and default_ticker not in tickers:
        tickers.insert(0, default_ticker)
    return render_template(
        "index.html",
        default_start=default_start,
        default_end=default_end,
        default_ticker=default_ticker,
        tickers=tickers,
    )


@app.route("/api/data", methods=["POST"])
def api_data():
    payload = request.get_json(force=True)
    raw_ticker = payload.get("ticker", "AAPL")
    ticker = normalize_ticker(raw_ticker)
    start = payload.get("start")
    end = payload.get("end")
    interval = payload.get("interval", "1d")
    force_reload = bool(payload.get("force_reload", False))
    cache_only = bool(payload.get("cache_only", False))
    raw_only = bool(payload.get("raw_only", False))
    view_all_dates = bool(payload.get("view_all_dates", False))

    app.logger.info(
        "api_data request ticker=%s resolved=%s start=%s end=%s interval=%s force=%s cache_only=%s view_all=%s raw_only=%s",
        raw_ticker,
        ticker,
        start,
        end,
        interval,
        force_reload,
        cache_only,
        view_all_dates,
        raw_only,
    )

    if not start or not end:
        return jsonify({"error": "start and end are required"}), 400
    start_date = parse_date_safe(start)
    end_date = parse_date_safe(end)
    if not start_date or not end_date:
        return jsonify({"error": "start and end must be YYYY-MM-DD"}), 400

    if start_date > end_date:
        return jsonify({"error": "start date must be before end date"}), 400

    if not (cache_only and view_all_dates):
        today = datetime.now(timezone.utc).date()
        if start_date > today or end_date > today:
            return jsonify({"error": "date range is in the future"}), 400

    init_db()
    set_setting("last_ticker", raw_ticker)

    existing = (
        fetch_all_from_db(ticker)
        if view_all_dates
        else fetch_from_db(ticker, start, end)
    )
    should_download = force_reload or existing.empty

    if cache_only:
        should_download = False

    if should_download:
        app.logger.info("Downloading data from yfinance for %s", ticker)
        downloaded = download_prices(ticker, start, end, interval)
        if downloaded.empty:
            return jsonify({"error": "no data returned from yfinance"}), 404
        upsert_prices(ticker, downloaded)
        existing = (
            fetch_all_from_db(ticker)
            if view_all_dates
            else fetch_from_db(ticker, start, end)
        )
        app.logger.info("Stored %s rows into SQLite", len(existing))
    elif existing.empty:
        return jsonify({"error": "no cached data for this range"}), 404

    if raw_only:
        raw = existing[
            ["date", "open", "high", "low", "close", "volume"]
        ].copy()
        columns = raw.columns.tolist()
        rows = raw.fillna("").values.tolist()
    else:
        enriched = compute_features(existing)
        columns = enriched.columns.tolist()
        rows = enriched.fillna("").values.tolist()

    return jsonify(
        {
            "ticker": raw_ticker,
            "resolved_ticker": ticker,
            "rows": rows,
            "columns": columns,
            "downloaded": should_download,
        }
    )


@app.route("/api/monthly", methods=["POST"])
def api_monthly():
    payload = request.get_json(force=True)
    raw_ticker = payload.get("ticker", "AAPL")
    ticker = normalize_ticker(raw_ticker)
    start = payload.get("start")
    end = payload.get("end")
    interval = payload.get("interval", "1d")
    force_reload = bool(payload.get("force_reload", False))
    cache_only = bool(payload.get("cache_only", False))
    view_all_dates = bool(payload.get("view_all_dates", False))

    if not start or not end:
        return jsonify({"error": "start and end are required"}), 400
    start_date = parse_date_safe(start)
    end_date = parse_date_safe(end)
    if not start_date or not end_date:
        return jsonify({"error": "start and end must be YYYY-MM-DD"}), 400

    if start_date > end_date:
        return jsonify({"error": "start date must be before end date"}), 400

    if not (cache_only and view_all_dates):
        today = datetime.now(timezone.utc).date()
        if start_date > today or end_date > today:
            return jsonify({"error": "date range is in the future"}), 400

    init_db()
    set_setting("last_ticker", raw_ticker)

    existing = (
        fetch_all_from_db(ticker)
        if view_all_dates
        else fetch_from_db(ticker, start, end)
    )
    should_download = force_reload or existing.empty

    if cache_only:
        should_download = False

    if should_download:
        downloaded = download_prices(ticker, start, end, interval)
        if downloaded.empty:
            return jsonify({"error": "no data returned from yfinance"}), 404
        upsert_prices(ticker, downloaded)
        existing = (
            fetch_all_from_db(ticker)
            if view_all_dates
            else fetch_from_db(ticker, start, end)
        )
    elif existing.empty:
        return jsonify({"error": "no cached data for this range"}), 404

    monthly = compute_monthly_performance(existing)
    columns = monthly.columns.tolist()
    rows = monthly.fillna("").values.tolist()

    return jsonify(
        {
            "ticker": raw_ticker,
            "resolved_ticker": ticker,
            "rows": rows,
            "columns": columns,
        }
    )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5004, debug=True)
