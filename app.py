from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, render_template, request

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "data" / "market.db"
DEFAULT_SETTINGS = {
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
    frame["semester"] = ((frame["date"].dt.month - 1) // 6 + 1).astype(int)
    frame["trimester"] = ((frame["date"].dt.month - 1) // 4 + 1).astype(int)
    frame["decade"] = (frame["year"] // 10 * 10).astype(int)
    frame["month"] = frame["date"].dt.month
    frame["day of year"] = frame["date"].dt.dayofyear.astype(int)
    frame["trading day"] = (
        frame.groupby(frame["date"].dt.year).cumcount() + 1
    ).astype(int)
    month_group = frame.groupby(
        [frame["date"].dt.year, frame["date"].dt.month]
    )
    frame["trading day of the month"] = (month_group.cumcount() + 1).astype(int)
    month_sizes = month_group["date"].transform("size").astype(int)
    frame["remaining trading days in the month"] = (
        month_sizes - frame["trading day of the month"]
    ).astype(int)
    trimester_group = frame.groupby([frame["year"], frame["trimester"]])
    frame["trading day of the trimester"] = (
        trimester_group.cumcount() + 1
    ).astype(int)
    trimester_sizes = trimester_group["date"].transform("size").astype(int)
    frame["remaining trading days in the trimester"] = (
        trimester_sizes - frame["trading day of the trimester"]
    ).astype(int)
    semester_group = frame.groupby([frame["year"], frame["semester"]])
    frame["trading day of the semester"] = (
        semester_group.cumcount() + 1
    ).astype(int)
    semester_sizes = semester_group["date"].transform("size").astype(int)
    frame["remaining trading days in the semester"] = (
        semester_sizes - frame["trading day of the semester"]
    ).astype(int)
    frame["week"] = frame["date"].dt.isocalendar().week.astype(int)
    frame["cc"] = (frame["close"] / frame["close"].shift(1) - 1.0) * 100.0
    frame["oc"] = (frame["close"] / frame["open"] - 1.0) * 100.0
    frame["co"] = (frame["open"] / frame["close"].shift(1) - 1.0) * 100.0
    for period in (2, 3, 4, 5, 6, 7, 8, 9, 10, 21, 30, 42, 63, 84, 105, 126):
        frame[f"cc -{period}"] = frame["close"].pct_change(period) * 100.0
        frame[f"cc +{period}"] = frame["close"].pct_change(-period) * 100.0
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    column_order = [
        "date",
        "open",
        "close",
        "weekday",
        "year",
        "semester",
        "trimester",
        "decade",
        "month",
        "week",
        "day of year",
        "trading day",
        "trading day of the month",
        "remaining trading days in the month",
        "trading day of the trimester",
        "remaining trading days in the trimester",
        "trading day of the semester",
        "remaining trading days in the semester",
        "oc",
        "cc",
        "co",
        "cc -2",
        "cc -3",
        "cc -4",
        "cc -5",
        "cc -6",
        "cc -7",
        "cc -8",
        "cc -9",
        "cc -10",
        "cc -21",
        "cc -30",
        "cc -42",
        "cc -63",
        "cc -84",
        "cc -105",
        "cc -126",
        "cc +2",
        "cc +3",
        "cc +4",
        "cc +5",
        "cc +6",
        "cc +7",
        "cc +8",
        "cc +9",
        "cc +10",
        "cc +21",
        "cc +30",
        "cc +42",
        "cc +63",
        "cc +84",
        "cc +105",
        "cc +126",
    ]
    extra_columns = [col for col in frame.columns if col not in column_order]
    return frame[column_order + extra_columns]


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


def compute_weekday_performance(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data.dropna(subset=["close"])
    if data.empty:
        return data
    data["weekday"] = data["date"].dt.day_name().str.lower()
    data = data.sort_values("date")
    data["week_year"] = data["date"].dt.isocalendar().year.astype(int)
    data["perf_pct"] = data.groupby("weekday")["close"].pct_change() * 100.0
    summary = (
        data.groupby(["weekday", "week_year"], as_index=False)["perf_pct"]
        .mean()
    )
    pivot = summary.pivot(index="weekday", columns="week_year", values="perf_pct")
    order = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
    ]
    pivot = pivot.reindex(order)
    pivot = pivot.sort_index(axis=1)
    pivot.index.name = "weekday"
    pivot = pivot.reset_index()
    return pivot


def compute_weekday_performance_by_quarter(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data.dropna(subset=["close"])
    if data.empty:
        return data
    data["weekday"] = data["date"].dt.day_name().str.lower()
    data["quarter"] = data["date"].dt.to_period("Q").astype(str)
    data["perf_pct"] = data.groupby("weekday")["close"].pct_change() * 100.0
    summary = (
        data.groupby(["weekday", "quarter"], as_index=False)["perf_pct"]
        .mean()
    )
    pivot = summary.pivot(index="weekday", columns="quarter", values="perf_pct")
    order = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
    ]
    pivot = pivot.reindex(order)
    pivot = pivot.sort_index(axis=1)
    pivot.index.name = "weekday"
    pivot = pivot.reset_index()
    return pivot


@app.route("/")
def index():
    init_db()
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
        default_ticker=default_ticker,
        tickers=tickers,
    )


@app.route("/api/data", methods=["POST"])
def api_data():
    payload = request.get_json(force=True)
    raw_ticker = payload.get("ticker", "AAPL")
    ticker = normalize_ticker(raw_ticker)
    action = payload.get("action", "cached")
    raw_only = bool(payload.get("raw_only", False))

    app.logger.info(
        "api_data request ticker=%s resolved=%s action=%s raw_only=%s",
        raw_ticker,
        ticker,
        action,
        raw_only,
    )

    init_db()
    set_setting("last_ticker", raw_ticker)

    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)

    message = None
    if action == "new":
        start = "2020-01-01"
        end = yesterday.strftime("%Y-%m-%d")
        app.logger.info("Downloading new data for %s from %s to %s", ticker, start, end)
        downloaded = download_prices(ticker, start, end, "1d")
        if downloaded.empty:
            return jsonify({"error": "no data returned from yfinance"}), 404
        upsert_prices(ticker, downloaded)
    elif action == "update":
        min_date, max_date = fetch_date_bounds(ticker)
        if not max_date:
            return jsonify({"error": "no cached data to update"}), 404
        start_date = datetime.strptime(max_date, "%Y-%m-%d").date() + timedelta(
            days=1
        )
        if start_date > yesterday:
            message = "already up to date"
        start = start_date.strftime("%Y-%m-%d")
        end = yesterday.strftime("%Y-%m-%d")
        if not message:
            app.logger.info("Updating data for %s from %s to %s", ticker, start, end)
            downloaded = download_prices(ticker, start, end, "1d")
            if downloaded.empty:
                return jsonify({"error": "no data returned from yfinance"}), 404
            upsert_prices(ticker, downloaded)

    existing = fetch_all_from_db(ticker)
    if existing.empty:
        return jsonify({"error": "no cached data for this ticker"}), 404

    if raw_only:
        raw = existing[
            ["date", "open", "high", "low", "close", "volume"]
        ].copy()
        columns = raw.columns.tolist()
        rows = raw.fillna("").values.tolist()
    else:
        enriched = compute_features(existing)
        enriched = enriched.drop(
            columns=["high", "low", "volume"], errors="ignore"
        )
        columns = enriched.columns.tolist()
        rows = enriched.fillna("").values.tolist()

    return jsonify(
        {
            "ticker": raw_ticker,
            "resolved_ticker": ticker,
            "rows": rows,
            "columns": columns,
            "downloaded": action in ("new", "update") and not message,
            "message": message,
        }
    )


@app.route("/api/monthly", methods=["POST"])
def api_monthly():
    payload = request.get_json(force=True)
    raw_ticker = payload.get("ticker", "AAPL")
    ticker = normalize_ticker(raw_ticker)

    init_db()
    set_setting("last_ticker", raw_ticker)

    existing = fetch_all_from_db(ticker)
    if existing.empty:
        return jsonify({"error": "no cached data for this ticker"}), 404

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


@app.route("/api/weekday", methods=["POST"])
def api_weekday():
    payload = request.get_json(force=True)
    raw_ticker = payload.get("ticker", "AAPL")
    ticker = normalize_ticker(raw_ticker)

    init_db()
    set_setting("last_ticker", raw_ticker)

    existing = fetch_all_from_db(ticker)
    if existing.empty:
        return jsonify({"error": "no cached data for this ticker"}), 404

    weekly = compute_weekday_performance(existing)
    columns = weekly.columns.tolist()
    rows = weekly.fillna("").values.tolist()

    return jsonify(
        {
            "ticker": raw_ticker,
            "resolved_ticker": ticker,
            "rows": rows,
            "columns": columns,
        }
    )


@app.route("/api/weekday-quarter", methods=["POST"])
def api_weekday_quarter():
    payload = request.get_json(force=True)
    raw_ticker = payload.get("ticker", "AAPL")
    ticker = normalize_ticker(raw_ticker)

    init_db()
    set_setting("last_ticker", raw_ticker)

    existing = fetch_all_from_db(ticker)
    if existing.empty:
        return jsonify({"error": "no cached data for this ticker"}), 404

    weekly = compute_weekday_performance_by_quarter(existing)
    columns = weekly.columns.tolist()
    rows = weekly.fillna("").values.tolist()

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
