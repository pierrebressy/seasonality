from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, render_template, request

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "data" / "market.db"
DEFAULT_SETTINGS = {
    "last_ticker": "AAPL",
}
INDEX_TICKERS = [
    "^VIX",
    "^VIX9D",
    "^VIX3M",
    "^VIX6M",
    "^SKEW",
    "^SDEX",
    "^TDEX",
    "^VOLI",
]

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


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    init_db()
    if request.method == "GET":
        graph_settings = get_setting("graph_settings")
        return jsonify({"graph_settings": graph_settings})

    payload = request.get_json(force=True)
    graph_settings = payload.get("graph_settings")
    if graph_settings is not None:
        if isinstance(graph_settings, str):
            stored_value = graph_settings
        else:
            stored_value = json.dumps(graph_settings)
        set_setting("graph_settings", stored_value)
    return jsonify({"ok": True})


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

    def third_friday(year: int, month: int) -> datetime.date:
        first = datetime(year, month, 1)
        days_until_friday = (4 - first.weekday()) % 7
        return (first + timedelta(days=days_until_friday + 14)).date()

    dates = frame["date"].dt.date
    months = frame["date"].dt.month
    years = frame["date"].dt.year
    current_tw = [
        third_friday(year, month) for year, month in zip(years, months)
    ]
    current_tw = pd.Series(current_tw, index=frame.index)
    before_or_on = dates <= current_tw
    next_tw = current_tw.copy()
    next_month = months + 1
    next_year = years + (next_month > 12).astype(int)
    next_month = ((next_month - 1) % 12) + 1
    next_tw_after = [
        third_friday(year, month) for year, month in zip(next_year, next_month)
    ]
    next_tw.loc[~before_or_on] = pd.Series(next_tw_after, index=frame.index)[
        ~before_or_on
    ]

    prev_month = months - 1
    prev_year = years - (prev_month < 1).astype(int)
    prev_month = ((prev_month - 1) % 12) + 1
    prev_tw = [
        third_friday(year, month) for year, month in zip(prev_year, prev_month)
    ]
    prev_tw = pd.Series(prev_tw, index=frame.index)
    prev_tw.loc[~before_or_on] = current_tw[~before_or_on]

    date_values = frame["date"].values.astype("datetime64[D]")
    next_tw_values = pd.to_datetime(next_tw).values.astype("datetime64[D]")
    prev_tw_values = pd.to_datetime(prev_tw).values.astype("datetime64[D]")
    current_pos = np.arange(len(frame))
    next_positions = np.searchsorted(date_values, next_tw_values, side="left")
    prev_positions = np.searchsorted(date_values, prev_tw_values, side="left")
    next_positions = np.where(next_positions >= len(frame), -1, next_positions)
    prev_positions = np.where(prev_positions >= len(frame), -1, prev_positions)

    remaining_before_tw = np.where(
        next_positions >= 0,
        np.maximum(next_positions - current_pos, 0),
        np.nan,
    )
    elapsed_since_tw = np.where(
        prev_positions >= 0,
        np.maximum(current_pos - prev_positions, 0),
        np.nan,
    )

    quarter_month = ((months - 1) // 3 + 1) * 3
    current_q_tw = [
        third_friday(year, month) for year, month in zip(years, quarter_month)
    ]
    current_q_tw = pd.Series(current_q_tw, index=frame.index)
    before_or_on_q = dates <= current_q_tw
    next_q_tw = current_q_tw.copy()
    next_quarter_month = quarter_month + 3
    next_quarter_year = years + (next_quarter_month > 12).astype(int)
    next_quarter_month = ((next_quarter_month - 1) % 12) + 1
    next_q_tw_after = [
        third_friday(year, month)
        for year, month in zip(next_quarter_year, next_quarter_month)
    ]
    next_q_tw.loc[~before_or_on_q] = pd.Series(
        next_q_tw_after, index=frame.index
    )[~before_or_on_q]

    prev_quarter_month = quarter_month - 3
    prev_quarter_year = years - (prev_quarter_month < 1).astype(int)
    prev_quarter_month = ((prev_quarter_month - 1) % 12) + 1
    prev_q_tw = [
        third_friday(year, month)
        for year, month in zip(prev_quarter_year, prev_quarter_month)
    ]
    prev_q_tw = pd.Series(prev_q_tw, index=frame.index)
    prev_q_tw.loc[~before_or_on_q] = current_q_tw[~before_or_on_q]

    next_q_tw_values = pd.to_datetime(next_q_tw).values.astype("datetime64[D]")
    prev_q_tw_values = pd.to_datetime(prev_q_tw).values.astype("datetime64[D]")
    next_q_positions = np.searchsorted(date_values, next_q_tw_values, side="left")
    prev_q_positions = np.searchsorted(date_values, prev_q_tw_values, side="left")
    next_q_positions = np.where(next_q_positions >= len(frame), -1, next_q_positions)
    prev_q_positions = np.where(prev_q_positions >= len(frame), -1, prev_q_positions)

    remaining_before_q_tw = np.where(
        next_q_positions >= 0,
        np.maximum(next_q_positions - current_pos, 0),
        np.nan,
    )
    elapsed_since_q_tw = np.where(
        prev_q_positions >= 0,
        np.maximum(current_pos - prev_q_positions, 0),
        np.nan,
    )

    frame["3 🧙 date"] = pd.to_datetime(next_tw).dt.strftime("%Y-%m-%d")
    frame["trading days remaining before 3 🧙"] = (
        pd.Series(remaining_before_tw, index=frame.index).astype("Int64")
    )
    frame["trading days elapsed since 3 🧙"] = (
        pd.Series(elapsed_since_tw, index=frame.index).astype("Int64")
    )
    frame["4 🧙 date"] = pd.to_datetime(next_q_tw).dt.strftime("%Y-%m-%d")
    frame["trading days remaining before 4 🧙"] = pd.Series(
        remaining_before_q_tw, index=frame.index
    ).astype("Int64")
    frame["trading days elapsed since 4 🧙"] = pd.Series(
        elapsed_since_q_tw, index=frame.index
    ).astype("Int64")
    frame["cc"] = (frame["close"] / frame["close"].shift(1) - 1.0) * 100.0
    frame["oc"] = (frame["close"] / frame["open"] - 1.0) * 100.0
    frame["co"] = (frame["open"] / frame["close"].shift(1) - 1.0) * 100.0
    oo = (frame["open"] / frame["open"].shift(1) - 1.0) * 100.0
    hh = (frame["high"] / frame["high"].shift(1) - 1.0) * 100.0
    ll = (frame["low"] / frame["low"].shift(1) - 1.0) * 100.0

    def compute_streaks(series: pd.Series, positive: bool) -> pd.Series:
        mask = series.gt(0) if positive else series.lt(0)
        streak = mask.groupby((mask != mask.shift()).cumsum()).cumcount() + 1
        return streak.where(mask, 0).astype(int)

    def compute_mask_streaks(mask: pd.Series) -> pd.Series:
        streak = mask.groupby((mask != mask.shift()).cumsum()).cumcount() + 1
        return streak.where(mask, 0).astype(int)

    frame["bullish run oo"] = compute_streaks(oo, positive=True)
    frame["bullish run oc"] = compute_streaks(frame["oc"], positive=True)
    frame["bullish run hh"] = compute_streaks(hh, positive=True)
    frame["bullish run ll"] = compute_streaks(ll, positive=True)
    frame["bearish run oo"] = compute_streaks(oo, positive=False)
    frame["bearish run oc"] = compute_streaks(frame["oc"], positive=False)
    frame["bearish run hh"] = compute_streaks(hh, positive=False)
    frame["bearish run ll"] = compute_streaks(ll, positive=False)
    ath_open = frame["open"].cummax()
    ath_high = frame["high"].cummax()
    ath_close = frame["close"].cummax()
    frame["ath open"] = frame["open"].eq(ath_open).astype(int)
    frame["ath high"] = frame["high"].eq(ath_high).astype(int)
    frame["ath close"] = frame["close"].eq(ath_close).astype(int)
    frame["consecutive ath open"] = compute_mask_streaks(
        frame["open"].eq(ath_open)
    )
    frame["consecutive ath high"] = compute_mask_streaks(
        frame["high"].eq(ath_high)
    )
    frame["consecutive ath close"] = compute_mask_streaks(
        frame["close"].eq(ath_close)
    )
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
        "3 🧙 date",
        "trading days remaining before 3 🧙",
        "trading days elapsed since 3 🧙",
        "4 🧙 date",
        "trading days remaining before 4 🧙",
        "trading days elapsed since 4 🧙",
        "oc",
        "cc",
        "co",
        "bullish run oo",
        "bullish run oc",
        "bullish run hh",
        "bullish run ll",
        "bearish run oo",
        "bearish run oc",
        "bearish run hh",
        "bearish run ll",
        "ath open",
        "ath high",
        "ath close",
        "consecutive ath open",
        "consecutive ath high",
        "consecutive ath close",
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
        rows = (
            enriched.astype(object)
            .where(pd.notna(enriched), "")
            .values.tolist()
        )

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


@app.route("/api/update-indexes", methods=["POST"])
def api_update_indexes():
    init_db()
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    results = []

    for raw_ticker in INDEX_TICKERS:
        ticker = normalize_ticker(raw_ticker)
        min_date, max_date = fetch_date_bounds(ticker)
        if max_date:
            start_date = datetime.strptime(max_date, "%Y-%m-%d").date() + timedelta(
                days=1
            )
            if start_date > yesterday:
                results.append(
                    {"ticker": raw_ticker, "status": "ok", "message": "up to date"}
                )
                continue
            start = start_date.strftime("%Y-%m-%d")
        else:
            start = "2020-01-01"
        end = yesterday.strftime("%Y-%m-%d")
        downloaded = download_prices(ticker, start, end, "1d")
        if downloaded.empty:
            results.append(
                {"ticker": raw_ticker, "status": "error", "message": "no data"}
            )
            continue
        upsert_prices(ticker, downloaded)
        results.append(
            {
                "ticker": raw_ticker,
                "status": "ok",
                "message": f"updated {start} to {end}",
            }
        )

    return jsonify({"results": results})


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


@app.route("/api/vix", methods=["POST"])
def api_vix():
    init_db()
    vix = fetch_all_from_db("^VIX")
    vix9d = fetch_all_from_db("^VIX9D")
    vix3m = fetch_all_from_db("^VIX3M")
    vix6m = fetch_all_from_db("^VIX6M")
    skew = fetch_all_from_db("^SKEW")
    sdex = fetch_all_from_db("^SDEX")
    tdex = fetch_all_from_db("^TDEX")
    voli = fetch_all_from_db("^VOLI")
    if (
        vix.empty
        or vix3m.empty
        or vix9d.empty
        or vix6m.empty
        or skew.empty
        or sdex.empty
        or tdex.empty
        or voli.empty
    ):
        return jsonify({"error": "missing VIX data"}), 404
    vix = vix[["date", "close"]].rename(columns={"close": "^VIX"})
    vix9d = vix9d[["date", "close"]].rename(columns={"close": "^VIX9D"})
    vix3m = vix3m[["date", "close"]].rename(columns={"close": "^VIX3M"})
    vix6m = vix6m[["date", "close"]].rename(columns={"close": "^VIX6M"})
    skew = skew[["date", "close"]].rename(columns={"close": "SKEW"})
    sdex = sdex[["date", "close"]].rename(columns={"close": "DEX S"})
    tdex = tdex[["date", "close"]].rename(columns={"close": "DEX T"})
    voli = voli[["date", "close"]].rename(columns={"close": "VOL I"})
    merged = (
        vix.merge(vix9d, on="date", how="inner")
        .merge(vix3m, on="date", how="inner")
        .merge(vix6m, on="date", how="inner")
        .merge(skew, on="date", how="inner")
        .merge(sdex, on="date", how="inner")
        .merge(tdex, on="date", how="inner")
        .merge(voli, on="date", how="inner")
        .sort_values("date")
    )
    merged["VIX9D / VIX"] = merged["^VIX9D"] / merged["^VIX"]
    merged["VIX 9D / VIX 3M"] = merged["^VIX9D"] / merged["^VIX3M"]
    merged["VIX 9D / VIX 6M"] = merged["^VIX9D"] / merged["^VIX6M"]
    merged["VIX / VIX 3M"] = merged["^VIX"] / merged["^VIX3M"]
    merged["VIX / VIX 6M"] = merged["^VIX"] / merged["^VIX6M"]
    merged["VIX 3M / VIX 6M"] = merged["^VIX3M"] / merged["^VIX6M"]
    merged["C/B"] = merged["^VIX"] / merged["^VIX3M"]
    columns = [
        "date",
        "^VIX",
        "^VIX9D",
        "^VIX3M",
        "^VIX6M",
        "SKEW",
        "DEX S",
        "DEX T",
        "VOL I",
        "C/B",
        "VIX9D / VIX",
        "VIX 9D / VIX 3M",
        "VIX 9D / VIX 6M",
        "VIX / VIX 3M",
        "VIX / VIX 6M",
        "VIX 3M / VIX 6M",
    ]
    rows = (
        merged[columns]
        .astype(object)
        .where(pd.notna(merged[columns]), "")
        .values.tolist()
    )
    return jsonify({"columns": columns, "rows": rows})


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
