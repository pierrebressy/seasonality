const form = document.getElementById("query-form");
const cacheOnlyButton = document.getElementById("cache-only");
const statusEl = document.getElementById("status");
const currentTickerEl = document.getElementById("current-ticker");
const resultsTable = document.getElementById("results-table");
const dataTable = document.getElementById("data-table");
const monthlyTable = document.getElementById("monthly-table");
const tickerSelect = document.getElementById("ticker-select");
const newTickerInput = document.querySelector("input[name='new_ticker']");
const tabButtons = document.querySelectorAll(".tab-button");
const tabPanels = document.querySelectorAll(".tab-panel");
const sortStates = new Map();
let autoRefreshTimer = null;

function updateTickerList(ticker) {
  if (!tickerSelect || !ticker) {
    return;
  }
  const value = String(ticker).trim();
  if (!value) {
    return;
  }
  const exists = Array.from(tickerSelect.options).some(
    (option) => option.value === value
  );
  if (!exists) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    tickerSelect.appendChild(option);
  }
  tickerSelect.value = value;
}

function clearTable(tableEl) {
  const thead = tableEl.querySelector("thead");
  const tbody = tableEl.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  sortStates.delete(tableEl);
}

function renderTable(tableEl, columns, rows, options = {}) {
  const thead = tableEl.querySelector("thead");
  const tbody = tableEl.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  const priceColumns = new Set(["open", "high", "low", "close"]);
  const sortState = sortStates.get(tableEl) || null;
  const percentAll = Boolean(options.percentAll);
  const heatmap = Boolean(options.heatmap);
  let heatmapRange = null;
  if (heatmap) {
    const values = rows
      .flatMap((row, rowIndex) =>
        row
          .map((cell, colIndex) => ({
            value: Number(cell),
            colIndex,
          }))
          .filter(
            (entry) =>
              entry.colIndex > 0 &&
              Number.isFinite(entry.value) &&
              !Number.isNaN(entry.value)
          )
      )
      .map((entry) => entry.value);
    if (values.length) {
      const min = Math.min(...values);
      const max = Math.max(...values);
      heatmapRange = { min, max };
    }
  }

  const headRow = document.createElement("tr");
  columns.forEach((col, index) => {
    const th = document.createElement("th");
    const label = document.createElement("span");
    label.textContent = col;
    th.appendChild(label);
    if (sortState && sortState.index === index) {
      const indicator = document.createElement("span");
      indicator.className = "sort-indicator";
      indicator.textContent = sortState.direction === "asc" ? "▲" : "▼";
      th.appendChild(indicator);
    }
    th.addEventListener("click", () => {
      const current =
        sortStates.get(tableEl) && sortStates.get(tableEl).index === index
          ? sortStates.get(tableEl).direction
          : "none";
      const next = current === "asc" ? "desc" : "asc";

      const sorted = [...rows].sort((a, b) => {
        const left = a[index];
        const right = b[index];
        const leftNum = Number(left);
        const rightNum = Number(right);
        const bothNumeric =
          left !== "" &&
          right !== "" &&
          Number.isFinite(leftNum) &&
          Number.isFinite(rightNum);

        if (bothNumeric) {
          return next === "asc" ? leftNum - rightNum : rightNum - leftNum;
        }

        const leftText = String(left ?? "");
        const rightText = String(right ?? "");
        return next === "asc"
          ? leftText.localeCompare(rightText)
          : rightText.localeCompare(leftText);
      });

      sortStates.set(tableEl, { index, direction: next });
      renderTable(tableEl, columns, sorted);
    });
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((cell, index) => {
      const td = document.createElement("td");
      const colName = columns[index];
      const colNameText = String(colName);
      const number = Number(cell);
      if (priceColumns.has(colName) && cell !== "") {
        td.textContent = Number.isFinite(number) ? number.toFixed(2) : cell;
        tr.appendChild(td);
        return;
      }
      if ((colNameText.includes("_pct") || (percentAll && index > 0)) && cell !== "") {
        td.textContent = Number.isFinite(number)
          ? `${number.toFixed(2)}%`
          : cell;
        if (heatmap && heatmapRange && Number.isFinite(number) && index > 0) {
          const span = heatmapRange.max - heatmapRange.min || 1;
          const ratio = (number - heatmapRange.min) / span;
          const red = Math.round(200 * (1 - ratio) + 55);
          const green = Math.round(200 * ratio + 55);
          const blue = 80;
          td.style.backgroundColor = `rgb(${red}, ${green}, ${blue})`;
          const luminance = (0.2126 * red + 0.7152 * green + 0.0722 * blue) / 255;
          td.style.color = luminance > 0.6 ? "#1b1a18" : "#fff9f2";
        }
        tr.appendChild(td);
        return;
      }
      td.textContent = cell;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function buildPayload(options = {}) {
  const formData = new FormData(form);
  const newTicker = String(formData.get("new_ticker") || "").trim();
  const payload = {
    ticker: newTicker || formData.get("ticker"),
    start: formData.get("start"),
    end: formData.get("end"),
    interval: formData.get("interval"),
    force_reload: false,
    cache_only: Boolean(options.cacheOnly),
    raw_only: Boolean(options.rawOnly),
    view_all_dates: formData.get("view_all_dates") === "on",
  };
  if (typeof options.viewAllDates !== "undefined") {
    payload.view_all_dates = Boolean(options.viewAllDates);
  }
  return payload;
}

async function submitQuery(options = {}) {
  statusEl.textContent = "Loading...";

  const payload = buildPayload(options);
  console.log("submitQuery", payload);

  try {
    const response = await fetch("/api/data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await response.json();
    if (!response.ok) {
      statusEl.textContent = data.error || "Request failed.";
      const targetTable = options.cacheOnly ? dataTable : resultsTable;
      clearTable(targetTable);
      return false;
    }

    const targetTable = options.cacheOnly ? dataTable : resultsTable;
    renderTable(targetTable, data.columns, data.rows);
    if (payload.cache_only) {
      statusEl.textContent = "Loaded cached data only.";
    } else {
      statusEl.textContent = data.downloaded
        ? "Data downloaded from yfinance."
        : "Loaded from SQLite cache.";
    }
    if (currentTickerEl) {
      const resolved = data.resolved_ticker || payload.ticker;
      currentTickerEl.textContent = `Ticker: ${resolved}`;
    }
    updateTickerList(payload.ticker);
    if (newTickerInput && newTickerInput.value.trim() !== "") {
      newTickerInput.value = "";
    }
    return true;
  } catch (error) {
    statusEl.textContent = "Unable to reach the API.";
    const targetTable = options.cacheOnly ? dataTable : resultsTable;
    clearTable(targetTable);
    return false;
  }
}

async function submitMonthly(options = {}) {
  const payload = buildPayload(options);
  console.log("submitMonthly", payload);
  try {
    const response = await fetch("/api/monthly", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      clearTable(monthlyTable);
      return;
    }
    renderTable(monthlyTable, data.columns, data.rows, {
      percentAll: true,
      heatmap: true,
    });
  } catch (error) {
    clearTable(monthlyTable);
    return;
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (autoRefreshTimer) {
    clearTimeout(autoRefreshTimer);
    autoRefreshTimer = null;
  }
  const ok = await submitQuery();
  if (ok) {
    submitQuery({ cacheOnly: true, rawOnly: true });
    submitMonthly({ cacheOnly: true, viewAllDates: true });
  }
});

cacheOnlyButton.addEventListener("click", () => {
  submitQuery({ cacheOnly: true, rawOnly: true });
  submitMonthly({ cacheOnly: true, viewAllDates: true });
});

window.addEventListener("DOMContentLoaded", () => {
  submitQuery();
  submitQuery({ cacheOnly: true, rawOnly: true });
  submitMonthly({ cacheOnly: true, viewAllDates: true });
});

form.addEventListener("change", () => {
  console.log("form change detected");
  if (autoRefreshTimer) {
    clearTimeout(autoRefreshTimer);
  }
  autoRefreshTimer = setTimeout(() => {
    submitQuery();
    submitQuery({ cacheOnly: true, rawOnly: true });
    submitMonthly({ cacheOnly: true, viewAllDates: true });
    autoRefreshTimer = null;
  }, 300);
});

tabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const tabName = button.dataset.tab;
    tabButtons.forEach((btn) => btn.classList.remove("active"));
    tabPanels.forEach((panel) => panel.classList.remove("active"));
    button.classList.add("active");
    document
      .getElementById(`tab-${tabName}`)
      .classList.add("active");
  });
});
