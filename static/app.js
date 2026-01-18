const form = document.getElementById("query-form");
const cacheOnlyButton = document.getElementById("cache-only");
const updateDataButton = document.getElementById("update-data");
const statusEl = document.getElementById("status");
const currentTickerEl = document.getElementById("current-ticker");
const resultsTable = document.getElementById("results-table");
const dataTable = document.getElementById("data-table");
const monthlyTable = document.getElementById("monthly-table");
const weekdayTable = document.getElementById("weekday-table");
const weekdayQuarterTable = document.getElementById("weekday-quarter-table");
const exportDataButton = document.getElementById("export-data");
const exportResultsButton = document.getElementById("export-results");
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

function tableToCsv(tableEl) {
  const rows = Array.from(tableEl.querySelectorAll("tr")).map((row) =>
    Array.from(row.querySelectorAll("th, td")).map((cell) => {
      const text = cell.textContent || "";
      const escaped = text.replace(/"/g, '""');
      return `"${escaped}"`;
    })
  );
  return rows.map((row) => row.join(",")).join("\n");
}

function downloadCsv(filename, csvText) {
  const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.setAttribute("download", filename);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
}

function renderTable(tableEl, columns, rows, options = {}) {
  const thead = tableEl.querySelector("thead");
  const tbody = tableEl.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  const priceColumns = new Set(["open", "high", "low", "close"]);
  const percentColumns = new Set([
    "cc",
    "oc",
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
  ]);
  const sortState = sortStates.get(tableEl) || null;
  const percentAll = Boolean(options.percentAll);
  const percentPrecision =
    typeof options.percentPrecision === "number" ? options.percentPrecision : 2;
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
      if ((percentColumns.has(colNameText) || (percentAll && index > 0)) && cell !== "") {
        const precision = percentColumns.has(colNameText)
          ? colNameText === "oo_1d" || colNameText === "cc"
            ? 1
            : 2
          : percentPrecision;
        td.textContent = Number.isFinite(number)
          ? `${number.toFixed(precision)}%`
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
  const action = options.action
    ? options.action
    : newTicker
    ? "new"
    : "cached";
  const payload = {
    ticker: newTicker || formData.get("ticker"),
    action,
    raw_only: Boolean(options.rawOnly),
  };
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
      const targetTable = options.target === "data" ? dataTable : resultsTable;
      clearTable(targetTable);
      return false;
    }

    const targetTable = options.target === "data" ? dataTable : resultsTable;
    renderTable(targetTable, data.columns, data.rows);
    if (payload.action === "cached" && payload.raw_only) {
      statusEl.textContent = "Loaded cached data only.";
    } else if (data.message) {
      statusEl.textContent = data.message;
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
    const targetTable = options.target === "data" ? dataTable : resultsTable;
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

async function submitWeekday(options = {}) {
  const payload = buildPayload(options);
  console.log("submitWeekday", payload);
  try {
    const response = await fetch("/api/weekday", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      clearTable(weekdayTable);
      return;
    }
    renderTable(weekdayTable, data.columns, data.rows, {
      percentAll: true,
      percentPrecision: 1,
      heatmap: true,
    });
  } catch (error) {
    clearTable(weekdayTable);
  }
}

async function submitWeekdayQuarter(options = {}) {
  const payload = buildPayload(options);
  console.log("submitWeekdayQuarter", payload);
  try {
    const response = await fetch("/api/weekday-quarter", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      clearTable(weekdayQuarterTable);
      return;
    }
    renderTable(weekdayQuarterTable, data.columns, data.rows, {
      percentAll: true,
      percentPrecision: 1,
      heatmap: true,
    });
  } catch (error) {
    clearTable(weekdayQuarterTable);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (autoRefreshTimer) {
    clearTimeout(autoRefreshTimer);
    autoRefreshTimer = null;
  }
  const ok = await submitQuery({ target: "results" });
  if (ok) {
    submitQuery({ action: "cached", rawOnly: true, target: "data" });
    submitMonthly();
    submitWeekday();
    submitWeekdayQuarter();
  }
});

cacheOnlyButton.addEventListener("click", () => {
  submitQuery({ action: "cached", rawOnly: true, target: "data" });
  submitMonthly();
  submitWeekday();
  submitWeekdayQuarter();
});

updateDataButton.addEventListener("click", async () => {
  const ok = await submitQuery({ action: "update", target: "results" });
  if (ok) {
    submitQuery({ action: "cached", rawOnly: true, target: "data" });
    submitMonthly();
    submitWeekday();
    submitWeekdayQuarter();
  }
});

window.addEventListener("DOMContentLoaded", () => {
  submitQuery({ action: "cached", target: "results" });
  submitQuery({ action: "cached", rawOnly: true, target: "data" });
  submitMonthly();
  submitWeekday();
  submitWeekdayQuarter();
});

form.addEventListener("change", (event) => {
  console.log("form change detected");
  if (event.target !== tickerSelect) {
    return;
  }
  if (autoRefreshTimer) {
    clearTimeout(autoRefreshTimer);
  }
  autoRefreshTimer = setTimeout(() => {
    submitQuery({ action: "cached", target: "results" });
    submitQuery({ action: "cached", rawOnly: true, target: "data" });
    submitMonthly();
    submitWeekday();
    submitWeekdayQuarter();
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

exportDataButton.addEventListener("click", () => {
  const csv = tableToCsv(dataTable);
  if (!csv.trim()) {
    statusEl.textContent = "No data to export.";
    return;
  }
  const ticker = tickerSelect ? tickerSelect.value : "data";
  const filename = `${ticker}_data.csv`;
  downloadCsv(filename, csv);
});

exportResultsButton.addEventListener("click", () => {
  const csv = tableToCsv(resultsTable);
  if (!csv.trim()) {
    statusEl.textContent = "No results to export.";
    return;
  }
  const ticker = tickerSelect ? tickerSelect.value : "results";
  const filename = `${ticker}_results.csv`;
  downloadCsv(filename, csv);
});
