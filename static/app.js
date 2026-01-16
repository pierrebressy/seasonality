const form = document.getElementById("query-form");
const cacheOnlyButton = document.getElementById("cache-only");
const statusEl = document.getElementById("status");
const resultsTable = document.getElementById("results-table");
const dataTable = document.getElementById("data-table");
const monthlyTable = document.getElementById("monthly-table");
const tabButtons = document.querySelectorAll(".tab-button");
const tabPanels = document.querySelectorAll(".tab-panel");
const sortStates = new Map();

function renderTable(tableEl, columns, rows, options = {}) {
  const thead = tableEl.querySelector("thead");
  const tbody = tableEl.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  const priceColumns = new Set(["open", "high", "low", "close"]);
  const sortState = sortStates.get(tableEl) || null;
  const percentAll = Boolean(options.percentAll);

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
  const payload = {
    ticker: formData.get("ticker"),
    start: formData.get("start"),
    end: formData.get("end"),
    interval: formData.get("interval"),
    force_reload: formData.get("force_reload") === "on",
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

  try {
    const response = await fetch("/api/data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await response.json();
    if (!response.ok) {
      statusEl.textContent = data.error || "Request failed.";
      return;
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
  } catch (error) {
    statusEl.textContent = "Unable to reach the API.";
  }
}

async function submitMonthly(options = {}) {
  const payload = buildPayload(options);
  try {
    const response = await fetch("/api/monthly", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      return;
    }
    renderTable(monthlyTable, data.columns, data.rows, { percentAll: true });
  } catch (error) {
    return;
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  submitQuery();
  submitMonthly();
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
