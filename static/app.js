const form = document.getElementById("query-form");
const cacheOnlyButton = document.getElementById("cache-only");
const updateDataButton = document.getElementById("update-data");
const statusEl = document.getElementById("status");
const currentTickerEl = document.getElementById("current-ticker");
const resultsTable = document.getElementById("results-table");
const dataTable = document.getElementById("data-table");
const variationTable = document.getElementById("variation-table");
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
let cachedRawData = null;
let variationMeta = null;
const variationRange = document.getElementById("variation-range");
const variationValue = document.getElementById("variation-value");

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

  const priceColumns = new Set(["open", "high", "low", "close", "lower", "higher"]);
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
    if (!options.disableSort) {
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
      renderTable(tableEl, columns, sorted, options);
      });
    }
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

function lowerBound(sortedValues, target) {
  let left = 0;
  let right = sortedValues.length;
  while (left < right) {
    const mid = Math.floor((left + right) / 2);
    if (sortedValues[mid] < target) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

function upperBound(sortedValues, target) {
  let left = 0;
  let right = sortedValues.length;
  while (left < right) {
    const mid = Math.floor((left + right) / 2);
    if (sortedValues[mid] <= target) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

function createSegmentTree(size, initialValue, combine) {
  let n = 1;
  while (n < size) {
    n *= 2;
  }
  const tree = new Array(2 * n).fill(initialValue);
  return {
    update(index, value) {
      let i = index + n;
      tree[i] = value;
      i = Math.floor(i / 2);
      while (i >= 1) {
        tree[i] = combine(tree[i * 2], tree[i * 2 + 1]);
        i = Math.floor(i / 2);
      }
    },
    query(left, right) {
      if (right < left) {
        return initialValue;
      }
      let l = left + n;
      let r = right + n;
      let result = initialValue;
      while (l <= r) {
        if (l % 2 === 1) {
          result = combine(result, tree[l]);
          l += 1;
        }
        if (r % 2 === 0) {
          result = combine(result, tree[r]);
          r -= 1;
        }
        l = Math.floor(l / 2);
        r = Math.floor(r / 2);
      }
      return result;
    },
  };
}

function updateVariationValueDisplay(value) {
  if (variationValue) {
    variationValue.textContent = Number.isFinite(value) ? value.toFixed(1) : "1";
  }
}

function computeVariationTable() {
  if (!variationTable || !cachedRawData) {
    return;
  }
  const { columns, rows } = cachedRawData;
  const dateIndex = columns.indexOf("date");
  const closeIndex = columns.indexOf("close");
  if (dateIndex === -1 || closeIndex === -1) {
    clearTable(variationTable);
    return;
  }
  const percent = variationRange ? Number(variationRange.value || 1) : 1;
  updateVariationValueDisplay(percent);
  const orderedRows = rows
    .map((row, rowIndex) => {
      const dateValue = row[dateIndex];
      const timestamp = Date.parse(dateValue);
      return {
        row,
        rowIndex,
        dateValue,
        timestamp: Number.isNaN(timestamp) ? rowIndex : timestamp,
        closeValue: Number(row[closeIndex]),
      };
    })
    .sort((a, b) => a.timestamp - b.timestamp);
  const closes = orderedRows.map((entry) => entry.closeValue);
  const dates = orderedRows.map((entry) => entry.dateValue);
  const lows = new Array(orderedRows.length).fill(null);
  const highs = new Array(orderedRows.length).fill(null);
  const uniqueCloses = Array.from(
    new Set(closes.filter((value) => Number.isFinite(value)))
  ).sort((a, b) => a - b);
  const timestamps = orderedRows.map((entry, index) => {
    const parsed = Date.parse(entry.dateValue);
    return Number.isNaN(parsed) ? index * 86400000 : parsed;
  });
  const pastTree = createSegmentTree(uniqueCloses.length, -1, Math.max);
  const futureTree = createSegmentTree(uniqueCloses.length, Infinity, Math.min);
  const futureOffsets = new Array(orderedRows.length).fill(0);

  for (let i = orderedRows.length - 1; i >= 0; i -= 1) {
    const closeValue = closes[i];
    if (!Number.isFinite(closeValue)) {
      continue;
    }
    const tolerance = percent / 100;
    const low = closeValue * (1 - tolerance);
    const high = closeValue * (1 + tolerance);
    lows[i] = low;
    highs[i] = high;
    const left = lowerBound(uniqueCloses, low);
    const right = upperBound(uniqueCloses, high) - 1;
    const futureIndex = futureTree.query(left, right);
    if (Number.isFinite(futureIndex)) {
      const diffMs = timestamps[futureIndex] - timestamps[i];
      futureOffsets[i] = Math.round(diffMs / 86400000);
    } else {
      futureOffsets[i] = 0;
    }
    const closeIdx = lowerBound(uniqueCloses, closeValue);
    futureTree.update(closeIdx, i);
  }

  const outputRows = orderedRows.map((entry, index) => {
    const row = entry.row;
    const closeValue = closes[index];
    let pastMatches = 0;
    let futureMatches = futureOffsets[index];
    let lower = "";
    let higher = "";
    if (Number.isFinite(closeValue)) {
      const tolerance = percent / 100;
      const low = closeValue * (1 - tolerance);
      const high = closeValue * (1 + tolerance);
      lower = low;
      higher = high;
      const left = lowerBound(uniqueCloses, low);
      const right = upperBound(uniqueCloses, high) - 1;
      const pastIndex = pastTree.query(left, right);
      if (pastIndex >= 0) {
        const diffMs = timestamps[index] - timestamps[pastIndex];
        pastMatches = Math.round(diffMs / 86400000);
      } else {
        pastMatches = 0;
      }
      const closeIdx = lowerBound(uniqueCloses, closeValue);
      pastTree.update(closeIdx, index);
    }
    return [
      dates[index],
      row[closeIndex],
      lower,
      higher,
      pastMatches,
      futureMatches,
    ];
  });

  const outputColumns = [
    "date",
    "close",
    "lower",
    "higher",
    `past days within +/-${percent.toFixed(1)}%`,
    `future days within +/-${percent.toFixed(1)}%`,
  ];
  renderTable(variationTable, outputColumns, outputRows, { disableSort: true });
  const rowNodes = variationTable.querySelectorAll("tbody tr");
  rowNodes.forEach((rowNode, index) => {
    rowNode.dataset.index = String(index);
    const closeCell = rowNode.children[1];
    if (closeCell) {
      closeCell.dataset.role = "variation-close";
    }
  });
  variationMeta = {
    closes,
    lows,
    highs,
  };
}

function clearVariationHighlights() {
  if (!variationTable) {
    return;
  }
  variationTable
    .querySelectorAll(".variation-match-past, .variation-match-future, .variation-match-current")
    .forEach((cell) => {
      cell.classList.remove(
        "variation-match-past",
        "variation-match-future",
        "variation-match-current"
      );
    });
}

function applyVariationHighlights(targetIndex) {
  if (!variationTable || !variationMeta) {
    return;
  }
  const low = variationMeta.lows[targetIndex];
  const high = variationMeta.highs[targetIndex];
  if (!Number.isFinite(low) || !Number.isFinite(high)) {
    clearVariationHighlights();
    return;
  }
  const rowNodes = variationTable.querySelectorAll("tbody tr");
  rowNodes.forEach((rowNode, index) => {
    const closeValue = variationMeta.closes[index];
    if (!Number.isFinite(closeValue)) {
      return;
    }
    if (closeValue < low || closeValue > high) {
      return;
    }
    const closeCell = rowNode.children[1];
    if (!closeCell) {
      return;
    }
    if (index < targetIndex) {
      closeCell.classList.add("variation-match-past");
    } else if (index > targetIndex) {
      closeCell.classList.add("variation-match-future");
    } else {
      closeCell.classList.add("variation-match-current");
    }
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
      if (options.target === "data") {
        cachedRawData = null;
        clearTable(variationTable);
      }
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
    if (options.target === "data") {
      cachedRawData = { columns: data.columns, rows: data.rows };
      computeVariationTable();
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
    if (options.target === "data") {
      cachedRawData = null;
      clearTable(variationTable);
    }
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
  if (variationRange) {
    updateVariationValueDisplay(Number(variationRange.value || 1));
  }
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

if (variationTable) {
  variationTable.addEventListener("mouseover", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.dataset.role !== "variation-close") {
      return;
    }
    const row = target.closest("tr");
    if (!row || row.dataset.index === undefined) {
      return;
    }
    const index = Number(row.dataset.index);
    if (!Number.isFinite(index)) {
      return;
    }
    clearVariationHighlights();
    applyVariationHighlights(index);
  });

  variationTable.addEventListener("mouseleave", () => {
    clearVariationHighlights();
  });
}

if (variationRange) {
  variationRange.addEventListener("input", () => {
    updateVariationValueDisplay(Number(variationRange.value || 1));
    computeVariationTable();
  });
}

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
