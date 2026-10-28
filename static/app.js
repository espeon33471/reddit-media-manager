/* Gedeelde helpers voor de Reddit Media Manager UI. */
"use strict";

const RMM = {
  $: (sel, el = document) => el.querySelector(sel),
  $$: (sel, el = document) => Array.from(el.querySelectorAll(sel)),

  async api(url, opts = {}) {
    const options = { headers: { "Content-Type": "application/json" }, ...opts };
    let res;
    try {
      res = await fetch(url, options);
    } catch (e) {
      throw new Error("Geen verbinding met de server: " + e.message);
    }
    if (!res.ok) {
      let detail = res.statusText;
      try {
        const body = await res.json();
        detail = body.detail || detail;
        if (typeof detail !== "string") detail = JSON.stringify(detail);
      } catch (_) { /* niet-JSON fout */ }
      throw new Error(detail);
    }
    return res.json();
  },

  toast(msg, kind = "ok", ms = 4200) {
    const el = document.getElementById("toast");
    if (!el) return;
    el.textContent = msg;
    el.className = "toast " + kind;
    clearTimeout(RMM._toastTimer);
    RMM._toastTimer = setTimeout(() => el.classList.add("hidden"), ms);
  },

  fmtBytes(n) {
    if (!isFinite(n) || n <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    let i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return n.toFixed(n >= 100 || i === 0 ? 0 : 1) + " " + units[i];
  },

  fmtDate(s) {
    if (!s) return "—";
    const d = new Date(s);
    return isNaN(d) ? s : d.toLocaleString();
  },

  esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, c => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[c]));
  },

  statusBadge(state) {
    return `<span class="badge badge-${state}">${state}</span>`;
  },

  _engineTimer: null,
  startEngineBadgePoll() {
    if (RMM._engineTimer) return;
    const tick = async () => {
      try {
        const st = await RMM.api("/api/status");
        const badge = document.getElementById("engine-badge");
        if (badge) {
          badge.textContent = st.state;
          badge.className = "badge badge-" + st.state;
        }
        if (window.RMM_onStatus) window.RMM_onStatus(st);
      } catch (_) { /* server even onbereikbaar */ }
    };
    tick();
    RMM._engineTimer = setInterval(tick, 2500);
  },

  /**
   * Make a table's column headers clickable for sorting.
   * @param {string} tableId - id of the <table> or its parent <div> containing the table
   * @param {number} [excludeCols] - array of column indices to exclude from sorting (e.g. action buttons)
   */
  sortableTable(tableId, excludeCols = []) {
    const container = document.getElementById(tableId);
    if (!container) return;
    const table = container.tagName === "TABLE" ? container : container.querySelector("table");
    if (!table) return;
    const ths = Array.from(table.querySelectorAll("thead th"));
    const tbody = table.querySelector("tbody");
    if (!tbody) return;

    let sortCol = -1;
    let sortDir = 1; // 1 = asc, -1 = desc

    ths.forEach((th, colIdx) => {
      if (excludeCols.includes(colIdx)) return;
      th.style.cursor = "pointer";
      th.style.userSelect = "none";
      th.title = "Klik om te sorteren";

      th.addEventListener("click", () => {
        if (sortCol === colIdx) {
          sortDir *= -1;
        } else {
          sortCol = colIdx;
          sortDir = 1;
        }

        // Update indicators
        ths.forEach((t, i) => {
          if (excludeCols.includes(i)) return;
          t.textContent = t.textContent.replace(/\s*[▲▼]\s*$/, "");
          if (i === sortCol) {
            t.textContent += sortDir === 1 ? " ▲" : " ▼";
          }
        });

        // Get all data rows
        const rows = Array.from(tbody.querySelectorAll("tr")).filter(r => r.dataset.sortable !== "false");
        rows.sort((a, b) => {
          const cellA = a.cells[colIdx]?.textContent.trim() || "";
          const cellB = b.cells[colIdx]?.textContent.trim() || "";
          // Try numeric comparison first
          const numA = parseFloat(cellA);
          const numB = parseFloat(cellB);
          if (!isNaN(numA) && !isNaN(numB) && cellA !== "" && cellB !== "") {
            return (numA - numB) * sortDir;
          }
          return cellA.localeCompare(cellB, "nl") * sortDir;
        });
        rows.forEach(r => tbody.appendChild(r));
      });
    });
  }
};
