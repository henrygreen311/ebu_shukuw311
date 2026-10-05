/* Arb: reads the arb_coins table from Supabase and renders it.

   Connection pattern (same as the QBOT dashboard): one Supabase client built from the
   CONFIG block below with the anon key, then a polling loop that re-reads the table every
   few seconds. No realtime setup is needed; the anon role only needs a SELECT policy.

   Sections: config, helpers, state, data layer, selectors, rendering, events, start. */
(function () {
  "use strict";

  /* ------------------------------------------------------------------ */
  /* Config                                                              */
  /* ------------------------------------------------------------------ */
  const CONFIG = {
    SUPABASE_URL: "https://gcmoppkkplzztiayvbdk.supabase.co",
    SUPABASE_ANON_KEY: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdjbW9wcGtrcGx6enRpYXl2YmRrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE2MTg5MTMsImV4cCI6MjA4NzE5NDkxM30.ZSgnOL471BMBIeDMlOp-RhuXGLk51rqDNektdoYHmC4",
    TABLE: "arb_coins",
    POLL_MS: 5000,      // how often the table is re-read
    PAGE_SIZE: 1000,    // Supabase returns at most 1000 rows per request
    FRESH_MS: 6000,     // how long a newly arrived row stays marked
  };

  const client = window.supabase
    ? window.supabase.createClient(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_ANON_KEY)
    : null;

  /* ------------------------------------------------------------------ */
  /* Helpers                                                             */
  /* ------------------------------------------------------------------ */
  const $ = (selector, root = document) => root.querySelector(selector);

  const ESCAPES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => ESCAPES[c]);

  const toNumber = (value) => {
    if (value === null || value === undefined || value === "") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const fmt = {
    price(p) {
      if (p === null) return "–";
      const digits = p >= 1 ? 4 : p >= 0.0001 ? 6 : 8;
      return p.toLocaleString("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    },
    signed(n, digits) {
      if (n === null) return "–";
      const body = Math.abs(n).toLocaleString("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
      return (n > 0 ? "+" : n < 0 ? "−" : "") + body;
    },
    ago(date) {
      if (!date) return "–";
      const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
      if (seconds < 60) return "Just now";
      const minutes = Math.round(seconds / 60);
      if (minutes < 60) return `${minutes} min ago`;
      const hours = Math.round(minutes / 60);
      if (hours < 24) return `${hours} h ago`;
      const days = Math.round(hours / 24);
      if (days < 30) return `${days} d ago`;
      return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
    },
    clock(date) {
      return date.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    },
  };

  const ICON_COPY =
    '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" ' +
    'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<rect x="9" y="9" width="11" height="11" rx="2"/>' +
    '<path d="M5 15V6a2 2 0 0 1 2-2h9"/></svg>';

  /* ------------------------------------------------------------------ */
  /* State                                                               */
  /* ------------------------------------------------------------------ */
  const state = {
    rows: [],
    signature: "",
    filters: { q: "", exchange: "", network: "", minProfit: "", sort: "newest" },
    connection: "connecting", // connecting | live | error
    loading: true,
    loaded: false,
    error: null,
    updatedAt: null,
    fresh: new Set(),
  };

  /* ------------------------------------------------------------------ */
  /* Data layer                                                          */
  /* ------------------------------------------------------------------ */
  async function fetchAll() {
    if (!client) {
      throw new Error("The Supabase library did not load. Check your internet connection and reload.");
    }
    const rows = [];
    for (let from = 0; ; from += CONFIG.PAGE_SIZE) {
      const { data, error } = await client
        .from(CONFIG.TABLE)
        .select("*")
        .order("id", { ascending: false })
        .range(from, from + CONFIG.PAGE_SIZE - 1);
      if (error) throw error;
      rows.push(...(data || []));
      if (!data || data.length < CONFIG.PAGE_SIZE) break;
    }
    return rows;
  }

  function normalize(raw) {
    const symbol = String(raw.symbol ?? "");
    const [base, quote] = symbol.split("/");
    const created = raw.created_at ? new Date(raw.created_at) : null;
    return {
      id: raw.id,
      symbol,
      base: base || symbol || "–",
      quote: quote || "",
      gap: toNumber(raw.real_gap_pct),
      profit: toNumber(raw.profit_usdt) ?? 0,
      buyEx: raw.buy_exchange || "–",
      buyPrice: toNumber(raw.buy_price),
      sellEx: raw.sell_exchange || "–",
      sellPrice: toNumber(raw.sell_price),
      network: raw.network || "–",
      created: created && !Number.isNaN(created.getTime()) ? created : null,
    };
  }

  function describeError(error) {
    const message = String(error?.message || error?.hint || "");
    if (error?.code === "42501" || /permission denied|row-level security/i.test(message)) {
      return `Supabase blocked the read. Check that Row Level Security allows anon SELECT on ${CONFIG.TABLE}.`;
    }
    if (/Failed to fetch|NetworkError|Load failed/i.test(message)) {
      return "The page could not reach Supabase. Check your internet connection.";
    }
    if (/Invalid API key|JWT/i.test(message)) {
      return "Supabase rejected the key. Check SUPABASE_URL and SUPABASE_ANON_KEY in index.js.";
    }
    return (
      message ||
      `Check the SUPABASE_URL and SUPABASE_ANON_KEY values in index.js and that Row Level Security allows anon SELECT on ${CONFIG.TABLE}.`
    );
  }

  /* ------------------------------------------------------------------ */
  /* Selectors                                                           */
  /* ------------------------------------------------------------------ */
  const compareIds = (a, b) =>
    typeof a === "number" && typeof b === "number"
      ? a - b
      : String(a).localeCompare(String(b), "en", { numeric: true });

  const sorters = {
    newest: (a, b) =>
      (b.created?.getTime() ?? 0) - (a.created?.getTime() ?? 0) || compareIds(b.id, a.id),
    profit: (a, b) => b.profit - a.profit,
    gap: (a, b) => (b.gap ?? -Infinity) - (a.gap ?? -Infinity),
    symbol: (a, b) => a.symbol.localeCompare(b.symbol),
  };

  function visibleRows() {
    const { q, exchange, network, minProfit, sort } = state.filters;
    const needle = q.trim().toLowerCase();
    const min = toNumber(minProfit);

    return state.rows
      .filter((r) => !needle || r.symbol.toLowerCase().includes(needle))
      .filter((r) => !exchange || r.buyEx === exchange || r.sellEx === exchange)
      .filter((r) => !network || r.network === network)
      .filter((r) => min === null || r.profit >= min)
      .sort(sorters[sort] || sorters.newest);
  }

  /* ------------------------------------------------------------------ */
  /* Rendering                                                           */
  /* ------------------------------------------------------------------ */
  const els = {
    status: $("#status"),
    statusText: $("#status-text"),
    refresh: $("#refresh"),
    banner: $("#error-banner"),
    bannerText: $("#error-text"),
    sumShowing: $("#sum-showing"),
    sumBest: $("#sum-best"),
    sumAvg: $("#sum-avg"),
    filters: $("#filters"),
    fQ: $("#f-q"),
    fExchange: $("#f-exchange"),
    fNetwork: $("#f-network"),
    fMin: $("#f-min"),
    fSort: $("#f-sort"),
    clearBtn: $("#clear-filters"),
    board: $("#board"),
    head: $("#head"),
    list: $("#list"),
    stateBox: $("#state"),
    toast: $("#toast"),
  };

  const pollSeconds = Math.round(CONFIG.POLL_MS / 1000);

  function renderStatus() {
    let text = "Connecting";
    if (state.error) {
      text = "Connection problem";
    } else if (state.connection === "live" && state.updatedAt) {
      text = `Synced ${fmt.clock(state.updatedAt)}, every ${pollSeconds}s`;
    }
    els.status.dataset.state = state.error ? "error" : state.connection;
    els.statusText.textContent = text;

    els.banner.classList.toggle("show", Boolean(state.error));
    if (state.error) {
      els.bannerText.innerHTML =
        `<strong>Connection issue.</strong> ${esc(state.error)} Retrying every ${pollSeconds}s…`;
    }
  }

  function renderSummary(rows) {
    const total = state.rows.length;
    els.sumShowing.textContent = rows.length === total ? String(total) : `${rows.length} of ${total}`;

    if (!rows.length) {
      els.sumBest.textContent = "–";
      els.sumAvg.textContent = "–";
      return;
    }
    const best = rows.reduce((m, r) => Math.max(m, r.profit), -Infinity);
    const avg = rows.reduce((sum, r) => sum + r.profit, 0) / rows.length;
    els.sumBest.textContent = `${fmt.signed(best, 2)} USDT`;
    els.sumAvg.textContent = `${fmt.signed(avg, 2)} USDT`;
  }

  function syncSelect(select, key, values, anyLabel) {
    const signature = values.join("\u0001");
    if (select.dataset.signature === signature) return;
    select.dataset.signature = signature;

    const wanted = state.filters[key];
    select.innerHTML =
      `<option value="">${esc(anyLabel)}</option>` +
      values.map((v) => `<option value="${esc(v)}">${esc(v)}</option>`).join("");

    if (values.includes(wanted)) {
      select.value = wanted;
    } else {
      select.value = "";
      state.filters[key] = "";
    }
  }

  function renderFilterOptions() {
    const exchanges = new Set();
    const networks = new Set();
    for (const r of state.rows) {
      exchanges.add(r.buyEx);
      exchanges.add(r.sellEx);
      networks.add(r.network);
    }
    const sorted = (set) => [...set].filter((v) => v && v !== "–").sort((a, b) => a.localeCompare(b));
    syncSelect(els.fExchange, "exchange", sorted(exchanges), "Any exchange");
    syncSelect(els.fNetwork, "network", sorted(networks), "Any network");
  }

  function rowHtml(r, maxProfit) {
    const width = maxProfit > 0 ? Math.max(4, Math.round((Math.max(r.profit, 0) / maxProfit) * 100)) : 0;
    const fresh = state.fresh.has(r.id) ? " is-fresh" : "";
    const gap = r.gap === null ? "–" : `${fmt.signed(r.gap, 2)}%`;
    const found = r.created
      ? `<span title="${esc(r.created.toLocaleString())}">${esc(fmt.ago(r.created))}</span>`
      : "–";

    return `
<article class="row${fresh}" role="row">
  <div class="cell pair" role="cell">
    <span class="base">${esc(r.base)}</span><span class="quote">${r.quote ? "/" + esc(r.quote) : ""}</span>
    <button type="button" class="copy" data-copy="${esc(r.base)}" aria-label="Copy ${esc(r.base)}" title="Copy ${esc(r.base)}">${ICON_COPY}</button>
  </div>
  <div class="cell route" role="cell">
    <div class="leg leg-buy"><span class="role">Buy on</span><span class="ex">${esc(r.buyEx)}</span><span class="px">${fmt.price(r.buyPrice)}</span></div>
    <span class="route-line" aria-hidden="true"></span>
    <div class="leg leg-sell"><span class="role">Sell on</span><span class="ex">${esc(r.sellEx)}</span><span class="px">${fmt.price(r.sellPrice)}</span></div>
  </div>
  <div class="cell gap" role="cell" data-label="Real gap">${gap}</div>
  <div class="cell profit" role="cell">
    <span class="amt" title="${r.profit.toFixed(4)} USDT">${fmt.signed(r.profit, 2)}<small>USDT</small></span>
    <span class="bar" aria-hidden="true"><i style="width:${width}%"></i></span>
  </div>
  <div class="cell net" role="cell" data-label="Network"><span class="chip" title="${esc(r.network)}">${esc(r.network)}</span></div>
  <div class="cell found" role="cell" data-label="Found">${found}</div>
</article>`;
  }

  function stateHtml(kind) {
    switch (kind) {
      case "loading":
        return "<h2>Loading routes</h2><p>Reading the arb_coins table.</p>";
      case "error":
        return (
          "<h2>Could not load routes</h2>" +
          `<p>Arb keeps retrying every ${pollSeconds}s. You can also try now.</p>` +
          '<button type="button" class="btn-primary" data-action="retry">Try again</button>'
        );
      case "empty":
        return (
          "<h2>No routes saved yet</h2>" +
          "<p>The tracker saves a coin here when a confirmed route clears the minimum profit. " +
          "If you expect rows, check that Row Level Security allows anon SELECT on arb_coins.</p>"
        );
      default:
        return (
          "<h2>No routes match these filters</h2>" +
          "<p>Try a different coin or exchange, or lower the minimum profit.</p>" +
          '<button type="button" class="btn-quiet" data-action="clear">Clear filters</button>'
        );
    }
  }

  function renderList(rows) {
    let kind = null;
    if (state.error && !state.rows.length) kind = "error";
    else if (state.loading && !state.rows.length) kind = "loading";
    else if (!state.rows.length) kind = "empty";
    else if (!rows.length) kind = "none";

    els.board.setAttribute("aria-busy", String(state.loading));
    els.head.hidden = kind !== null;
    els.stateBox.hidden = kind === null;
    if (kind) els.stateBox.innerHTML = stateHtml(kind);

    const maxProfit = rows.reduce((m, r) => Math.max(m, r.profit), 0);
    els.list.innerHTML = rows.map((r) => rowHtml(r, maxProfit)).join("");
    els.board.classList.toggle("no-found", !state.rows.some((r) => r.created));
  }

  function render() {
    const rows = visibleRows();
    renderStatus();
    renderFilterOptions();
    renderSummary(rows);
    renderList(rows);
    document.title = state.rows.length ? `(${state.rows.length}) Arb` : "Arb";
  }

  function showToast(message) {
    els.toast.textContent = message;
    els.toast.hidden = false;
    clearTimeout(showToast.timer);
    showToast.timer = setTimeout(() => { els.toast.hidden = true; }, 1800);
  }

  /* ------------------------------------------------------------------ */
  /* Polling loop                                                        */
  /* ------------------------------------------------------------------ */
  function markFresh(id) {
    state.fresh.add(id);
    setTimeout(() => state.fresh.delete(id), CONFIG.FRESH_MS);
  }

  async function pollOnce() {
    const rows = (await fetchAll()).map(normalize);

    // Rows that were not in the previous read are new since the last poll.
    if (state.loaded) {
      const known = new Set(state.rows.map((r) => r.id));
      for (const r of rows) if (!known.has(r.id)) markFresh(r.id);
    }

    const signature = JSON.stringify(rows);
    const changed = signature !== state.signature;

    state.rows = rows;
    state.signature = signature;
    state.loaded = true;
    state.loading = false;
    state.error = null;
    state.connection = "live";
    state.updatedAt = new Date();

    // Rebuilding an unchanged list every poll would replay the "new row" highlight.
    if (changed || els.board.getAttribute("aria-busy") === "true") render();
    else renderStatus();
  }

  async function runPoll() {
    try {
      await pollOnce();
    } catch (err) {
      console.error("[arb] poll failed:", err);
      state.error = describeError(err);
      state.loading = false;
      state.connection = "error";
      render();
    }
  }

  async function loop() {
    await runPoll();
    setTimeout(loop, CONFIG.POLL_MS);
  }

  async function refreshNow() {
    els.refresh.classList.add("is-spinning");
    await runPoll();
    els.refresh.classList.remove("is-spinning");
  }

  /* ------------------------------------------------------------------ */
  /* Events                                                              */
  /* ------------------------------------------------------------------ */
  function readFilters() {
    state.filters = {
      q: els.fQ.value,
      exchange: els.fExchange.value,
      network: els.fNetwork.value,
      minProfit: els.fMin.value,
      sort: els.fSort.value,
    };
  }

  function clearFilters() {
    els.filters.reset();
    els.fSort.value = "newest";
    readFilters();
    render();
  }

  els.filters.addEventListener("input", () => { readFilters(); render(); });
  els.filters.addEventListener("submit", (e) => e.preventDefault());
  els.clearBtn.addEventListener("click", clearFilters);
  els.refresh.addEventListener("click", refreshNow);

  document.addEventListener("click", async (event) => {
    const copyBtn = event.target.closest("[data-copy]");
    if (copyBtn) {
      const text = copyBtn.dataset.copy;
      try {
        await navigator.clipboard.writeText(text);
        showToast(`Copied ${text}`);
      } catch (e) {
        showToast("Copy is blocked in this browser");
      }
      return;
    }
    const action = event.target.closest("[data-action]")?.dataset.action;
    if (action === "retry") refreshNow();
    else if (action === "clear") clearFilters();
  });

  // Keep "5 min ago" labels honest between data changes.
  setInterval(() => {
    if (state.rows.some((r) => r.created)) renderList(visibleRows());
  }, 60000);

  /* ------------------------------------------------------------------ */
  /* Start                                                               */
  /* ------------------------------------------------------------------ */
  render();
  loop();
})();
