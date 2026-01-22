package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type HTTPConfig struct {
	Addr       string
	LocNY      *time.Location
	Log        *Logger
	Broker     *Broker
	Store      *Storage
	CH         *ClickHouseClient
	RunID      string
	RunStartNY time.Time
	M          *Metrics
}

type HTTPServer struct {
	cfg HTTPConfig
	srv *http.Server
}

func NewHTTPServer(cfg HTTPConfig) *http.Server {
	mux := http.NewServeMux()
	hs := &HTTPServer{cfg: cfg}

	// New: ad-hoc SQL UI (ClickHouse)
	mux.HandleFunc("/", hs.handleQueryUI)
	mux.HandleFunc("/query", hs.handleQuery)
	mux.HandleFunc("/run", hs.handleRun)

	// Keep legacy dashboard (optional)
	mux.HandleFunc("/dashboard", hs.handleDashboardLegacy)

	// Existing endpoints
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/symbols", hs.handleSymbols)
	mux.HandleFunc("/stats/now", hs.handleStatsNow)

	// These now prefer ClickHouse, but fall back to file/in-mem if CH not available.
	mux.HandleFunc("/rank/afterhours", hs.handleRankAfterHours)
	mux.HandleFunc("/rank/window", hs.handleRankWindow)

	hs.srv = &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	return hs.srv
}

func (hs *HTTPServer) handleRun(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"run_id":        hs.cfg.RunID,
		"run_start_ny":  hs.cfg.RunStartNY.Format(time.RFC3339Nano),
		"run_start_ms":  hs.cfg.RunStartNY.UnixMilli(),
		"clickhouse_on": hs.cfg.CH != nil,
	})
}

func (hs *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	snap := hs.cfg.M.Snapshot()

	snap["run"] = map[string]any{
		"run_id":       hs.cfg.RunID,
		"run_start_ny": hs.cfg.RunStartNY.Format(time.RFC3339Nano),
		"run_start_ms": hs.cfg.RunStartNY.UnixMilli(),
	}

	if hs.cfg.CH != nil {
		snap["clickhouse_conn"] = map[string]any{
			"enabled": true,
			"addr":    hs.cfg.CH.Addr(),
			"db":      hs.cfg.CH.Database(),
			"secure":  hs.cfg.CH.Secure(),
		}
	} else {
		snap["clickhouse_conn"] = map[string]any{"enabled": false}
	}

	writeJSON(w, snap)
}

func (hs *HTTPServer) handleSymbols(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"count":   len(hs.cfg.Broker.Symbols()),
		"symbols": hs.cfg.Broker.SnapshotSymbols(),
	}
	writeJSON(w, resp)
}

func (hs *HTTPServer) handleStatsNow(w http.ResponseWriter, r *http.Request) {
	// Kept as in-memory/file behavior (fast, and still useful).
	mins := 5
	if q := r.URL.Query().Get("minutes"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 && v <= 120 {
			mins = v
		}
	}
	nowNY := time.Now().In(hs.cfg.LocNY)
	date := nowNY.Format("2006-01-02")
	session := SessionForNY(nowNY)
	curMinute := nowNY.Format("15:04")

	type row struct {
		Symbol string `json:"symbol"`

		Minute  string `json:"minute"`
		Session string `json:"session"`

		Now    RankRow `json:"now"`
		Window RankRow `json:"window"`
	}

	symbols := hs.cfg.Broker.Symbols()
	out := make([]row, 0, len(symbols))

	for _, sym := range symbols {
		// current minute totals
		kNow := AggKey{Date: date, Minute: curMinute, Session: session, Symbol: sym}
		nowCounts, _ := hs.cfg.Store.GetAggTotals(kNow)

		// window totals (same session only)
		var win AggCounts
		for i := 0; i < mins; i++ {
			tm := nowNY.Add(-time.Duration(i) * time.Minute)
			minStr := tm.Format("15:04")
			k := AggKey{Date: date, Minute: minStr, Session: session, Symbol: sym}
			c, ok := hs.cfg.Store.GetAggTotals(k)
			if !ok {
				continue
			}
			win.TradesTotal += c.TradesTotal
			win.AboveAsk += c.AboveAsk
			win.AtAsk += c.AtAsk
			win.MidToAsk += c.MidToAsk
			win.AtMid += c.AtMid
			win.MidToBid += c.MidToBid
			win.AtBid += c.AtBid
			win.BelowBid += c.BelowBid
		}

		out = append(out, row{
			Symbol:  sym,
			Minute:  curMinute,
			Session: session,
			Now:     countsToRank(sym, nowCounts),
			Window:  countsToRank(sym, win),
		})
	}

	resp := map[string]any{
		"asof_ny":  nowNY.Format(time.RFC3339),
		"date":     date,
		"session":  session,
		"minute":   curMinute,
		"minutes":  mins,
		"symbols":  out,
	}
	writeJSON(w, resp)
}

func (hs *HTTPServer) handleRankAfterHours(w http.ResponseWriter, r *http.Request) {
	nowNY := time.Now().In(hs.cfg.LocNY)
	dateStr := r.URL.Query().Get("date")
	if dateStr == "" {
		dateStr = nowNY.Format("2006-01-02")
	}

	sinceStr := r.URL.Query().Get("since")
	if sinceStr == "" {
		sinceStr = "16:00"
	}
	sinceMin, err := ParseSinceMinutes(sinceStr)
	if err != nil {
		http.Error(w, "bad since", http.StatusBadRequest)
		return
	}

	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}

	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		runID = hs.cfg.RunID
	}

	// Preferred: ClickHouse rollup
	if hs.cfg.CH != nil {
		startNY, endNY, err := ahStartEnd(hs.cfg.LocNY, dateStr, sinceMin)
		if err == nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()

			rows, qerr := hs.cfg.CH.RankAfterHours(ctx, runID, startNY, endNY, limit)
			if qerr == nil {
				writeJSON(w, map[string]any{
					"source": "clickhouse",
					"date":   dateStr,
					"since":  sinceStr,
					"limit":  limit,
					"run_id": runID,
					"rows":   rows,
				})
				return
			}
			hs.cfg.Log.Warnf("clickhouse /rank/afterhours failed, falling back to file: %v", qerr)
		}
	}

	// Fallback: file cache (original behavior)
	rows, err := hs.cfg.Store.RankAfterHoursFromFile(dateStr, sinceMin, hs.cfg.Broker.Symbols(), limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("rank error: %v", err), http.StatusNotFound)
		return
	}

	writeJSON(w, map[string]any{
		"source": "file",
		"date":   dateStr,
		"since":  sinceStr,
		"limit":  limit,
		"run_id": runID,
		"rows":   rows,
	})
}

func (hs *HTTPServer) handleRankWindow(w http.ResponseWriter, r *http.Request) {
	mins := 30
	if q := r.URL.Query().Get("minutes"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 && v <= 240 {
			mins = v
		}
	}
	session := r.URL.Query().Get("session")

	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}

	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		runID = hs.cfg.RunID
	}

	nowNY := time.Now().In(hs.cfg.LocNY)
	date := nowNY.Format("2006-01-02")
	if session == "" {
		session = SessionForNY(nowNY)
	}

	// Preferred: ClickHouse rollup (precise minute boundaries)
	if hs.cfg.CH != nil {
		endNY := nowNY.Truncate(time.Minute)
		startNY := endNY.Add(-time.Duration(mins-1) * time.Minute)

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		rows, err := hs.cfg.CH.RankWindow(ctx, runID, startNY, endNY, session, limit)
		if err == nil {
			writeJSON(w, map[string]any{
				"source":  "clickhouse",
				"date":    date,
				"session": session,
				"minutes": mins,
				"limit":   limit,
				"run_id":  runID,
				"rows":    rows,
			})
			return
		}
		hs.cfg.Log.Warnf("clickhouse /rank/window failed, falling back to in-mem: %v", err)
	}

	// Fallback: in-memory agg totals (original behavior)
	start := nowNY.Add(-time.Duration(mins-1) * time.Minute)
	symbols := hs.cfg.Broker.Symbols()

	out := make([]RankRow, 0, len(symbols))
	for _, sym := range symbols {
		var sum AggCounts
		for i := 0; i < mins; i++ {
			tm := start.Add(time.Duration(i) * time.Minute)
			minStr := tm.Format("15:04")
			k := AggKey{Date: date, Minute: minStr, Session: session, Symbol: sym}
			c, ok := hs.cfg.Store.GetAggTotals(k)
			if !ok {
				continue
			}
			sum.TradesTotal += c.TradesTotal
			sum.AboveAsk += c.AboveAsk
			sum.AtAsk += c.AtAsk
			sum.MidToAsk += c.MidToAsk
			sum.AtMid += c.AtMid
			sum.MidToBid += c.MidToBid
			sum.AtBid += c.AtBid
			sum.BelowBid += c.BelowBid
		}
		if sum.TradesTotal == 0 {
			continue
		}
		out = append(out, countsToRank(sym, sum))
	}

	sortRank(out)
	if limit > len(out) {
		limit = len(out)
	}

	writeJSON(w, map[string]any{
		"source":  "inmem",
		"date":    date,
		"session": session,
		"minutes": mins,
		"limit":   limit,
		"run_id":  runID,
		"rows":    out[:limit],
	})
}

func countsToRank(sym string, c AggCounts) RankRow {
	askish := c.Askish()
	bidish := c.Bidish()
	total := c.TradesTotal
	ratio := float64(askish) / float64(max64(1, bidish))
	share := float64(askish) / float64(max64(1, total))
	return RankRow{
		Symbol: sym,
		Askish: askish,
		Bidish: bidish,
		Total:  total,
		Ratio:  ratio,
		Share:  share,
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

// -------------------- SQL UI --------------------

const defaultQuery = `SELECT
  symbol,
  sum(at_mid + mid_to_ask + at_ask + above_ask) AS askish,
  sum(mid_to_bid + at_bid + below_bid) AS bidish,
  sum(trades_total) AS total,
  askish / greatest(bidish, 1) AS ratio,
  askish / greatest(total, 1) AS share
FROM tape_trades_1m
WHERE run_id = '{RUN_ID}'
  AND minute >= now('America/New_York') - INTERVAL 60 MINUTE
  AND session IN ('PRE','AH','RTH')
GROUP BY symbol
ORDER BY ratio DESC, share DESC, askish DESC
LIMIT 50
`

func (hs *HTTPServer) handleQueryUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	enabled := hs.cfg.CH != nil
	msg := ""
	if !enabled {
		msg = "ClickHouse is not connected (queries will fail). Start ClickHouse or enable it via env/flags."
	}
	fmt.Fprintf(w, queryUIHTML(hs.cfg.RunID, msg))
}

type queryReq struct {
	SQL string `json:"sql"`
}

type queryResp struct {
	Columns   []string        `json:"columns"`
	Rows      [][]any         `json:"rows"`
	ElapsedMS int64           `json:"elapsed_ms"`
	Truncated bool            `json:"truncated,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func (hs *HTTPServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if hs.cfg.CH == nil || hs.cfg.CH.SQLDB() == nil {
		http.Error(w, "ClickHouse not configured", http.StatusServiceUnavailable)
		return
	}

	const maxBody = 1 << 20 // 1MB
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	defer r.Body.Close()

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	var req queryReq
	if err := json.Unmarshal(b, &req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	sqlText := strings.TrimSpace(req.SQL)
	sqlText = strings.ReplaceAll(sqlText, "{RUN_ID}", hs.cfg.RunID)

	allowDDL := os.Getenv("ALLOW_DDL") == "1"
	clean, ok, reason := validateQuery(sqlText, allowDDL)
	if !ok {
		http.Error(w, reason, http.StatusBadRequest)
		return
	}

	const maxRows = 5000
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := hs.cfg.CH.SQLDB().QueryContext(ctx, clean)
	if err != nil {
		writeJSON(w, queryResp{Error: err.Error(), ElapsedMS: time.Since(start).Milliseconds()})
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		writeJSON(w, queryResp{Error: err.Error(), ElapsedMS: time.Since(start).Milliseconds()})
		return
	}

	outRows := make([][]any, 0, 256)
	truncated := false

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			writeJSON(w, queryResp{Error: err.Error(), ElapsedMS: time.Since(start).Milliseconds()})
			return
		}

		for i := range vals {
			vals[i] = normalizeSQLValue(vals[i])
		}
		outRows = append(outRows, vals)

		if len(outRows) >= maxRows {
			truncated = true
			break
		}
	}

	writeJSON(w, queryResp{
		Columns:   cols,
		Rows:      outRows,
		ElapsedMS: time.Since(start).Milliseconds(),
		Truncated: truncated,
	})
}

func normalizeSQLValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	default:
		return v
	}
}

// Very simple local safety gate:
// - SELECT/WITH only by default
// - reject DDL/DML keywords unless ALLOW_DDL=1
// - reject multi-statement (semicolons) except a single trailing one.
func validateQuery(sqlText string, allowDDL bool) (clean string, ok bool, reason string) {
	s := stripLeadingComments(sqlText)
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false, "empty sql"
	}

	// allow a single trailing semicolon
	if strings.Contains(s, ";") {
		ss := strings.TrimSpace(s)
		if strings.HasSuffix(ss, ";") && strings.Count(ss, ";") == 1 {
			s = strings.TrimSpace(strings.TrimSuffix(ss, ";"))
		} else {
			return "", false, "multi-statement queries are not allowed"
		}
	}

	l := strings.ToLower(s)

	if !allowDDL {
		if !(strings.HasPrefix(l, "select") || strings.HasPrefix(l, "with")) {
			return "", false, "only SELECT queries are allowed (set ALLOW_DDL=1 to override)"
		}
		for _, bad := range []string{
			"insert", "alter", "drop", "create", "truncate", "optimize",
			"attach", "detach", "system", "grant", "revoke",
		} {
			if strings.Contains(l, bad) {
				return "", false, "query rejected (DDL/DML detected). Set ALLOW_DDL=1 to override."
			}
		}
	}

	return s, true, ""
}

func stripLeadingComments(s string) string {
	for {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "--") {
			if i := strings.Index(s, "\n"); i >= 0 {
				s = s[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return ""
		}
		return s
	}
}

func queryUIHTML(runID string, banner string) string {
	bannerHTML := ""
	if banner != "" {
		bannerHTML = fmt.Sprintf(`<div class="banner">%s</div>`, htmlEscape(banner))
	}
	return fmt.Sprintf(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>tickerhawk | ClickHouse SQL</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background:#0c0c0f; color:#eaeaea; margin:0; }
    header { padding:12px 16px; border-bottom:1px solid #1c1f25; background:#0d0d12; display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
    .pill { padding:6px 10px; border-radius:999px; background:#2a2f3a; font-size:12px; }
    .banner { margin: 12px 16px; padding: 10px 12px; border: 1px solid #2a2f3a; border-radius: 10px; background: #11131a; color: #cbd5e1; }
    main { max-width: 1200px; margin: 16px auto; padding: 0 16px; }
    textarea { width:100%%; height: 220px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 13px; line-height: 1.35; background:#0d0d12; color:#eaeaea; border:1px solid #2a2f3a; border-radius: 10px; padding: 10px; }
    button { background:#2563eb; border:none; color:white; padding:10px 14px; border-radius:10px; font-weight:600; cursor:pointer; }
    button:disabled { opacity: .6; cursor: not-allowed; }
    .row { display:flex; gap:10px; align-items:center; margin: 10px 0; flex-wrap:wrap; }
    .muted { color:#8a8f98; }
    table { width:100%%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
    th, td { padding:8px 10px; border-bottom:1px solid #1c1f25; text-align:left; vertical-align: top; }
    th { position: sticky; top: 0; background: #0d0d12; }
    pre { white-space: pre-wrap; }
    a { color:#93c5fd; text-decoration:none; }
  </style>
</head>
<body>
<header>
  <div><b>tickerhawk</b> <span class="muted">ClickHouse SQL</span></div>
  <div class="pill">run_id: <span id="runid">%s</span> · <a href="/dashboard">legacy dashboard</a></div>
</header>
%s
<main>
  <div class="row">
    <button id="run">Run</button>
    <span class="muted" id="meta"></span>
  </div>
  <textarea id="sql">%s</textarea>
  <div style="margin-top: 14px;">
    <div class="muted" id="err"></div>
    <div style="overflow:auto; max-height: 60vh; border:1px solid #1c1f25; border-radius: 10px;">
      <table id="tbl">
        <thead id="thead"></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
</main>
<script>
const btn = document.getElementById('run');
const ta = document.getElementById('sql');
const meta = document.getElementById('meta');
const err = document.getElementById('err');
const thead = document.getElementById('thead');
const tbody = document.getElementById('tbody');

function esc(s) {
  return (''+s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
}

async function runQuery() {
  btn.disabled = true;
  err.textContent = '';
  meta.textContent = 'running...';
  thead.innerHTML = '';
  tbody.innerHTML = '';

  try {
    const resp = await fetch('/query', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql: ta.value})
    });
    const data = await resp.json();

    if (data.error) {
      err.textContent = data.error;
      meta.textContent = 'error';
      return;
    }

    meta.textContent = 'elapsed=' + (data.elapsed_ms || 0) + 'ms' + (data.truncated ? ' (truncated)' : '');

    // header
    const cols = data.columns || [];
    const trh = document.createElement('tr');
    for (const c of cols) {
      const th = document.createElement('th');
      th.textContent = c;
      trh.appendChild(th);
    }
    thead.appendChild(trh);

    // rows
    for (const row of (data.rows || [])) {
      const tr = document.createElement('tr');
      for (const cell of row) {
        const td = document.createElement('td');
        td.innerHTML = esc(cell === null || cell === undefined ? '' : cell);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  } catch (e) {
    err.textContent = '' + e;
    meta.textContent = 'error';
  } finally {
    btn.disabled = false;
  }
}

btn.addEventListener('click', runQuery);
runQuery();
</script>
</body>
</html>`, htmlEscape(runID), bannerHTML, htmlEscape(defaultQuery))
}

func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	return s
}

// -------------------- legacy dashboard --------------------

func (hs *HTTPServer) handleDashboardLegacy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(w, dashboardHTML)
}

const dashboardHTML = `<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>tickerhawk-ah</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background:#0c0c0f; color:#eaeaea; margin:0; }
    header { display:flex; justify-content:space-between; align-items:center; padding:12px 16px; border-bottom:1px solid #1c1f25; background:#0d0d12; gap:12px; flex-wrap:wrap; }
    .pill { padding:6px 10px; border-radius:999px; background:#2a2f3a; font-size:12px; }
    main { max-width: 1100px; margin: 16px auto; padding: 0 16px; }
    table { width:100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
    th, td { padding:8px 10px; border-bottom:1px solid #1c1f25; text-align:right; }
    th:first-child, td:first-child { text-align:left; }
    .muted { color:#8a8f98; }
    a { color:#93c5fd; text-decoration:none; }
  </style>
</head>
<body>
<header>
  <div><b>tickerhawk</b> <span class="muted">after-hours</span></div>
  <div class="pill"><a href="/">SQL UI</a></div>
  <div class="pill" id="status">loading…</div>
</header>
<main>
  <div class="muted" id="meta"></div>
  <h3>Top after-hours ratio (since 16:00 ET)</h3>
  <table>
    <thead>
      <tr>
        <th>Symbol</th><th>Askish</th><th>Bidish</th><th>Total</th><th>Ratio</th><th>Share</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
</main>
<script>
async function tick() {
  try {
    const h = await fetch('/health', {cache:'no-store'}).then(r=>r.json());
    const date = new Date().toLocaleDateString('en-CA', {timeZone:'America/New_York'});
    const rank = await fetch('/rank/afterhours?limit=25&since=16:00&date='+date, {cache:'no-store'}).then(r=>r.json());

    const ok = h?.ok ? 'ok' : 'bad';
    const rate1 = (h?.ingest?.trades_per_s?.['1s'] ?? 0).toFixed(1);
    const rc = h?.reconnect?.success ?? 0;
    document.getElementById('status').textContent = ok + ' | trades/s=' + rate1 + ' | reconnects=' + rc;

    document.getElementById('meta').textContent =
      'NY date=' + (rank?.date || date) + ', source=' + (rank?.source || '?') + ', rows=' + (rank?.rows?.length || 0);

    const tbody = document.getElementById('rows');
    tbody.innerHTML = '';
    for (const r of (rank.rows || [])) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td><b>' + r.symbol + '</b></td>' +
        '<td>' + r.askish + '</td>' +
        '<td>' + r.bidish + '</td>' +
        '<td>' + r.total + '</td>' +
        '<td>' + (r.ratio || 0).toFixed(3) + '</td>' +
        '<td>' + (r.share || 0).toFixed(3) + '</td>';
      tbody.appendChild(tr);
    }
  } catch (e) {
    document.getElementById('status').textContent = 'error';
  }
}
tick();
setInterval(tick, 2000);
</script>
</body>
</html>`
