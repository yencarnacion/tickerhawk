package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	start time.Time

	version   string
	commit    string
	buildDate string

	reconnectOK atomic.Int64
	reconnectNG atomic.Int64

	totalTrades atomic.Int64
	totalQuotes atomic.Int64

	droppedTrades atomic.Int64

	// ClickHouse writer metrics
	chInsertedRows       atomic.Int64
	chInsertErrors       atomic.Int64
	chDroppedDB          atomic.Int64
	chLastInsertLatencyMs atomic.Int64
	chLastInsertAtMs     atomic.Int64

	mu      sync.Mutex
	samples []rateSample // ring-ish, appended each second
}

type rateSample struct {
	at     time.Time
	trades int64
}

func NewMetrics(start time.Time, version, commit, buildDate string) *Metrics {
	return &Metrics{
		start:     start,
		version:   version,
		commit:    commit,
		buildDate: buildDate,
		samples:   make([]rateSample, 0, 16),
	}
}

func (m *Metrics) IncTrade()  { m.totalTrades.Add(1) }
func (m *Metrics) IncQuote()  { m.totalQuotes.Add(1) }
func (m *Metrics) DropTrade() { m.droppedTrades.Add(1) }

func (m *Metrics) CHInserted(n int64, latency time.Duration) {
	m.chInsertedRows.Add(n)
	m.chLastInsertLatencyMs.Store(latency.Milliseconds())
	m.chLastInsertAtMs.Store(time.Now().UnixMilli())
}
func (m *Metrics) CHInsertError() { m.chInsertErrors.Add(1) }
func (m *Metrics) CHDropped(n int64) {
	m.chDroppedDB.Add(n)
}

func (m *Metrics) ReconnectAttemptFailed() { m.reconnectNG.Add(1) }
func (m *Metrics) ReconnectSucceeded()     { m.reconnectOK.Add(1) }

func (m *Metrics) Run(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			tr := m.totalTrades.Load()

			m.mu.Lock()
			m.samples = append(m.samples, rateSample{at: now, trades: tr})
			// keep last ~30s
			if len(m.samples) > 40 {
				m.samples = m.samples[len(m.samples)-40:]
			}
			m.mu.Unlock()
		}
	}
}

func (m *Metrics) Snapshot() map[string]any {
	uptime := time.Since(m.start)

	trades := m.totalTrades.Load()
	quotes := m.totalQuotes.Load()

	r1, r5 := m.tradeRates()

	return map[string]any{
		"ok": true,

		"uptime_ms": uptime.Milliseconds(),
		"uptime":    uptime.String(),

		"build": map[string]any{
			"version":    m.version,
			"commit":     m.commit,
			"build_date": m.buildDate,
		},

		"reconnect": map[string]any{
			"success": m.reconnectOK.Load(),
			"failed":  m.reconnectNG.Load(),
		},

		"ingest": map[string]any{
			"trades_total": trades,
			"quotes_total": quotes,
			"trades_per_s": map[string]any{
				"1s": r1,
				"5s": r5,
			},
			"dropped_trades": m.droppedTrades.Load(),
		},

		"clickhouse": map[string]any{
			"inserted_rows_total":     m.chInsertedRows.Load(),
			"insert_errors_total":     m.chInsertErrors.Load(),
			"dropped_db_total":        m.chDroppedDB.Load(),
			"last_insert_latency_ms":  m.chLastInsertLatencyMs.Load(),
			"last_insert_at_unix_ms":  m.chLastInsertAtMs.Load(),
		},
	}
}

func (m *Metrics) tradeRates() (rate1 float64, rate5 float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.samples) < 2 {
		return 0, 0
	}
	latest := m.samples[len(m.samples)-1]

	// 1s rate: compare with prior sample
	prev := m.samples[len(m.samples)-2]
	dt := latest.at.Sub(prev.at).Seconds()
	if dt > 0 {
		rate1 = float64(latest.trades-prev.trades) / dt
	}

	// 5s rate: find sample >= 5s ago
	var older *rateSample
	for i := len(m.samples) - 1; i >= 0; i-- {
		if latest.at.Sub(m.samples[i].at) >= 5*time.Second {
			older = &m.samples[i]
			break
		}
	}
	if older != nil {
		dt5 := latest.at.Sub(older.at).Seconds()
		if dt5 > 0 {
			rate5 = float64(latest.trades-older.trades) / dt5
		}
	}
	return
}
