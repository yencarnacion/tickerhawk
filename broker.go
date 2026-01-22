package main

import (
	"sync"
	"time"
)

type NBBO struct {
	Bid float64 `json:"bid"`
	Ask float64 `json:"ask"`

	LastQuoteEventMs  int64 `json:"last_quote_event_ms"`
	LastQuoteIngestMs int64 `json:"last_quote_ingest_ms"`
}

type SymbolState struct {
	Symbol string `json:"symbol"`

	NBBO NBBO `json:"nbbo"`

	LastTradeEventMs  int64 `json:"last_trade_event_ms"`
	LastTradeIngestMs int64 `json:"last_trade_ingest_ms"`
}

type Broker struct {
	locNY *time.Location
	log   *Logger

	mu      sync.RWMutex
	symbols []string
	state   map[string]*SymbolState

	store   *Storage
	chw     *ClickHouseWriter // optional
	metrics *Metrics
}

func NewBroker(symbols []string, locNY *time.Location, store *Storage, chw *ClickHouseWriter, m *Metrics, log *Logger) *Broker {
	st := make(map[string]*SymbolState, len(symbols))
	for _, s := range symbols {
		st[s] = &SymbolState{Symbol: s}
	}
	return &Broker{
		locNY:   locNY,
		log:     log,
		symbols: append([]string(nil), symbols...),
		state:   st,
		store:   store,
		chw:     chw,
		metrics: m,
	}
}

func (b *Broker) Symbols() []string {
	return append([]string(nil), b.symbols...)
}

func (b *Broker) HandleQuote(q Quote) {
	b.metrics.IncQuote()
	nowMs := time.Now().UnixMilli()

	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.state[q.Symbol]
	if !ok {
		// symbol not in watchlist; ignore
		return
	}

	// Don't clobber good NBBO with zeros.
	if q.Bid > 0 {
		st.NBBO.Bid = q.Bid
	}
	if q.Ask > 0 {
		st.NBBO.Ask = q.Ask
	}
	st.NBBO.LastQuoteEventMs = q.TsEventMs
	st.NBBO.LastQuoteIngestMs = nowMs
}

func (b *Broker) HandleTrade(t Trade) {
	b.metrics.IncTrade()
	nowMs := time.Now().UnixMilli()

	var bid, ask float64

	b.mu.Lock()
	st, ok := b.state[t.Symbol]
	if ok {
		bid = st.NBBO.Bid
		ask = st.NBBO.Ask
		st.LastTradeEventMs = t.TsEventMs
		st.LastTradeIngestMs = nowMs
	}
	b.mu.Unlock()

	if !ok {
		return
	}

	bucket, mid := ClassifyBucket(t.Price, bid, ask)

	rec := TradeRecord{
		TsEventMs:  t.TsEventMs,
		TsIngestMs: nowMs,
		Symbol:     t.Symbol,
		Price:      t.Price,
		Size:       t.Size,
		Bid:        bid,
		Ask:        ask,
		Mid:        mid,
		Bucket:     bucket,
	}

	// ClickHouse enqueue (non-blocking). Never block ingestion on DB.
	if b.chw != nil {
		if ok := b.chw.TryEnqueue(rec); !ok {
			b.metrics.CHDropped(1)
		}
	}

	// Existing file/in-mem storage (optional fallback).
	if ok := b.store.TryEnqueue(rec); !ok {
		b.metrics.DropTrade()
	}
}

func (b *Broker) SnapshotSymbols() []SymbolState {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]SymbolState, 0, len(b.symbols))
	for _, s := range b.symbols {
		if st, ok := b.state[s]; ok {
			out = append(out, *st)
		}
	}
	return out
}
