package main

import (
	"context"
	"fmt"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseWriterConfig struct {
	BatchSize  int
	FlushEvery time.Duration
	BufferSize int
}

type ClickHouseWriter struct {
	cfg   ClickHouseWriterConfig
	conn  clickhouse.Conn
	run   RunContext
	locNY *time.Location
	log   *Logger
	m     *Metrics

	in chan TradeRecord
}

func NewClickHouseWriter(cfg ClickHouseWriterConfig, conn clickhouse.Conn, run RunContext, locNY *time.Location, m *Metrics, log *Logger) *ClickHouseWriter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 200 * time.Millisecond
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 200_000
	}
	return &ClickHouseWriter{
		cfg:   cfg,
		conn:  conn,
		run:   run,
		locNY: locNY,
		log:   log,
		m:     m,
		in:    make(chan TradeRecord, cfg.BufferSize),
	}
}

func (w *ClickHouseWriter) TryEnqueue(rec TradeRecord) bool {
	select {
	case w.in <- rec:
		return true
	default:
		return false
	}
}

func (w *ClickHouseWriter) Run(ctx context.Context) {
	t := time.NewTicker(w.cfg.FlushEvery)
	defer t.Stop()

	batch := make([]TradeRecord, 0, w.cfg.BatchSize)

	flush := func(buf []TradeRecord) {
		if len(buf) == 0 {
			return
		}
		const maxAttempts = 3

		var lastErr error
		for attempt := 0; attempt < maxAttempts; attempt++ {
			if ctx.Err() != nil {
				return
			}

			ctxIns, cancel := context.WithTimeout(ctx, 5*time.Second)
			start := time.Now()
			err := w.insertBatch(ctxIns, buf)
			cancel()
			lat := time.Since(start)

			if err == nil {
				w.m.CHInserted(int64(len(buf)), lat)
				return
			}

			lastErr = err
			w.m.CHInsertError()

			// backoff (bounded), but never block ingestion goroutines (this is the writer goroutine).
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			if backoff > 1500*time.Millisecond {
				backoff = 1500 * time.Millisecond
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}

		// Give up: count drops, log (rate-limited enough by batch size).
		w.m.CHDropped(int64(len(buf)))
		w.log.Errorf("clickhouse insert failed; dropped %d rows: %v", len(buf), lastErr)
	}

	for {
		select {
		case <-ctx.Done():
			// drain best-effort then final flush
		Drain:
			for {
				select {
				case rec := <-w.in:
					batch = append(batch, rec)
					if len(batch) >= w.cfg.BatchSize {
						tmp := batch
						batch = make([]TradeRecord, 0, w.cfg.BatchSize)
						flush(tmp)
					}
				default:
					break Drain
				}
			}
			flush(batch)
			return

		case rec := <-w.in:
			batch = append(batch, rec)
			if len(batch) >= w.cfg.BatchSize {
				tmp := batch
				batch = make([]TradeRecord, 0, w.cfg.BatchSize)
				flush(tmp)
			}

		case <-t.C:
			if len(batch) > 0 {
				tmp := batch
				batch = make([]TradeRecord, 0, w.cfg.BatchSize)
				flush(tmp)
			}
		}
	}
}

func (w *ClickHouseWriter) insertBatch(ctx context.Context, buf []TradeRecord) error {
	if w.conn == nil {
		return fmt.Errorf("no clickhouse conn")
	}

	const insertSQL = `
INSERT INTO tape_trades_raw
(run_id, run_start, ts_event, ts_ingest, symbol, price, size, bid, ask, mid, bucket, session)
`

	b, err := w.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return err
	}

	for _, rec := range buf {
		tsEvent := time.UnixMilli(rec.TsEventMs).In(w.locNY)
		tsIngest := time.UnixMilli(rec.TsIngestMs).In(w.locNY)
		session := SessionForNY(tsEvent)

		if err := b.Append(
			w.run.ID,
			w.run.StartNY,
			tsEvent,
			tsIngest,
			rec.Symbol,
			rec.Price,
			rec.Size,
			rec.Bid,
			rec.Ask,
			rec.Mid,
			rec.Bucket,
			session,
		); err != nil {
			return err
		}
	}

	return b.Send()
}
