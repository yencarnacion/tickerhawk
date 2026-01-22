package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseConfig struct {
	Enabled      bool
	Host         string
	Port         int
	User         string
	Pass         string
	DB           string
	Secure       bool
	AsyncInsert  bool
	BatchSize    int
	FlushEveryMS int
}

type ClickHouseClient struct {
	cfg   ClickHouseConfig
	conn  clickhouse.Conn // native driver conn (batch insert)
	db    *sql.DB         // database/sql for ad-hoc queries & endpoints
	locNY *time.Location
	log   *Logger
}

func (c *ClickHouseClient) Addr() string { return fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.Port) }
func (c *ClickHouseClient) Database() string {
	if c == nil {
		return ""
	}
	return c.cfg.DB
}
func (c *ClickHouseClient) Secure() bool {
	if c == nil {
		return false
	}
	return c.cfg.Secure
}
func (c *ClickHouseClient) NativeConn() clickhouse.Conn { return c.conn }
func (c *ClickHouseClient) SQLDB() *sql.DB              { return c.db }

func (c *ClickHouseClient) Close() {
	if c == nil {
		return
	}
	if c.db != nil {
		_ = c.db.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

var safeIdentRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func validateIdent(s string) error {
	if s == "" {
		return fmt.Errorf("empty identifier")
	}
	if !safeIdentRe.MatchString(s) {
		return fmt.Errorf("unsafe identifier %q (allowed: [a-zA-Z0-9_])", s)
	}
	return nil
}

func (cfg ClickHouseConfig) options(database string) *clickhouse.Options {
	opt := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: cfg.User,
			Password: cfg.Pass,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{},
		// these are used by the native conn; SQL DB has its own pool, but it’s fine.
		MaxOpenConns:    8,
		MaxIdleConns:    8,
		ConnMaxLifetime: 30 * time.Minute,
	}
	if cfg.Secure {
		opt.TLS = &tls.Config{}
	}
	// async insert is a ClickHouse setting; applying at session level is fine.
	if cfg.AsyncInsert {
		opt.Settings["async_insert"] = 1
		opt.Settings["wait_for_async_insert"] = 0
	} else {
		opt.Settings["async_insert"] = 0
	}
	return opt
}

func NewClickHouseClient(ctx context.Context, cfg ClickHouseConfig, locNY *time.Location, log *Logger) (*ClickHouseClient, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if err := validateIdent(cfg.DB); err != nil {
		return nil, err
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.Port <= 0 {
		cfg.Port = 9000
	}
	if cfg.User == "" {
		cfg.User = "default"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.FlushEveryMS <= 0 {
		cfg.FlushEveryMS = 200
	}

	// 1) Connect to "default" DB to ensure the target database exists.
	connDefault, err := clickhouse.Open(cfg.options("default"))
	if err != nil {
		return nil, fmt.Errorf("clickhouse open(default) failed: %w", err)
	}
	{
		ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := connDefault.Exec(ctxPing, "SELECT 1"); err != nil {
			_ = connDefault.Close()
			return nil, fmt.Errorf("clickhouse ping(default) failed: %w", err)
		}
		ddlDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", cfg.DB)
		if err := connDefault.Exec(ctxPing, ddlDB); err != nil {
			_ = connDefault.Close()
			return nil, fmt.Errorf("create database failed: %w", err)
		}
	}
	_ = connDefault.Close()

	// 2) Connect to target DB and ensure tables/MV exist.
	conn, err := clickhouse.Open(cfg.options(cfg.DB))
	if err != nil {
		return nil, fmt.Errorf("clickhouse open(%s) failed: %w", cfg.DB, err)
	}
	{
		ctxDDL, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := ensureClickHouseSchema(ctxDDL, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}

	// 3) Open database/sql handle for ad-hoc queries & endpoints.
	db := clickhouse.OpenDB(cfg.options(cfg.DB))
	db.SetMaxOpenConns(6)
	db.SetMaxIdleConns(6)
	db.SetConnMaxLifetime(30 * time.Minute)

	{
		ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctxPing); err != nil {
			_ = db.Close()
			_ = conn.Close()
			return nil, fmt.Errorf("clickhouse db.Ping failed: %w", err)
		}
	}

	log.Infof("clickhouse ready addr=%s db=%s async_insert=%v", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), cfg.DB, cfg.AsyncInsert)

	return &ClickHouseClient{
		cfg:   cfg,
		conn:  conn,
		db:    db,
		locNY: locNY,
		log:   log,
	}, nil
}

func ensureClickHouseSchema(ctx context.Context, conn clickhouse.Conn) error {
	ddlRaw := `
CREATE TABLE IF NOT EXISTS tape_trades_raw
(
  run_id String,
  run_start DateTime64(3, 'America/New_York'),
  ts_event DateTime64(3, 'America/New_York'),
  ts_ingest DateTime64(3, 'America/New_York'),
  symbol LowCardinality(String),
  price Float64,
  size Int64,
  bid Float64,
  ask Float64,
  mid Float64,
  bucket LowCardinality(String),
  session LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (symbol, ts_event, run_id)
`

	// IMPORTANT: Specify only the count columns to sum, so run_start (DateTime64) isn't summed.
	ddl1m := `
CREATE TABLE IF NOT EXISTS tape_trades_1m
(
  run_id String,
  run_start DateTime64(3, 'America/New_York'),
  day Date,
  minute DateTime('America/New_York'),
  symbol LowCardinality(String),
  session LowCardinality(String),
  trades_total UInt64,
  above_ask UInt64,
  at_ask UInt64,
  mid_to_ask UInt64,
  at_mid UInt64,
  mid_to_bid UInt64,
  at_bid UInt64,
  below_bid UInt64
)
ENGINE = SummingMergeTree(trades_total, above_ask, at_ask, mid_to_ask, at_mid, mid_to_bid, at_bid, below_bid)
PARTITION BY day
ORDER BY (run_id, symbol, minute, session)
`

	ddlMV := `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tape_trades_1m
TO tape_trades_1m
AS
SELECT
  run_id,
  any(run_start) AS run_start,
  toDate(ts_event) AS day,
  toDateTime(toStartOfMinute(ts_event), 'America/New_York') AS minute,
  symbol,
  session,
  count() AS trades_total,
  countIf(bucket='above_ask') AS above_ask,
  countIf(bucket='at_ask') AS at_ask,
  countIf(bucket='mid_to_ask') AS mid_to_ask,
  countIf(bucket='at_mid') AS at_mid,
  countIf(bucket='mid_to_bid') AS mid_to_bid,
  countIf(bucket='at_bid') AS at_bid,
  countIf(bucket='below_bid') AS below_bid
FROM tape_trades_raw
GROUP BY
  run_id,
  toDate(ts_event),
  toDateTime(toStartOfMinute(ts_event), 'America/New_York'),
  symbol,
  session
`

	for i, q := range []string{ddlRaw, ddl1m, ddlMV} {
		if err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("clickhouse ddl step %d failed: %w", i+1, err)
		}
	}
	return nil
}

// -------------------- ClickHouse-backed analytics helpers --------------------

func (c *ClickHouseClient) RankWindow(ctx context.Context, runID string, startNY, endNY time.Time, session string, limit int) ([]RankRow, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("clickhouse not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	args := make([]any, 0, 8)
	q := `
SELECT
  symbol,
  sum(at_mid + mid_to_ask + at_ask + above_ask) AS askish,
  sum(mid_to_bid + at_bid + below_bid) AS bidish,
  sum(trades_total) AS total,
  sum(at_mid + mid_to_ask + at_ask + above_ask) / greatest(sum(mid_to_bid + at_bid + below_bid), 1) AS ratio,
  sum(at_mid + mid_to_ask + at_ask + above_ask) / greatest(sum(trades_total), 1) AS share
FROM tape_trades_1m
WHERE run_id = ?
  AND minute >= ?
  AND minute <= ?
`
	args = append(args, runID, startNY, endNY)

	if session != "" && session != "ALL" {
		q += "  AND session = ?\n"
		args = append(args, session)
	}

	q += `
GROUP BY symbol
ORDER BY ratio DESC, share DESC, askish DESC
LIMIT ` + strconv.Itoa(limit)

	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RankRow, 0, limit)
	for rows.Next() {
		var sym string
		var askish, bidish, total uint64
		var ratio, share float64
		if err := rows.Scan(&sym, &askish, &bidish, &total, &ratio, &share); err != nil {
			return nil, err
		}
		out = append(out, RankRow{
			Symbol: sym,
			Askish: int64(askish),
			Bidish: int64(bidish),
			Total:  int64(total),
			Ratio:  ratio,
			Share:  share,
		})
	}
	return out, rows.Err()
}

func (c *ClickHouseClient) RankAfterHours(ctx context.Context, runID string, startNY, endNY time.Time, limit int) ([]RankRow, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("clickhouse not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	q := `
SELECT
  symbol,
  sum(at_mid + mid_to_ask + at_ask + above_ask) AS askish,
  sum(mid_to_bid + at_bid + below_bid) AS bidish,
  sum(trades_total) AS total,
  sum(at_mid + mid_to_ask + at_ask + above_ask) / greatest(sum(mid_to_bid + at_bid + below_bid), 1) AS ratio,
  sum(at_mid + mid_to_ask + at_ask + above_ask) / greatest(sum(trades_total), 1) AS share
FROM tape_trades_1m
WHERE run_id = ?
  AND session = 'AH'
  AND minute >= ?
  AND minute < ?
GROUP BY symbol
ORDER BY ratio DESC, share DESC, askish DESC
LIMIT ` + strconv.Itoa(limit)

	rows, err := c.db.QueryContext(ctx, q, runID, startNY, endNY)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RankRow, 0, limit)
	for rows.Next() {
		var sym string
		var askish, bidish, total uint64
		var ratio, share float64
		if err := rows.Scan(&sym, &askish, &bidish, &total, &ratio, &share); err != nil {
			return nil, err
		}
		out = append(out, RankRow{
			Symbol: sym,
			Askish: int64(askish),
			Bidish: int64(bidish),
			Total:  int64(total),
			Ratio:  ratio,
			Share:  share,
		})
	}
	return out, rows.Err()
}
