package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	_ "time/tzdata"

	"github.com/joho/godotenv"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

type CLIConfig struct {
	Port      int
	Watchlist string
	DataDir   string
	FlushMS   int
	Rotate    string
	LogLevel  string

	ClickHouseEnabled bool
	CHHost            string
	CHPort            int
	CHUser            string
	CHPass            string
	CHDB              string
	CHSecure          bool
	CHAsyncInsert     int
	CHBatchSize       int
	CHFlushMS         int
}

func main() {
	_ = godotenv.Load()

	cfg := CLIConfig{}

	flag.IntVar(&cfg.Port, "port", 8092, "HTTP port")
	flag.StringVar(&cfg.Watchlist, "watchlist", "./watchlist.yaml", "Path to watchlist.yaml")
	flag.StringVar(&cfg.DataDir, "data-dir", "./data", "Base data directory")
	flag.IntVar(&cfg.FlushMS, "flush-ms", 250, "Raw writer flush cadence (ms). Agg flush is max(5s, flush-ms).")
	flag.StringVar(&cfg.Rotate, "rotate", "daily", "Partitioning rotation (only 'daily' supported)")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug|info|warn|error")

	// ClickHouse flags (env-backed defaults)
	flag.BoolVar(&cfg.ClickHouseEnabled, "clickhouse", envBool("CLICKHOUSE_ENABLED", true), "Enable ClickHouse ingest+analytics")
	flag.StringVar(&cfg.CHHost, "ch-host", envString("CLICKHOUSE_HOST", "localhost"), "ClickHouse host")
	flag.IntVar(&cfg.CHPort, "ch-port", envInt("CLICKHOUSE_PORT", 9000), "ClickHouse native port")
	flag.StringVar(&cfg.CHUser, "ch-user", envString("CLICKHOUSE_USER", "default"), "ClickHouse user")
	flag.StringVar(&cfg.CHPass, "ch-pass", envString("CLICKHOUSE_PASS", ""), "ClickHouse password")
	flag.StringVar(&cfg.CHDB, "ch-db", envString("CLICKHOUSE_DB", "tickerhawk"), "ClickHouse database")
	flag.BoolVar(&cfg.CHSecure, "ch-secure", envBool("CLICKHOUSE_SECURE", false), "Use TLS to ClickHouse")
	flag.IntVar(&cfg.CHAsyncInsert, "ch-async-insert", envInt("CLICKHOUSE_ASYNC_INSERT", 1), "ClickHouse async_insert setting (0/1)")
	flag.IntVar(&cfg.CHBatchSize, "ch-batch-size", envInt("CLICKHOUSE_BATCH_SIZE", 5000), "ClickHouse insert batch size")
	flag.IntVar(&cfg.CHFlushMS, "ch-flush-ms", envInt("CLICKHOUSE_FLUSH_MS", 200), "ClickHouse flush cadence (ms)")

	flag.Parse()

	log := NewLogger(cfg.LogLevel)

	locNY, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("failed to load America/New_York: %v", err)
		os.Exit(1)
	}

	apiKey := getenvAny("MASSIVE_API_KEY", "POLYGON_API_KEY")
	if apiKey == "" {
		log.Errorf("missing MASSIVE_API_KEY (or legacy POLYGON_API_KEY) in env/.env")
		os.Exit(1)
	}

	wsURL := getenvAny("MASSIVE_WS_URL", "POLYGON_WS_URL")
	wsFeed := NormalizeWSFeed(wsURL)

	symbols, err := LoadWatchlist(cfg.Watchlist)
	if err != nil {
		log.Errorf("failed to load watchlist: %v", err)
		os.Exit(1)
	}
	if len(symbols) == 0 {
		log.Errorf("watchlist is empty")
		os.Exit(1)
	}
	log.Infof("loaded watchlist symbols=%d (%s)", len(symbols), filepath.Base(cfg.Watchlist))

	// Run context (NY timezone)
	nowNY := time.Now().In(locNY)
	runStartNY := time.UnixMilli(nowNY.UnixMilli()).In(locNY) // ms precision, no monotonic
	run := RunContext{
		ID:      newRunID(),
		StartNY: runStartNY,
	}

	metrics := NewMetrics(runStartNY, version, commit, buildDate)

	storage := NewStorage(StorageConfig{
		DataDir:   cfg.DataDir,
		FlushRaw:  time.Duration(cfg.FlushMS) * time.Millisecond,
		Rotate:    cfg.Rotate,
		MaxOpen:   64,
		LocNY:     locNY,
		Log:       log,
	}, metrics)

	// ClickHouse init (schema + connections)
	var chClient *ClickHouseClient
	var chw *ClickHouseWriter

	if cfg.ClickHouseEnabled {
		chCfg := ClickHouseConfig{
			Enabled:      true,
			Host:         cfg.CHHost,
			Port:         cfg.CHPort,
			User:         cfg.CHUser,
			Pass:         cfg.CHPass,
			DB:           cfg.CHDB,
			Secure:       cfg.CHSecure,
			AsyncInsert:  cfg.CHAsyncInsert != 0,
			BatchSize:    cfg.CHBatchSize,
			FlushEveryMS: cfg.CHFlushMS,
		}

		ctxInit, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		chClient, err = NewClickHouseClient(ctxInit, chCfg, locNY, log)
		cancel()

		if err != nil {
			log.Errorf("clickhouse init failed (continuing without CH): %v", err)
			chClient = nil
		} else {
			chw = NewClickHouseWriter(ClickHouseWriterConfig{
				BatchSize:  chCfg.BatchSize,
				FlushEvery: time.Duration(chCfg.FlushEveryMS) * time.Millisecond,
				BufferSize: 200_000,
			}, chClient.NativeConn(), run, locNY, metrics, log)
		}
	} else {
		log.Infof("clickhouse disabled")
	}

	broker := NewBroker(symbols, locNY, storage, chw, metrics, log)

	stream := NewMassiveStream(MassiveStreamConfig{
		APIKey:  apiKey,
		FeedURL: wsFeed,
		Symbols: symbols,
		LocNY:   locNY,
		Log:     log,
	}, broker, metrics)

	httpSrv := NewHTTPServer(HTTPConfig{
		Addr:       fmt.Sprintf(":%d", cfg.Port),
		LocNY:      locNY,
		Log:        log,
		Broker:     broker,
		Store:      storage,
		CH:         chClient,
		RunID:      run.ID,
		RunStartNY: run.StartNY,
		M:          metrics,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// start background components
	go metrics.Run(ctx)
	go storage.Run(ctx)
	if chw != nil {
		go chw.Run(ctx)
	}
	go stream.Run(ctx)

	// run HTTP server
	go func() {
		log.Infof("http listening on http://localhost:%d", cfg.Port)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("http server error: %v", err)
			stop()
		}
	}()

	<-ctx.Done()

	// graceful shutdown
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Infof("shutting down...")
	_ = httpSrv.Shutdown(shCtx)
	storage.Close()
	if chClient != nil {
		chClient.Close()
	}
	log.Infof("bye")
}

func getenvAny(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func envString(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(k)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}

func newRunID() string {
	// UUID v4 without external deps.
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func ahStartEnd(loc *time.Location, dateStr string, sinceMin int) (startNY, endNY time.Time, err error) {
	// dateStr: YYYY-MM-DD, sinceMin: minutes since midnight
	d, err := time.ParseInLocation("2006-01-02", dateStr, loc)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	startNY = d.Add(time.Duration(sinceMin) * time.Minute).In(loc)
	// AH ends at 20:00 ET per spec
	endNY = d.Add(20 * time.Hour).In(loc)
	return startNY, endNY, nil
}
